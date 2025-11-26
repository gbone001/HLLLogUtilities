from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Coroutine, Sequence

from utils import get_config

from lib.logs import LogLine

from .postgres import (
    PostgresConfig,
    PostgresStorage,
    SessionCreateParams,
    ensure_partitions,
)

LOGGER = logging.getLogger(__name__)
QUEUE_WARN_THRESHOLD = 25


class StorageMode(str, Enum):
    SQLITE = "sqlite"
    DUAL = "dual"
    POSTGRES = "postgres"


@dataclass(slots=True)
class SessionReplica:
    session_id: int
    guild_id: int
    name: str
    start_time: datetime
    end_time: datetime | None
    is_auto: bool
    credentials_id: int | None
    modifier_flags: int


@dataclass(slots=True)
class CredentialReplica:
    id: int
    guild_id: int
    name: str
    address: str
    port: int
    password: str
    default_modifiers: int
    autosession_enabled: bool


@dataclass(slots=True)
class ApiKeyReplica:
    id: int
    guild_id: int
    tag: str
    key: str | None


_database_section = get_config()["Database"] if get_config().has_section("Database") else None
_mode_raw = os.getenv("HLL_STORAGE_MODE") or (_database_section.get("Mode") if _database_section else None)
try:
    STORAGE_MODE = StorageMode((_mode_raw or "sqlite").strip().lower())
except ValueError:
    LOGGER.warning("Unknown storage mode '%s', defaulting to sqlite", _mode_raw)
    STORAGE_MODE = StorageMode.SQLITE

_POSTGRES_DSN = os.getenv("HLL_DB_URL") or (_database_section.get("Url") if _database_section else "")
_POSTGRES_DSN = (_POSTGRES_DSN or "").strip()
_pool_min = int(_database_section.get("PoolMinSize", "1")) if _database_section else 1
_pool_max = int(_database_section.get("PoolMaxSize", "10")) if _database_section else 10
_stmt_timeout = _database_section.get("StatementTimeoutSeconds") if _database_section else None
if _stmt_timeout is not None and _stmt_timeout.strip() == "":
    _stmt_timeout = None
_POSTGRES_CONFIG = (
    PostgresConfig(
        dsn=_POSTGRES_DSN,
        pool_min_size=_pool_min,
        pool_max_size=_pool_max,
        statement_timeout_seconds=int(_stmt_timeout) if _stmt_timeout else None,
    )
    if _POSTGRES_DSN
    else None
)

if STORAGE_MODE in (StorageMode.DUAL, StorageMode.POSTGRES) and not _POSTGRES_CONFIG:
    LOGGER.warning(
        "Storage mode %s requires Database.Url or HLL_DB_URL; falling back to sqlite",
        STORAGE_MODE.value,
    )
    STORAGE_MODE = StorageMode.SQLITE

_pg_storage: PostgresStorage | None = None
_pending_tasks: deque[asyncio.Task] = deque()
_partition_cache: set[str] = set()


def get_storage_mode() -> StorageMode:
    return STORAGE_MODE


def should_write_to_postgres() -> bool:
    return STORAGE_MODE in (StorageMode.DUAL, StorageMode.POSTGRES) and _POSTGRES_CONFIG is not None


def postgres_ready() -> bool:
    return _pg_storage is not None


def pending_task_count() -> int:
    return len(_pending_tasks)


async def startup() -> None:
    global _pg_storage
    if not should_write_to_postgres():
        if STORAGE_MODE in (StorageMode.DUAL, StorageMode.POSTGRES):
            LOGGER.warning("Storage mode %s enabled but no Database.Url configured", STORAGE_MODE.value)
        return
    if _pg_storage:
        return
    _pg_storage = PostgresStorage(_POSTGRES_CONFIG)  # type: ignore[arg-type]
    await _pg_storage.connect()
    LOGGER.info(
        "Connected PostgreSQL pool (mode=%s, min=%s, max=%s)",
        STORAGE_MODE.value,
        _POSTGRES_CONFIG.pool_min_size if _POSTGRES_CONFIG else "?",
        _POSTGRES_CONFIG.pool_max_size if _POSTGRES_CONFIG else "?",
    )


async def shutdown() -> None:
    global _pg_storage
    if _pg_storage:
        await _pg_storage.close()
        _pg_storage = None
        LOGGER.info("Closed PostgreSQL pool")


def _track_task(task: asyncio.Task, label: str) -> None:
    _pending_tasks.append(task)
    if len(_pending_tasks) > QUEUE_WARN_THRESHOLD:
        LOGGER.warning("Postgres write backlog at %s tasks (latest=%s)", len(_pending_tasks), label)

    def _task_done(t: asyncio.Task) -> None:
        try:
            _pending_tasks.remove(t)
        except ValueError:
            pass
        try:
            t.result()
        except Exception:
            LOGGER.exception("Postgres task '%s' failed", label)

    task.add_done_callback(_task_done)


def _schedule(coro: Coroutine[Any, Any, Any], label: str) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
        return
    task = loop.create_task(coro, name=label)
    _track_task(task, label)


def _ensure_storage(label: str) -> bool:
    if not should_write_to_postgres():
        return False
    if not _pg_storage:
        LOGGER.warning("Postgres storage not ready for %s", label)
        return False
    return True


def _normalize_month(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


async def _ensure_partitions_for_logs(logs: Sequence[LogLine]) -> None:
    if not logs or not _pg_storage:
        return
    months = {_normalize_month(log.event_time) for log in logs}
    for month in months:
        key = month.strftime("%Y-%m-01")
        if key in _partition_cache:
            continue
        _partition_cache.add(key)
        LOGGER.info("Ensuring partition for %s", key)
        try:
            await ensure_partitions(_pg_storage, month)
        except Exception:
            _partition_cache.discard(key)
            LOGGER.exception("Failed to ensure partition for %s", key)
            raise


def replicate_session(record: SessionReplica) -> None:
    if not _ensure_storage("session-upsert"):
        return

    async def _run() -> None:
        assert _pg_storage
        params = SessionCreateParams(
            guild_id=record.guild_id,
            name=record.name,
            start_time=record.start_time,
            end_time=record.end_time,
            is_auto=record.is_auto,
            credentials_id=record.credentials_id,
            modifier_flags=record.modifier_flags,
            session_id=record.session_id,
        )
        await _pg_storage.create_session(params)

    _schedule(_run(), f"pg-session-{record.session_id}")


def replicate_session_deletion(session_id: int) -> None:
    if not _ensure_storage("session-delete"):
        return

    async def _run() -> None:
        assert _pg_storage
        await _pg_storage.delete_session(session_id)

    _schedule(_run(), f"pg-session-delete-{session_id}")


def replicate_session_mark_deleted(session_id: int, deleted_at: datetime | None = None) -> None:
    if not _ensure_storage("session-mark-deleted"):
        return

    async def _run() -> None:
        assert _pg_storage
        await _pg_storage.mark_session_deleted(session_id, deleted_at)

    _schedule(_run(), f"pg-session-mark-deleted-{session_id}")


def replicate_session_logs(session_id: int, logs: Sequence[LogLine]) -> None:
    if not logs or not _ensure_storage("session-logs"):
        return
    payload = [log.model_copy(deep=True) for log in logs]

    async def _run() -> None:
        assert _pg_storage
        await _ensure_partitions_for_logs(payload)
        await _pg_storage.insert_logs(session_id, payload)

    _schedule(_run(), f"pg-session-logs-{session_id}")


def replicate_session_log_purge(session_id: int) -> None:
    if not _ensure_storage("session-log-purge"):
        return

    async def _run() -> None:
        assert _pg_storage
        deleted = await _pg_storage.purge_session_logs(session_id)
        LOGGER.info("Purged %s log rows for session %s", deleted, session_id)

    _schedule(_run(), f"pg-session-log-purge-{session_id}")


def replicate_credentials(record: CredentialReplica) -> None:
    if not _ensure_storage("credentials-upsert"):
        return

    async def _run() -> None:
        assert _pg_storage
        query = (
            "INSERT INTO credentials (id, guild_id, name, address, port, password, default_modifiers, autosession_enabled) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
            "ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, address=EXCLUDED.address, port=EXCLUDED.port, "
            "password=EXCLUDED.password, default_modifiers=EXCLUDED.default_modifiers, autosession_enabled=EXCLUDED.autosession_enabled"
        )
        async with _pg_storage.pool.acquire() as conn:
            await conn.execute(
                query,
                record.id,
                record.guild_id,
                record.name,
                record.address,
                record.port,
                record.password,
                record.default_modifiers,
                record.autosession_enabled,
            )

    _schedule(_run(), f"pg-credentials-{record.id}")


def delete_credentials(credential_id: int) -> None:
    if not _ensure_storage("credentials-delete"):
        return

    async def _run() -> None:
        assert _pg_storage
        async with _pg_storage.pool.acquire() as conn:
            await conn.execute("DELETE FROM credentials WHERE id = $1", credential_id)

    _schedule(_run(), f"pg-credentials-delete-{credential_id}")


def replicate_api_key(record: ApiKeyReplica) -> None:
    if not _ensure_storage("api-key-upsert"):
        return

    async def _run() -> None:
        assert _pg_storage
        query = (
            "INSERT INTO hss_api_keys (id, guild_id, tag, key) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT (id) DO UPDATE SET tag=EXCLUDED.tag, key=EXCLUDED.key"
        )
        async with _pg_storage.pool.acquire() as conn:
            await conn.execute(query, record.id, record.guild_id, record.tag, record.key)

    _schedule(_run(), f"pg-api-key-{record.id}")


def delete_api_key(api_key_id: int) -> None:
    if not _ensure_storage("api-key-delete"):
        return

    async def _run() -> None:
        assert _pg_storage
        async with _pg_storage.pool.acquire() as conn:
            await conn.execute("DELETE FROM hss_api_keys WHERE id = $1", api_key_id)

    _schedule(_run(), f"pg-api-key-delete-{api_key_id}")
```}