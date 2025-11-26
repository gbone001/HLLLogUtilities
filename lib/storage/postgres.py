from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence

import asyncpg

from lib.logs import LogLine

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class PostgresConfig:
    dsn: str
    pool_min_size: int = 1
    pool_max_size: int = 10
    statement_timeout_seconds: int | None = None


@dataclass(slots=True)
class SessionCreateParams:
    guild_id: int
    name: str
    start_time: datetime
    end_time: datetime | None
    is_auto: bool = False
    credentials_id: int | None = None
    modifier_flags: int = 0
    session_id: int | None = None


LOG_LINE_FIELDS: tuple[str, ...] = tuple(LogLine.model_fields.keys())
LOG_COLUMNS: tuple[str, ...] = ("session_id", *LOG_LINE_FIELDS)


class PostgresStorageError(RuntimeError):
    pass


class PostgresStorage:
    """Thin repository wrapper around asyncpg for session persistence."""

    def __init__(self, config: PostgresConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        async def _init(conn: asyncpg.Connection):
            if self._config.statement_timeout_seconds:
                ms = max(self._config.statement_timeout_seconds, 0) * 1000
                await conn.execute(f"SET SESSION statement_timeout = {int(ms)}")

        LOGGER.info("Creating PostgreSQL pool (min=%s, max=%s)", self._config.pool_min_size, self._config.pool_max_size)
        self._pool = await asyncpg.create_pool(
            dsn=self._config.dsn,
            min_size=self._config.pool_min_size,
            max_size=self._config.pool_max_size,
            init=_init,
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def pool(self) -> asyncpg.Pool:
        if not self._pool:
            raise PostgresStorageError("Pool has not been initialised. Call connect() first.")
        return self._pool

    async def create_session(self, params: SessionCreateParams) -> int:
        fields = [
            "guild_id",
            "name",
            "start_time",
            "end_time",
            "is_auto",
            "credentials_id",
            "modifier_flags",
        ]
        args = [
            params.guild_id,
            params.name,
            params.start_time,
            params.end_time,
            params.is_auto,
            params.credentials_id,
            params.modifier_flags,
        ]

        placeholders = [f"${idx}" for idx in range(1, len(args) + 1)]
        returning = "RETURNING id"

        if params.session_id is not None:
            fields.insert(0, "id")
            args.insert(0, params.session_id)
            placeholders = [f"${idx}" for idx in range(1, len(args) + 1)]
            conflict_set = ", ".join(
                f"{field}=EXCLUDED.{field}" for field in fields if field != "id"
            )
            query = (
                f"INSERT INTO sessions ({', '.join(fields)}) VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO UPDATE SET {conflict_set} {returning}"
            )
        else:
            query = f"INSERT INTO sessions ({', '.join(fields)}) VALUES ({', '.join(placeholders)}) {returning}"

        async with self.pool.acquire() as conn:
            session_id = await conn.fetchval(query, *args)
        LOGGER.debug("Created session %s", session_id)
        return int(session_id)

    async def update_session_end(self, session_id: int, end_time: datetime | None) -> None:
        query = "UPDATE sessions SET end_time = $2 WHERE id = $1"
        async with self.pool.acquire() as conn:
            await conn.execute(query, session_id, end_time)

    async def mark_session_deleted(self, session_id: int, deleted_at: datetime | None = None) -> None:
        query = "UPDATE sessions SET deleted_at = COALESCE($2, NOW()) WHERE id = $1"
        async with self.pool.acquire() as conn:
            await conn.execute(query, session_id, deleted_at)

    async def delete_session(self, session_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM sessions WHERE id = $1", session_id)

    async def insert_logs(self, session_id: int, logs: Sequence[LogLine]) -> int:
        if not logs:
            return 0

        def _render_records() -> Iterable[Sequence[object | None]]:
            for log_line in logs:
                yield (session_id, *[getattr(log_line, field) for field in LOG_LINE_FIELDS])

        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "session_logs",
                records=_render_records(),
                columns=LOG_COLUMNS,
            )
        return len(logs)

    async def fetch_logs(
        self,
        session_id: int,
        *,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        limit: int | None = None,
        event_types: Sequence[str] | None = None,
    ) -> list[LogLine]:
        clauses = ["session_id = $1"]
        args: list[object] = [session_id]
        arg_idx = 2

        if from_time is not None:
            clauses.append(f"event_time >= ${arg_idx}")
            args.append(from_time)
            arg_idx += 1
        if to_time is not None:
            clauses.append(f"event_time < ${arg_idx}")
            args.append(to_time)
            arg_idx += 1
        if event_types:
            clauses.append(f"event_type = ANY(${arg_idx})")
            args.append(list(event_types))
            arg_idx += 1

        where_clause = " AND ".join(clauses)
        limit_clause = f" LIMIT {limit}" if limit is not None else ""
        columns = ", ".join(LOG_LINE_FIELDS)
        query = f"SELECT {columns} FROM session_logs WHERE {where_clause} ORDER BY event_time ASC{limit_clause}"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [LogLine(**dict(row)) for row in rows]

    async def purge_session_logs(self, session_id: int) -> int:
        async with self.pool.acquire() as conn:
            res = await conn.execute("DELETE FROM session_logs WHERE session_id = $1", session_id)
        # asyncpg returns strings like 'DELETE 42'
        return int(res.split(" ")[1])

    async def cleanup_expired_sessions(self, before: datetime) -> list[int]:
        query = (
            "DELETE FROM sessions WHERE COALESCE(end_time, start_time) < $1 AND deleted_at IS NOT NULL "
            "RETURNING id"
        )
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, before)
        deleted_ids = [int(row["id"]) for row in rows]
        if deleted_ids:
            LOGGER.info("Deleted %s expired sessions", len(deleted_ids))
        return deleted_ids


def _get_month_end(month_start: datetime) -> datetime:
    if month_start.month == 12:
        return datetime(month_start.year + 1, 1, 1, tzinfo=month_start.tzinfo)
    return datetime(month_start.year, month_start.month + 1, 1, tzinfo=month_start.tzinfo)


async def ensure_partitions(storage: PostgresStorage, month_start: datetime) -> None:
    """Creates a monthly partition for session_logs if it does not exist."""
    partition_name = f"session_logs_{month_start:%Y_%m}"
    month_end = _get_month_end(month_start)
    ddl = (
        "CREATE TABLE IF NOT EXISTS {name} PARTITION OF session_logs "
        "FOR VALUES FROM ('{start}') TO ('{end}')"
    ).format(
        name=partition_name,
        start=month_start.strftime("%Y-%m-01"),
        end=month_end.strftime("%Y-%m-01"),
    )
    async with storage.pool.acquire() as conn:
        await conn.execute(ddl)