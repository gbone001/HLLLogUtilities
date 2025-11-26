from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Sequence

from lib.logs import LogLine
from lib.storage.postgres import PostgresConfig, PostgresStorage, SessionCreateParams, ensure_partitions

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class Args:
    sqlite_path: str
    postgres_dsn: str
    pool_min_size: int
    pool_max_size: int
    batch_size: int
    start_session_id: int | None
    end_session_id: int | None
    dry_run: bool


def parse_args() -> Args:
    parser = argparse.ArgumentParser(description="Backfill SQLite data into PostgreSQL")
    parser.add_argument("--sqlite-path", default="sessions.db", help="Path to the legacy SQLite database")
    parser.add_argument("--postgres-dsn", required=True, help="postgresql:// connection string")
    parser.add_argument("--pool-min-size", type=int, default=1, help="Minimum asyncpg pool size")
    parser.add_argument("--pool-max-size", type=int, default=5, help="Maximum asyncpg pool size")
    parser.add_argument("--batch-size", type=int, default=1000, help="Number of log rows to stream per batch")
    parser.add_argument("--start-session-id", type=int, help="First session ROWID to migrate (inclusive)")
    parser.add_argument("--end-session-id", type=int, help="Last session ROWID to migrate (inclusive)")
    parser.add_argument("--dry-run", action="store_true", help="Validate connectivity without writing")
    parsed = parser.parse_args()
    return Args(
        sqlite_path=parsed.sqlite_path,
        postgres_dsn=parsed.postgres_dsn,
        pool_min_size=parsed.pool_min_size,
        pool_max_size=parsed.pool_max_size,
        batch_size=max(1, parsed.batch_size),
        start_session_id=parsed.start_session_id,
        end_session_id=parsed.end_session_id,
        dry_run=parsed.dry_run,
    )


def _connect_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _coerce_dt(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_month(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _require_dt(value: datetime | None, field: str) -> datetime:
    if value is None:
        raise ValueError(f"{field} is missing")
    return value


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return cur.fetchone() is not None


def _fetch_credentials(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _table_exists(conn, "credentials"):
        LOGGER.warning("Skipping credentials copy; table missing")
        return []
    cur = conn.execute(
        """
        SELECT ROWID AS id, guild_id, name, address, port, password, default_modifiers, autosession_enabled
        FROM credentials ORDER BY ROWID
        """
    )
    return list(cur.fetchall())


def _fetch_hss_keys(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _table_exists(conn, "hss_api_keys"):
        LOGGER.warning("Skipping hss_api_keys copy; table missing")
        return []
    cur = conn.execute(
        """
        SELECT ROWID AS id, guild_id, tag, key
        FROM hss_api_keys ORDER BY ROWID
        """
    )
    return list(cur.fetchall())


def _iter_sessions(conn: sqlite3.Connection, start_id: int | None, end_id: int | None) -> Iterator[sqlite3.Row]:
    if not _table_exists(conn, "sessions"):
        LOGGER.warning("No sessions table found; skipping session migration")
        return iter(())
    query = "SELECT ROWID AS id, guild_id, name, start_time, end_time, deleted, credentials_id, modifiers FROM sessions"
    clauses: list[str] = []
    params: list[int] = []
    if start_id is not None:
        clauses.append("ROWID >= ?")
        params.append(start_id)
    if end_id is not None:
        clauses.append("ROWID <= ?")
        params.append(end_id)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY ROWID"
    cur = conn.execute(query, params)
    yield from cur.fetchall()


def _iter_log_batches(conn: sqlite3.Connection, session_id: int, batch_size: int) -> Iterator[list[LogLine]]:
    table = f"session{session_id}"
    try:
        cur = conn.execute(f'SELECT * FROM "{table}" ORDER BY event_time ASC')
    except sqlite3.OperationalError:
        LOGGER.warning("Skipping missing log table %s", table)
        return
    columns = [col[0] for col in cur.description]
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        logs: list[LogLine] = []
        for raw in rows:
            payload = {col: raw[idx] for idx, col in enumerate(columns)}
            payload["event_time"] = _require_dt(_coerce_dt(payload["event_time"]), "event_time")
            logs.append(LogLine(**payload))
        yield logs


def _build_storage(args: Args) -> PostgresStorage:
    config = PostgresConfig(
        dsn=args.postgres_dsn,
        pool_min_size=args.pool_min_size,
        pool_max_size=args.pool_max_size,
    )
    return PostgresStorage(config)


async def _copy_credentials(conn: sqlite3.Connection, storage: PostgresStorage | None) -> None:
    rows = _fetch_credentials(conn)
    LOGGER.info("Migrating %s credentials", len(rows))
    if not storage:
        return
    query = (
        "INSERT INTO credentials (id, guild_id, name, address, port, password, default_modifiers, autosession_enabled) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
        "ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, address=EXCLUDED.address, port=EXCLUDED.port, "
        "password=EXCLUDED.password, default_modifiers=EXCLUDED.default_modifiers, autosession_enabled=EXCLUDED.autosession_enabled"
    )
    async with storage.pool.acquire() as conn_pg:
        await conn_pg.executemany(
            query,
            [
                (
                    row["id"],
                    row["guild_id"],
                    row["name"],
                    row["address"],
                    row["port"],
                    row["password"],
                    row["default_modifiers"],
                    bool(row["autosession_enabled"]),
                )
                for row in rows
            ],
        )


async def _copy_hss_keys(conn: sqlite3.Connection, storage: PostgresStorage | None) -> None:
    rows = _fetch_hss_keys(conn)
    LOGGER.info("Migrating %s HSS API keys", len(rows))
    if not storage:
        return
    query = (
        "INSERT INTO hss_api_keys (id, guild_id, tag, key) VALUES ($1,$2,$3,$4) "
        "ON CONFLICT (id) DO UPDATE SET tag=EXCLUDED.tag, key=EXCLUDED.key"
    )
    async with storage.pool.acquire() as conn_pg:
        await conn_pg.executemany(
            query,
            [
                (row["id"], row["guild_id"], row["tag"], row["key"]) for row in rows
            ],
        )


async def _ensure_partitions_for_logs(storage: PostgresStorage, logs: Sequence[LogLine]) -> None:
    months = {_normalize_month(log.event_time) for log in logs if log.event_time}
    for month in months:
        await ensure_partitions(storage, month)


async def _copy_session(storage: PostgresStorage | None, row: sqlite3.Row) -> None:
    start_time = _require_dt(_coerce_dt(row["start_time"]), "start_time")
    params = SessionCreateParams(
        session_id=row["id"],
        guild_id=row["guild_id"],
        name=row["name"],
        start_time=start_time,
        end_time=_coerce_dt(row["end_time"]),
        is_auto=row["end_time"] is None,
        credentials_id=row["credentials_id"],
        modifier_flags=row["modifiers"] or 0,
    )
    if storage:
        await storage.create_session(params)
        if row["deleted"]:
            await storage.mark_session_deleted(row["id"], datetime.now(timezone.utc))


async def _copy_logs_for_session(
    conn: sqlite3.Connection,
    storage: PostgresStorage | None,
    session_id: int,
    batch_size: int,
) -> int:
    total = 0
    for batch in _iter_log_batches(conn, session_id, batch_size):
        total += len(batch)
        if storage:
            await _ensure_partitions_for_logs(storage, batch)
            await storage.insert_logs(session_id, batch)
    return total


async def migrate(args: Args) -> None:
    sqlite_conn = _connect_sqlite(args.sqlite_path)
    storage: PostgresStorage | None = None
    if args.dry_run:
        LOGGER.info("Running in dry-run mode; no PostgreSQL writes will be issued")
    else:
        storage = _build_storage(args)
        await storage.connect()
    try:
        await _copy_credentials(sqlite_conn, storage)
        await _copy_hss_keys(sqlite_conn, storage)
        for row in _iter_sessions(sqlite_conn, args.start_session_id, args.end_session_id):
            LOGGER.info("Migrating session #%s (%s)", row["id"], row["name"])
            await _copy_session(storage, row)
            copied = await _copy_logs_for_session(sqlite_conn, storage, row["id"], args.batch_size)
            LOGGER.info("  -> copied %s log rows", copied)
    finally:
        sqlite_conn.close()
        if storage:
            await storage.close()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s] %(message)s")
    asyncio.run(migrate(args))


if __name__ == "__main__":
    main()
