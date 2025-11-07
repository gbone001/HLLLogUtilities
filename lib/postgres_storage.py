import logging
import os
from functools import lru_cache
from typing import Sequence

import psycopg

from lib.logs import LogLine

_LOGGER = logging.getLogger(__name__)

_connection: psycopg.Connection | None = None
_schema_ready = False


def _is_disabled() -> bool:
    value = os.getenv("POSTGRES_ENABLED")
    if value is None:
        return False
    return value.strip().lower() in {"0", "false", "no"}


@lru_cache()
def _get_dsn() -> str | None:
    if _is_disabled():
        return None

    env_url = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    host = os.getenv("POSTGRES_HOST") or os.getenv("PGHOST")
    database = os.getenv("POSTGRES_DB") or os.getenv("PGDATABASE")
    user = os.getenv("POSTGRES_USER") or os.getenv("PGUSER")
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD", "")
    port = os.getenv("POSTGRES_PORT") or os.getenv("PGPORT") or "5432"

    if host and database and user:
        password_part = f":{password}" if password else ""
        return f"postgresql://{user}{password_part}@{host}:{port}/{database}"

    return None


def _get_connection() -> psycopg.Connection | None:
    global _connection, _schema_ready
    dsn = _get_dsn()
    if not dsn:
        return None

    if _connection is None or _connection.closed:
        try:
            _connection = psycopg.connect(dsn)
        except Exception as exc:
            _LOGGER.warning("Unable to connect to Postgres (%s), disabling replication", exc)
            _connection = None
            return None

    if not _schema_ready:
        try:
            _ensure_schema(_connection)
            _schema_ready = True
        except Exception:
            _LOGGER.exception("Failed to initialize Postgres schema, disabling replication")
            try:
                _connection.close()
            finally:
                _connection = None
            return None

    return _connection


def _ensure_schema(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS session_logs (
                id BIGSERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL,
                guild_id BIGINT,
                event_time TIMESTAMPTZ NOT NULL,
                event_type TEXT NOT NULL,
                player_name TEXT,
                player_id TEXT,
                player_team TEXT,
                player_role TEXT,
                player_combat_score INTEGER,
                player_offense_score INTEGER,
                player_defense_score INTEGER,
                player_support_score INTEGER,
                player2_name TEXT,
                player2_id TEXT,
                player2_team TEXT,
                player2_role TEXT,
                weapon TEXT,
                old TEXT,
                new TEXT,
                team_name TEXT,
                squad_name TEXT,
                message TEXT
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_session_logs_session_id
                ON session_logs (session_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_session_logs_guild_event_time
                ON session_logs (guild_id, event_time DESC);
            """
        )
    connection.commit()


def mirror_session_logs(session_id: int, guild_id: int | None, logs: Sequence[LogLine]) -> None:
    connection = _get_connection()
    if not connection or not logs:
        return

    payload = []
    for log in logs:
        data = log.model_dump()
        payload.append(
            (
                session_id,
                guild_id,
                data["event_time"],
                data["event_type"],
                data.get("player_name"),
                data.get("player_id"),
                data.get("player_team"),
                data.get("player_role"),
                data.get("player_combat_score"),
                data.get("player_offense_score"),
                data.get("player_defense_score"),
                data.get("player_support_score"),
                data.get("player2_name"),
                data.get("player2_id"),
                data.get("player2_team"),
                data.get("player2_role"),
                data.get("weapon"),
                data.get("old"),
                data.get("new"),
                data.get("team_name"),
                data.get("squad_name"),
                data.get("message"),
            )
        )

    try:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO session_logs (
                    session_id,
                    guild_id,
                    event_time,
                    event_type,
                    player_name,
                    player_id,
                    player_team,
                    player_role,
                    player_combat_score,
                    player_offense_score,
                    player_defense_score,
                    player_support_score,
                    player2_name,
                    player2_id,
                    player2_team,
                    player2_role,
                    weapon,
                    old,
                    new,
                    team_name,
                    squad_name,
                    message
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
                """,
                payload,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        _LOGGER.exception("Failed to mirror %s logs to Postgres", len(payload))
