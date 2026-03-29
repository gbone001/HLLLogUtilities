import logging
import json
from datetime import datetime, timezone
import os
import time
from pypika import Table, Query
import sqlite3
from typing import Any, Sequence
import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.types.json import Jsonb

from lib.logs import LogLine
from utils import SESSIONS_DB_PATH, get_config

DB_VERSION = 7
HLU_VERSION = "v2.2.15"
ARCHIVE_DB_VERSION = 1

database = sqlite3.connect(str(SESSIONS_DB_PATH))
cursor = database.cursor()

config = get_config()

def _config_value(section: str, option: str, fallback: str) -> str:
    if config.has_section(section) and config.has_option(section, option):
        return config.get(section, option)
    return fallback

def _build_archive_conninfo() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        conninfo = conninfo_to_dict(database_url)
        conninfo.setdefault("connect_timeout", "10")
        return make_conninfo(**conninfo)

    return make_conninfo(
        host=os.getenv('HLU_POSTGRES_HOST', os.getenv('PGHOST', _config_value('Database', 'Host', 'localhost'))),
        port=os.getenv('HLU_POSTGRES_PORT', os.getenv('PGPORT', _config_value('Database', 'Port', '5432'))),
        dbname=os.getenv('HLU_POSTGRES_DB', os.getenv('PGDATABASE', _config_value('Database', 'Name', 'hll_logs'))),
        user=os.getenv('HLU_POSTGRES_USER', os.getenv('PGUSER', _config_value('Database', 'User', 'hll'))),
        password=os.getenv('HLU_POSTGRES_PASSWORD', os.getenv('PGPASSWORD', _config_value('Database', 'Password', 'hll'))),
        connect_timeout="10",
    )


def _connect_archive_database(retries: int = 8, delay_seconds: float = 2.0):
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(_build_archive_conninfo())
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                break
            logging.warning("PostgreSQL connection attempt %s/%s failed: %s", attempt, retries, exc)
            time.sleep(delay_seconds)
    assert last_exc is not None
    raise last_exc


archive_database = _connect_archive_database()
archive_cursor = archive_database.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS "db_version" (
	"format_version"	INTEGER DEFAULT 1 NOT NULL
);
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "credentials" (
	"guild_id"	VARCHAR(18) NOT NULL,
	"name"	VARCHAR(80) NOT NULL,
	"address"	VARCHAR(25),
	"port"	INTEGER,
	"password"	VARCHAR(50)
);
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "hss_api_keys" (
	"guild_id"	VARCHAR(18) NOT NULL,
	"tag"	VARCHAR(10) NOT NULL,
	"key"	VARCHAR(120)
);
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "sessions" (
	"guild_id"	INTEGER NOT NULL,
	"name"	VARCHAR(40) NOT NULL,
	"start_time"	VARCHAR(30) NOT NULL,
	"end_time"	VARCHAR(30) NOT NULL,
	"deleted"	BOOLEAN NOT NULL CHECK ("deleted" IN (0, 1)) DEFAULT 0,
	"credentials_id"	INTEGER,
    FOREIGN KEY(credentials_id) REFERENCES credentials(ROWID) ON DELETE SET NULL
);
""")

cursor.execute("""
INSERT INTO "db_version" ("format_version")
    SELECT 1 WHERE NOT EXISTS(
        SELECT 1 FROM "db_version"
    );
""")

database.commit()

archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "archive_db_version" (
	"format_version"	INTEGER DEFAULT 1 NOT NULL
);
""")
archive_cursor.execute("""
INSERT INTO "archive_db_version" ("format_version")
    SELECT 1 WHERE NOT EXISTS(
        SELECT 1 FROM "archive_db_version"
    );
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "capture_sessions" (
    "session_id" BIGINT PRIMARY KEY,
    "guild_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "credentials_id" INTEGER,
    "server_name" TEXT,
    "server_address" TEXT,
    "server_port" INTEGER,
    "modifiers" INTEGER NOT NULL DEFAULT 0,
    "start_time" TIMESTAMPTZ NOT NULL,
    "planned_end_time" TIMESTAMPTZ,
    "actual_end_time" TIMESTAMPTZ,
    "is_auto_session" BOOLEAN NOT NULL DEFAULT FALSE,
    "deleted" BOOLEAN NOT NULL DEFAULT FALSE,
    "created_at" TIMESTAMPTZ NOT NULL,
    "updated_at" TIMESTAMPTZ NOT NULL
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "session_raw_logs" (
    "id" BIGSERIAL PRIMARY KEY,
    "session_id" BIGINT NOT NULL,
    "event_time" TIMESTAMPTZ,
    "log_line" TEXT NOT NULL,
    "raw_line" TEXT NOT NULL,
    "parsed" BOOLEAN NOT NULL DEFAULT FALSE,
    "parse_error" TEXT,
    "created_at" TIMESTAMPTZ NOT NULL
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "session_iterations" (
    "id" BIGSERIAL PRIMARY KEY,
    "session_id" BIGINT NOT NULL,
    "captured_at" TIMESTAMPTZ NOT NULL,
    "server_name" TEXT,
    "server_map" TEXT,
    "server_state" TEXT,
    "round_start" TIMESTAMPTZ,
    "max_players" INTEGER,
    "player_count" INTEGER NOT NULL DEFAULT 0,
    "squad_count" INTEGER NOT NULL DEFAULT 0,
    "team1_score" INTEGER,
    "team2_score" INTEGER,
    "snapshot_json" JSONB NOT NULL
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "session_iteration_players" (
    "iteration_id" BIGINT NOT NULL,
    "session_id" BIGINT NOT NULL,
    "captured_at" TIMESTAMPTZ NOT NULL,
    "player_id" TEXT NOT NULL,
    "name" TEXT,
    "platform" TEXT,
    "eos_id" TEXT,
    "team_id" INTEGER,
    "team_name" TEXT,
    "team_faction" TEXT,
    "squad_id" INTEGER,
    "squad_name" TEXT,
    "role" TEXT,
    "loadout" TEXT,
    "level" INTEGER,
    "kills" INTEGER,
    "deaths" INTEGER,
    "combat_score" INTEGER,
    "offense_score" INTEGER,
    "defense_score" INTEGER,
    "support_score" INTEGER,
    "is_alive" BOOLEAN NOT NULL DEFAULT FALSE,
    "is_spectator" BOOLEAN NOT NULL DEFAULT FALSE,
    "location_x" REAL,
    "location_y" REAL,
    "location_z" REAL,
    "joined_at" TIMESTAMPTZ
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "session_events" (
    "id" BIGSERIAL PRIMARY KEY,
    "session_id" BIGINT NOT NULL,
    "event_time" TIMESTAMPTZ NOT NULL,
    "event_type" TEXT NOT NULL,
    "log_json" JSONB,
    "event_json" JSONB NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "matches" (
    "id" BIGSERIAL PRIMARY KEY,
    "session_id" BIGINT NOT NULL,
    "start_time" TIMESTAMPTZ,
    "end_time" TIMESTAMPTZ,
    "map_name" TEXT,
    "server_name" TEXT,
    "allied_score" INTEGER,
    "axis_score" INTEGER,
    "status" TEXT NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL,
    "updated_at" TIMESTAMPTZ NOT NULL
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "manual_uploads" (
    "id" BIGSERIAL PRIMARY KEY,
    "session_name" TEXT NOT NULL,
    "server_name" TEXT,
    "source_filename" TEXT NOT NULL,
    "content_type" TEXT,
    "file_format" TEXT NOT NULL,
    "uploader_name" TEXT,
    "notes" TEXT,
    "start_time" TIMESTAMPTZ,
    "end_time" TIMESTAMPTZ,
    "log_count" INTEGER NOT NULL DEFAULT 0,
    "parsed_log_count" INTEGER NOT NULL DEFAULT 0,
    "raw_text" TEXT NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL,
    "metadata_json" JSONB NOT NULL DEFAULT '{}'::jsonb
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "manual_upload_logs" (
    "id" BIGSERIAL PRIMARY KEY,
    "upload_id" BIGINT NOT NULL,
    "event_time" TIMESTAMPTZ,
    "event_type" TEXT,
    "log_json" JSONB NOT NULL
);
""")
archive_cursor.execute("""
CREATE TABLE IF NOT EXISTS "manual_upload_matches" (
    "id" BIGSERIAL PRIMARY KEY,
    "upload_id" BIGINT NOT NULL,
    "map_name" TEXT,
    "start_time" TIMESTAMPTZ,
    "end_time" TIMESTAMPTZ,
    "duration_seconds" INTEGER,
    "allied_score" INTEGER,
    "axis_score" INTEGER,
    "player_count" INTEGER NOT NULL DEFAULT 0
);
""")
archive_database.commit()


def rename_table_columns(table_name: str, old: list[str], new: list[str]):
    if len(old) != len(new):
        raise ValueError("Old and new column lists must be of the same length")

    table_name_new = table_name + "_new"

    # Create a new table with the proper columns
    cursor.execute(LogLine._get_create_query(table_name_new, _explicit_fields=new))
    # Copy over the values
    query = Query.into(table_name_new).columns(*new).from_(table_name).select(*old)
    cursor.execute(str(query))
    # Drop the old table
    cursor.execute(str(Query.drop_table(table_name)))
    # Rename the new table
    cursor.execute(f'ALTER TABLE "{table_name_new}" RENAME TO "{table_name}";')

    database.commit()

    added = [c for c in new if c not in old]
    removed = [c for c in old if c not in new]
    logging.info("Altered table %s: Added %s and removed %s", table_name, added, removed)


def update_table_columns(table_name: str, old: list[str], new: list[str], defaults: dict = {}):
    table_name_new = table_name + "_new"

    # Create a new table with the proper columns
    cursor.execute(LogLine._get_create_query(table_name_new, _explicit_fields=new))
    # Copy over the values
    to_copy = [c for c in old if c in new]
    query = Query.into(table_name_new).columns(*to_copy).from_(table_name).select(*to_copy)
    cursor.execute(str(query))
    # Insert defaults
    defaults = {col: val for col, val in defaults.items() if col in new and col not in old}
    if defaults:
        query = Query.update(table_name_new)
        for col, val in defaults.items():
            query = query.set(col, val)
        cursor.execute(str(query))
    # Drop the old table
    cursor.execute(str(Query.drop_table(table_name)))
    # Rename the new table
    cursor.execute(f'ALTER TABLE "{table_name_new}" RENAME TO "{table_name}";')

    database.commit()

    added = [c for c in new if c not in old]
    removed = [c for c in old if c not in new]
    logging.info("Altered table %s: Added %s and removed %s", table_name, added, removed)

cursor.execute("SELECT format_version FROM db_version")
db_version: int = cursor.fetchone()[0]

# Very dirty way of doing this, I know
if db_version > DB_VERSION:
    logging.warn('Unrecognized database format version! Expected %s but got %s. Certain functionality may be broken. Did you downgrade versions?', DB_VERSION, db_version)
elif db_version < DB_VERSION:
    logging.info('Outdated database format version! Expected %s but got %s. Migrating now...', DB_VERSION, db_version)

    if db_version < 2:
        # Add a "modifiers" column to the "sessions" table
        cursor.execute('ALTER TABLE "sessions" ADD "modifiers" INTEGER DEFAULT 0 NOT NULL;')
    
    if db_version < 3:
        # Add "player_score_X" columns to all session logs tables
        cursor.execute('SELECT name FROM sqlite_master WHERE type = "table" AND name LIKE "session%";')
        for (table_name,) in cursor.fetchall():
            try:
                int(table_name[7:])
            except ValueError:
                if table_name.endswith('_new'):
                    logging.warning('Found table with name %s, you will likely need to manually delete it', table_name)
                continue

            update_table_columns(table_name,
                old=['event_time', 'type', 'player_name', 'player_id', 'player_team', 'player_role', 'player2_name', 'player2_id',
                     'player2_team', 'player2_role', 'weapon', 'old', 'new', 'team_name', 'squad_name', 'message'],
                new=['event_time', 'type', 'player_name', 'player_id', 'player_team', 'player_role', 'player_combat_score',
                     'player_offense_score', 'player_defense_score', 'player_support_score', 'player2_name', 'player2_id', 'player2_team',
                     'player2_role', 'weapon', 'old', 'new', 'team_name', 'squad_name', 'message']
            )
    
    if db_version < 4:
        # Add a "default_modifiers" column to the "credentials" table
        cursor.execute('ALTER TABLE "credentials" ADD "default_modifiers" INTEGER DEFAULT 0 NOT NULL;')

    if db_version < 5:
        # Add a "autosession_enabled" column to the "credentials" table
        cursor.execute('ALTER TABLE "credentials" ADD "autosession_enabled" BOOLEAN NOT NULL CHECK ("autosession_enabled" IN (0, 1)) DEFAULT 0;')

        # Remove NOT NULL constraint from "end_time" column of the "sessions" table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS "sessions_new" (
            "guild_id"	INTEGER NOT NULL,
            "name"	VARCHAR(40) NOT NULL,
            "start_time"	VARCHAR(30) NOT NULL,
            "end_time"	VARCHAR(30),
            "deleted"	BOOLEAN NOT NULL CHECK ("deleted" IN (0, 1)) DEFAULT 0,
            "credentials_id"	INTEGER,
            "modifiers" INTEGER DEFAULT 0 NOT NULL,
            FOREIGN KEY(credentials_id) REFERENCES credentials(ROWID) ON DELETE SET NULL
        );
        """)
        cursor.execute('INSERT INTO "sessions_new" SELECT * FROM "sessions";')
        cursor.execute(str(Query.drop_table("sessions")))
        cursor.execute('ALTER TABLE "sessions_new" RENAME TO "sessions";')

    if db_version < 6:
        # Create a new table with the proper columns
        table_name = 'hss_api_keys'
        table_name_new = f'{table_name}_new'
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name_new}" (
                "guild_id"	VARCHAR(18) NOT NULL,
                "tag"	VARCHAR(10) NOT NULL,
                "key"	VARCHAR(120)
            );
            """)
        # Copy over the values
        to_copy = ['guild_id', 'tag', 'key']
        query = Query.into(table_name_new).columns(*to_copy).from_(table_name).select(*to_copy)
        cursor.execute(str(query))
        # Drop the old table
        cursor.execute(str(Query.drop_table(table_name)))
        # Rename the new table
        cursor.execute(f'ALTER TABLE "{table_name_new}" RENAME TO "{table_name}";')

        database.commit()

    if db_version < 7:
        # Rename "player_steamid" and "player2_steamid" columns to "player_id" and "player2_id" respectively in all session logs tables
        cursor.execute('SELECT name FROM sqlite_master WHERE type = "table" AND name LIKE "session%";')
        for (table_name,) in cursor.fetchall():
            try:
                int(table_name[7:])
            except ValueError:
                if table_name.endswith('_new'):
                    logging.warning('Found table with name %s, you will likely need to manually delete it', table_name)
                continue

            rename_table_columns(table_name,
                old=['event_time', 'type', 'player_name', 'player_steamid', 'player_team', 'player_role', 'player_combat_score',
                     'player_offense_score', 'player_defense_score', 'player_support_score', 'player2_name', 'player2_steamid', 'player2_team',
                     'player2_role', 'weapon', 'old', 'new', 'team_name', 'squad_name', 'message'],
                new=['event_time', 'event_type', 'player_name', 'player_id', 'player_team', 'player_role', 'player_combat_score',
                     'player_offense_score', 'player_defense_score', 'player_support_score', 'player2_name', 'player2_id', 'player2_team',
                     'player2_role', 'weapon', 'old', 'new', 'team_name', 'squad_name', 'message']
            )

    cursor.execute('UPDATE "db_version" SET "format_version" = ?', (DB_VERSION,))
    database.commit()
    logging.info('Migrated database to format version %s!', DB_VERSION)


def insert_many_logs(sess_id: int, logs: Sequence['LogLine'], sort: bool = True):
    sess_name = f"session{int(sess_id)}"
    table = Table(sess_name)

    if sort:
        logs = sorted(logs, key=lambda log: log.event_time)

    # Insert the logs
    insert_query = table
    for log in logs:
        insert_query = insert_query.insert(*log.model_dump().values())
    cursor.execute(str(insert_query))
    
    database.commit()

def delete_logs(sess_id: int):
    sess_name = f"session{int(sess_id)}"

    # Drop the table
    drop_query = Query.drop_table(sess_name)
    cursor.execute(str(drop_query))
    
    database.commit()


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _isoformat(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _serialize_log_line(log: LogLine) -> Jsonb:
    return Jsonb(log.model_dump(mode='json'))


def _serialize_snapshot(snapshot: Any) -> dict[str, Any]:
    server = snapshot.server
    return {
        "server": server.model_dump(mode='json') if server else None,
        "teams": [team.model_dump(mode='json') for team in snapshot.teams],
        "squads": [squad.model_dump(mode='json') for squad in snapshot.squads],
        "disbanded_squads": [squad.model_dump(mode='json') for squad in snapshot.disbanded_squads],
        "players": [player.model_dump(mode='json') for player in snapshot.players],
        "disconnected_players": [player.model_dump(mode='json') for player in snapshot.disconnected_players],
    }


def sync_capture_session(session: Any):
    now = datetime.now(timezone.utc)
    archive_cursor.execute(
        """
        INSERT INTO capture_sessions (
            session_id, guild_id, name, credentials_id, server_name, server_address, server_port,
            modifiers, start_time, planned_end_time, actual_end_time, is_auto_session, deleted, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(session_id) DO UPDATE SET
            guild_id = excluded.guild_id,
            name = excluded.name,
            credentials_id = excluded.credentials_id,
            server_name = excluded.server_name,
            server_address = excluded.server_address,
            server_port = excluded.server_port,
            modifiers = excluded.modifiers,
            start_time = excluded.start_time,
            planned_end_time = excluded.planned_end_time,
            actual_end_time = excluded.actual_end_time,
            is_auto_session = excluded.is_auto_session,
            deleted = excluded.deleted,
            updated_at = excluded.updated_at
        """,
        (
            session.id,
            session.guild_id,
            session.name,
            session.credentials.id if session.credentials else None,
            session.credentials.name if session.credentials else None,
            session.credentials.address if session.credentials else None,
            session.credentials.port if session.credentials else None,
            session.modifier_flags.value,
            session.start_time,
            None if session.is_auto_session else session.end_time,
            session.end_time if session.active_in() is False else None,
            bool(session.is_auto_session),
            False,
            now,
            now,
        ),
    )
    archive_database.commit()


def mark_capture_session_deleted(session: Any):
    archive_cursor.execute(
        """
        UPDATE capture_sessions
        SET deleted = 1,
            actual_end_time = COALESCE(actual_end_time, %s),
            updated_at = %s
        WHERE session_id = %s
        """,
        (session.end_time, datetime.now(timezone.utc), session.id),
    )
    archive_database.commit()


def insert_raw_logs(session_id: int, raw_logs: Sequence[dict[str, Any]]):
    if not raw_logs:
        return

    now = datetime.now(timezone.utc)
    archive_cursor.executemany(
        """
        INSERT INTO session_raw_logs (session_id, event_time, log_line, raw_line, parsed, parse_error, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                session_id,
                log.get("event_time"),
                log.get("log", ""),
                log.get("raw", log.get("log", "")),
                bool(log.get("parsed", False)),
                log.get("parse_error"),
                now,
            )
            for log in raw_logs
        ],
    )
    archive_database.commit()


def insert_snapshot(session_id: int, captured_at: Any, snapshot: Any):
    payload = _serialize_snapshot(snapshot)
    server = snapshot.server
    team1 = snapshot.teams[0] if len(snapshot.teams) > 0 else None
    team2 = snapshot.teams[1] if len(snapshot.teams) > 1 else None

    archive_cursor.execute(
        """
        INSERT INTO session_iterations (
            session_id, captured_at, server_name, server_map, server_state, round_start, max_players,
            player_count, squad_count, team1_score, team2_score, snapshot_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            session_id,
            captured_at,
            server.name if server else None,
            server.map if server else None,
            server.state if server else None,
            server.round_start if server else None,
            server.max_players if server else None,
            len(snapshot.players),
            len(snapshot.squads),
            team1.score if team1 else None,
            team2.score if team2 else None,
            Jsonb(payload),
        ),
    )
    iteration_id = archive_cursor.fetchone()[0]

    if iteration_id is not None and snapshot.players:
        archive_cursor.executemany(
            """
            INSERT INTO session_iteration_players (
                iteration_id, session_id, captured_at, player_id, name, platform, eos_id, team_id, team_name,
                team_faction, squad_id, squad_name, role, loadout, level, kills, deaths, combat_score,
                offense_score, defense_score, support_score, is_alive, is_spectator, location_x, location_y,
                location_z, joined_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    iteration_id,
                    session_id,
                    captured_at,
                    player.id,
                    player.name,
                    str(player.platform),
                    player.eos_id,
                    player.team_id,
                    team.name if (team := player.get_team()) else None,
                    team.faction if team else None,
                    player.squad_id,
                    squad.name if (squad := player.get_squad()) else None,
                    player.role.name,
                    player.loadout,
                    player.level,
                    player.kills,
                    player.deaths,
                    player.score.combat,
                    player.score.offense,
                    player.score.defense,
                    player.score.support,
                    bool(player.is_alive),
                    bool(player.is_spectator),
                    player.location[0],
                    player.location[1],
                    player.location[2],
                    player.joined_at,
                )
                for player in snapshot.players
            ],
        )

    archive_database.commit()
    return iteration_id


def _parse_match_score(score: str | None) -> tuple[int | None, int | None]:
    if not score or " - " not in score:
        return None, None
    allied, axis = score.split(" - ", 1)
    try:
        return int(allied), int(axis)
    except ValueError:
        return None, None


def _upsert_match_from_event(session_id: int, event_type: str, event_time: Any, event_payload: dict[str, Any], log: LogLine | None):
    now = datetime.now(timezone.utc)

    if event_type == "server_match_start":
        map_name = event_payload.get("map_name") or (log.new if log else None)
        archive_cursor.execute(
            """
            INSERT INTO matches (session_id, start_time, map_name, server_name, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                session_id,
                event_time,
                map_name,
                event_payload.get("server_name"),
                "in_progress",
                now,
                now,
            ),
        )
        return

    if event_type != "server_match_end":
        return

    map_name = event_payload.get("map_name") or (log.new if log else None)
    allied_score, axis_score = _parse_match_score(event_payload.get("score") or (log.message if log else None))
    archive_cursor.execute(
        """
        SELECT id FROM matches
        WHERE session_id = %s AND end_time IS NULL
        ORDER BY start_time DESC NULLS LAST, id DESC
        LIMIT 1
        """,
        (session_id,),
    )
    row = archive_cursor.fetchone()

    if row:
        archive_cursor.execute(
            """
            UPDATE matches
            SET end_time = %s, map_name = COALESCE(map_name, %s), allied_score = %s, axis_score = %s, status = %s, updated_at = %s
            WHERE id = %s
            """,
            (event_time, map_name, allied_score, axis_score, "completed", now, row[0]),
        )
    else:
        archive_cursor.execute(
            """
            INSERT INTO matches (
                session_id, start_time, end_time, map_name, allied_score, axis_score, status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                session_id,
                event_time,
                event_time,
                map_name,
                allied_score,
                axis_score,
                "completed",
                now,
                now,
            ),
        )


def insert_events(session_id: int, events: Sequence[Any], logs: Sequence[LogLine]):
    if not events:
        return

    now = datetime.now(timezone.utc)
    for event, log in zip(events, logs):
        event_payload = event.model_dump(mode='json')
        event_type = event.get_type().name
        archive_cursor.execute(
            """
            INSERT INTO session_events (session_id, event_time, event_type, log_json, event_json, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                session_id,
                event.event_time,
                event_type,
                _serialize_log_line(log),
                Jsonb(event_payload),
                now,
            ),
        )
        _upsert_match_from_event(session_id, event_type, event.event_time, event_payload, log)

    archive_database.commit()


def create_manual_upload(
    *,
    session_name: str,
    server_name: str | None,
    source_filename: str,
    content_type: str | None,
    file_format: str,
    uploader_name: str | None,
    notes: str | None,
    raw_text: str,
    logs: Sequence[LogLine],
    metadata: dict[str, Any] | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    start_time = logs[0].event_time if logs else None
    end_time = logs[-1].event_time if logs else None
    archive_cursor.execute(
        """
        INSERT INTO manual_uploads (
            session_name, server_name, source_filename, content_type, file_format, uploader_name, notes,
            start_time, end_time, log_count, parsed_log_count, raw_text, created_at, metadata_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            session_name,
            server_name,
            source_filename,
            content_type,
            file_format,
            uploader_name,
            notes,
            start_time,
            end_time,
            len(logs),
            len(logs),
            raw_text,
            now,
            Jsonb(metadata or {}),
        ),
    )
    upload_id = archive_cursor.fetchone()[0]

    if logs:
        archive_cursor.executemany(
            """
            INSERT INTO manual_upload_logs (upload_id, event_time, event_type, log_json)
            VALUES (%s, %s, %s, %s)
            """,
            [
                (
                    upload_id,
                    log.event_time,
                    log.event_type,
                    _serialize_log_line(log),
                )
                for log in logs
            ],
        )

    archive_database.commit()
    return upload_id


def create_manual_upload_with_unparsed_file(
    *,
    session_name: str,
    server_name: str | None,
    source_filename: str,
    content_type: str | None,
    file_format: str,
    uploader_name: str | None,
    notes: str | None,
    raw_text: str,
    metadata: dict[str, Any] | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    archive_cursor.execute(
        """
        INSERT INTO manual_uploads (
            session_name, server_name, source_filename, content_type, file_format, uploader_name, notes,
            log_count, parsed_log_count, raw_text, created_at, metadata_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            session_name,
            server_name,
            source_filename,
            content_type,
            file_format,
            uploader_name,
            notes,
            0,
            0,
            raw_text,
            now,
            Jsonb(metadata or {}),
        ),
    )
    upload_id = archive_cursor.fetchone()[0]
    archive_database.commit()
    return upload_id


def insert_manual_upload_matches(upload_id: int, matches: Sequence[dict[str, Any]]):
    if not matches:
        return

    archive_cursor.executemany(
        """
        INSERT INTO manual_upload_matches (
            upload_id, map_name, start_time, end_time, duration_seconds, allied_score, axis_score, player_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                upload_id,
                match.get("map_name"),
                match.get("start_time"),
                match.get("end_time"),
                match.get("duration_seconds"),
                match.get("allied_score"),
                match.get("axis_score"),
                match.get("player_count", 0),
            )
            for match in matches
        ],
    )
    archive_database.commit()


def list_recent_manual_uploads(limit: int = 20) -> list[dict[str, Any]]:
    archive_cursor.execute(
        """
        SELECT id, session_name, server_name, source_filename, file_format, uploader_name, notes,
               start_time, end_time, log_count, parsed_log_count, created_at
        FROM manual_uploads
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = archive_cursor.fetchall()
    keys = (
        "id", "session_name", "server_name", "source_filename", "file_format", "uploader_name", "notes",
        "start_time", "end_time", "log_count", "parsed_log_count", "created_at",
    )
    return [dict(zip(keys, row)) for row in rows]


def list_recent_capture_sessions(limit: int = 20) -> list[dict[str, Any]]:
    archive_cursor.execute(
        """
        SELECT session_id, name, server_name, start_time, planned_end_time, actual_end_time, deleted, created_at
        FROM capture_sessions
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = archive_cursor.fetchall()
    keys = (
        "session_id", "name", "server_name", "start_time", "planned_end_time", "actual_end_time", "deleted", "created_at",
    )
    return [dict(zip(keys, row)) for row in rows]
