from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

metadata = sa.MetaData()

credentials = sa.Table(
    "credentials",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("guild_id", sa.BigInteger, nullable=False),
    sa.Column("name", sa.String(80), nullable=False),
    sa.Column("address", sa.String(64), nullable=False),
    sa.Column("port", sa.Integer, nullable=False),
    sa.Column("password", sa.String(128), nullable=False),
    sa.Column("default_modifiers", sa.BigInteger, nullable=False, server_default="0"),
    sa.Column("autosession_enabled", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
)

sessions = sa.Table(
    "sessions",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("guild_id", sa.BigInteger, nullable=False),
    sa.Column("name", sa.String(80), nullable=False),
    sa.Column("start_time", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("end_time", sa.TIMESTAMP(timezone=True)),
    sa.Column("is_auto", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("credentials_id", sa.BigInteger, sa.ForeignKey("credentials.id", ondelete="SET NULL")),
    sa.Column("modifier_flags", sa.BigInteger, nullable=False, server_default="0"),
    sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
)

session_logs = sa.Table(
    "session_logs",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("session_id", sa.BigInteger, sa.ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False),
    sa.Column("event_time", sa.TIMESTAMP(timezone=True), nullable=False, primary_key=True),
    sa.Column("event_type", sa.Text, nullable=False),
    sa.Column("player_name", sa.Text),
    sa.Column("player_id", sa.Text),
    sa.Column("player_team", sa.Text),
    sa.Column("player_role", sa.Text),
    sa.Column("player_combat_score", sa.Integer),
    sa.Column("player_offense_score", sa.Integer),
    sa.Column("player_defense_score", sa.Integer),
    sa.Column("player_support_score", sa.Integer),
    sa.Column("player2_name", sa.Text),
    sa.Column("player2_id", sa.Text),
    sa.Column("player2_team", sa.Text),
    sa.Column("player2_role", sa.Text),
    sa.Column("weapon", sa.Text),
    sa.Column("old", sa.Text),
    sa.Column("new", sa.Text),
    sa.Column("team_name", sa.Text),
    sa.Column("squad_name", sa.Text),
    sa.Column("message", sa.Text),
    postgresql_partition_by="RANGE (event_time)",
)
sa.Index("ix_session_logs_session_time", session_logs.c.session_id, session_logs.c.event_time, session_logs.c.id)

session_modifiers = sa.Table(
    "session_modifiers",
    metadata,
    sa.Column("session_id", sa.BigInteger, sa.ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False),
    sa.Column("modifier", sa.String(64), nullable=False),
    sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.true()),
    sa.PrimaryKeyConstraint("session_id", "modifier"),
)

autosession_state = sa.Table(
    "autosession_state",
    metadata,
    sa.Column("credentials_id", sa.BigInteger, sa.ForeignKey("credentials.id", ondelete="CASCADE"), primary_key=True),
    sa.Column("last_seen_playercount", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_error", sa.Text),
    sa.Column("failed_attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("cooldown_until", sa.TIMESTAMP(timezone=True)),
)

hss_api_keys = sa.Table(
    "hss_api_keys",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("guild_id", sa.BigInteger, nullable=False),
    sa.Column("tag", sa.String(10), nullable=False),
    sa.Column("key", sa.String(120)),
    sa.UniqueConstraint("guild_id", "tag", name="uq_hss_api_key"),
)

__all__ = [
    "metadata",
    "credentials",
    "sessions",
    "session_logs",
    "session_modifiers",
    "autosession_state",
    "hss_api_keys",
]
