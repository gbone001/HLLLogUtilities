import os
from datetime import datetime, timezone, timedelta

import pytest
import pytest_asyncio
import sqlalchemy as sa

from db import models
from lib.logs import LogLine
from lib.storage.postgres import PostgresConfig, PostgresStorage, SessionCreateParams, ensure_partitions

POSTGRES_TEST_DSN = os.getenv("POSTGRES_TEST_DSN")
pytestmark = pytest.mark.skipif(not POSTGRES_TEST_DSN, reason="POSTGRES_TEST_DSN not set")


def _prepare_schema():
    engine = sa.create_engine(POSTGRES_TEST_DSN, isolation_level="AUTOCOMMIT")
    with engine.begin() as conn:
        conn.execute(sa.text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA public"))
        models.metadata.create_all(conn)
        conn.execute(sa.text("CREATE TABLE IF NOT EXISTS session_logs_default PARTITION OF session_logs DEFAULT"))
    engine.dispose()


@pytest_asyncio.fixture
async def storage():
    if not POSTGRES_TEST_DSN:
        pytest.skip("POSTGRES_TEST_DSN not configured")
    _prepare_schema()
    config = PostgresConfig(dsn=POSTGRES_TEST_DSN, pool_min_size=1, pool_max_size=2)
    store = PostgresStorage(config)
    await store.connect()
    await ensure_partitions(store, datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0))
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_create_session_roundtrip(storage: PostgresStorage):
    params = SessionCreateParams(
        guild_id=1,
        name="pytest",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc) + timedelta(hours=1),
        modifier_flags=42,
    )
    session_id = await storage.create_session(params)
    assert session_id > 0

    await storage.update_session_end(session_id, params.end_time + timedelta(hours=1))
    await storage.mark_session_deleted(session_id)
    removed = await storage.cleanup_expired_sessions(datetime.now(timezone.utc) + timedelta(days=1))
    assert session_id in removed


@pytest.mark.asyncio
async def test_insert_and_fetch_logs(storage: PostgresStorage):
    session_id = await storage.create_session(
        SessionCreateParams(
            guild_id=10,
            name="logs",
            start_time=datetime.now(timezone.utc),
            end_time=None,
            is_auto=True,
        )
    )
    lines = [
        LogLine(
            event_time=datetime.now(timezone.utc) + timedelta(seconds=i),
            event_type="TEST",
            player_name=f"P{i}",
            player_id=str(i),
            message="hello",
        )
        for i in range(3)
    ]
    inserted = await storage.insert_logs(session_id, lines)
    assert inserted == len(lines)

    fetched = await storage.fetch_logs(session_id)
    assert len(fetched) == len(lines)
    assert fetched[0].player_name == "P0"

    deleted = await storage.purge_session_logs(session_id)
    assert deleted == len(lines)
    await storage.delete_session(session_id)
