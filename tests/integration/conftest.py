"""Fixtures for integration tests against real Redis / PostgreSQL.

Tests skip (not fail) when a service is unreachable, so the default
``pytest`` run stays green without infrastructure.
"""

import asyncio
from pathlib import Path

import pytest
import pytest_asyncio
import redis.asyncio as redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app import db
from app.config import settings

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


async def _cleanup(client: redis.Redis) -> None:
    """Remove only the keys these tests use, so we don't flush a shared DB."""
    await client.delete(settings.rate_limit_key)
    window_keys = await client.keys(f"{settings.user_service_window_key}:*")
    if window_keys:
        await client.delete(*window_keys)


@pytest_asyncio.fixture
async def redis_client():
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis not available (set REDIS_URL to run integration tests)")
    await _cleanup(client)
    try:
        yield client
    finally:
        await _cleanup(client)
        await client.aclose()


def _alembic_upgrade_head() -> None:
    """Run in a worker thread: Alembic's env.py uses its own asyncio loop."""
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_PROJECT_ROOT / "alembic"))
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture
async def pg():
    """``db`` on real PostgreSQL; migrates so the partitioned schema runs."""
    probe = create_async_engine(settings.database_url)
    try:
        async with probe.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        await probe.dispose()
        pytest.skip("Postgres not available (set DATABASE_URL to run)")
    await probe.dispose()

    await asyncio.to_thread(_alembic_upgrade_head)

    await db.init_engine()
    session_factory = db.get_sessionmaker()
    async with session_factory() as session:
        await session.execute(text("TRUNCATE clicks"))
        await session.commit()

    try:
        yield db
    finally:
        await db.close_engine()
