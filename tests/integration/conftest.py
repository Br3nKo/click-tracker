"""Fixtures for integration tests that talk to a real Redis.

Set ``REDIS_URL`` to point at a running Redis (docker compose provides one).
If Redis is unreachable, these tests skip rather than fail, so the default
``pytest`` run stays green without infrastructure.
"""

import pytest
import pytest_asyncio
import redis.asyncio as redis

from app.config import settings


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
