"""Integration tests for the distributed token bucket against a real Redis.

These exercise the Lua script itself (the unit tests only cover the Python
wrapper), and prove the key property: two buckets sharing a key are throttled
to a single *aggregate* rate — i.e. scaling workers does not multiply the rate.
"""

import asyncio
import time

import pytest

from app.config import settings
from app.ratelimit import RedisTokenBucket

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_single_bucket_throttles_to_rate(redis_client):
    rate = 20
    bucket = RedisTokenBucket(
        redis_client, key=settings.rate_limit_key, rate=rate, capacity=1
    )

    start = time.monotonic()
    for _ in range(11):  # 1 free (capacity) + 10 paced at 1/rate
        await bucket.acquire()
    elapsed = time.monotonic() - start

    # 10 paced tokens at 20/s ≈ 0.5s. Generous lower bound to avoid flakiness.
    assert elapsed >= 10 / rate * 0.8


@pytest.mark.asyncio
async def test_aggregate_rate_shared_across_buckets(redis_client):
    """Two buckets on the same key behave as one — the whole point of the fix."""
    rate = 20
    key = settings.rate_limit_key
    b1 = RedisTokenBucket(redis_client, key=key, rate=rate, capacity=1)
    b2 = RedisTokenBucket(redis_client, key=key, rate=rate, capacity=1)

    async def grab(bucket, n):
        for _ in range(n):
            await bucket.acquire()

    start = time.monotonic()
    await asyncio.gather(grab(b1, 6), grab(b2, 5))  # 11 tokens total
    elapsed = time.monotonic() - start

    # Shared: ~10 paced tokens / 20 per s ≈ 0.5s.
    # If the buckets were independent (the bug), it would be ~half that — so a
    # 0.4s floor distinguishes "shared" from "per-worker".
    assert elapsed >= 0.4
