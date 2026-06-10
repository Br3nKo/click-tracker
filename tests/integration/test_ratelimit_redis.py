"""Integration tests: shared key throttles to a single aggregate rate."""

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
    """Two buckets on the same key behave as one aggregate limiter."""
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

    # 0.4s floor distinguishes "shared" from independent per-worker buckets.
    assert elapsed >= 0.4


@pytest.mark.asyncio
async def test_aggregate_never_exceeds_rate(redis_client):
    """Concurrent workers cannot collectively exceed the aggregate rate."""
    rate = 100
    n = 51
    key = settings.rate_limit_key
    workers = [
        RedisTokenBucket(redis_client, key=key, rate=rate, capacity=1)
        for _ in range(4)
    ]

    start = time.monotonic()
    await asyncio.gather(
        *(workers[i % len(workers)].acquire() for i in range(n))
    )
    elapsed = time.monotonic() - start

    # (n - 1) paced tokens at `rate`/s; 0.85 slack absorbs scheduling jitter.
    min_seconds = (n - 1) / rate
    assert elapsed >= min_seconds * 0.85
    # Equivalently: effective rate stayed at/under the limit.
    assert n / elapsed <= rate / 0.85
