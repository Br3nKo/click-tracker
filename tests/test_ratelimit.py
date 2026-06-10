"""Tests for the token-bucket rate limiters."""

import asyncio
import time

import pytest

from app import ratelimit
from app.ratelimit import RedisTokenBucket, TokenBucket


@pytest.mark.asyncio
async def test_burst_up_to_capacity_is_immediate():
    """The initial burst (up to capacity) should not block."""
    bucket = TokenBucket(rate=100, capacity=10)
    start = time.monotonic()
    for _ in range(10):
        await bucket.acquire()
    assert time.monotonic() - start < 0.05


@pytest.mark.asyncio
async def test_throttles_to_rate():
    """Beyond capacity, acquisitions are paced at roughly ``rate``/sec."""
    rate = 50
    bucket = TokenBucket(rate=rate, capacity=1)
    start = time.monotonic()
    acquisitions = 11  # 1 free (capacity) + 10 paced at 1/rate each
    for _ in range(acquisitions):
        await bucket.acquire()
    elapsed = time.monotonic() - start
    # 10 paced acquisitions at 1/50s ≈ 0.2s; allow generous scheduling slack.
    assert elapsed >= (acquisitions - 1) / rate * 0.8


@pytest.mark.asyncio
async def test_rejects_non_positive_rate():
    with pytest.raises(ValueError):
        TokenBucket(rate=0)


@pytest.mark.asyncio
async def test_concurrent_acquire_respects_rate():
    """Concurrent callers are collectively throttled to the rate."""
    rate = 100
    bucket = TokenBucket(rate=rate, capacity=1)
    start = time.monotonic()
    await asyncio.gather(*(bucket.acquire() for _ in range(21)))
    elapsed = time.monotonic() - start
    assert elapsed >= 20 / rate * 0.8


class _FakeScript:
    """Returns a scripted sequence of wait-times (ms), mimicking the Lua call."""

    def __init__(self, waits: list[int]) -> None:
        self._waits = list(waits)

    async def __call__(self, keys=None, args=None) -> int:
        return self._waits.pop(0)


class _FakeRedis:
    def __init__(self, waits: list[int]) -> None:
        self._waits = waits

    def register_script(self, lua: str) -> _FakeScript:
        return _FakeScript(self._waits)


@pytest.mark.asyncio
async def test_redis_bucket_grants_immediately_when_available(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(ratelimit.asyncio, "sleep", lambda s: slept.append(s))

    bucket = RedisTokenBucket(_FakeRedis([0]), key="k", rate=200)
    await bucket.acquire()

    assert slept == []  # granted on the first try, no waiting


@pytest.mark.asyncio
async def test_redis_bucket_waits_then_retries(monkeypatch):
    slept: list[float] = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(ratelimit.asyncio, "sleep", fake_sleep)

    # First call: must wait 7ms; second call: granted.
    bucket = RedisTokenBucket(_FakeRedis([7, 0]), key="k", rate=200)
    await bucket.acquire()

    assert slept == [0.007]


@pytest.mark.asyncio
async def test_redis_bucket_rejects_non_positive_rate():
    with pytest.raises(ValueError):
        RedisTokenBucket(_FakeRedis([0]), key="k", rate=0)
