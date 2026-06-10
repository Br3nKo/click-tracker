"""Token-bucket rate limiters throttling calls to the user service."""

import asyncio
import time
from typing import Protocol

from app.lua import load_script


class RateLimiter(Protocol):
    """Anything with an awaitable ``acquire`` that blocks until permitted."""

    async def acquire(self, tokens: float = 1.0) -> None: ...


class TokenBucket:
    """In-process token bucket allowing ``rate`` operations per second."""

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self._rate = rate
        self._capacity = capacity if capacity is not None else rate
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._updated
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._updated = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until ``tokens`` are available, then consume them."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self._rate
            await asyncio.sleep(wait)


# Atomic token-bucket step; see app/lua/token_bucket.lua.
_TOKEN_BUCKET_LUA = load_script("token_bucket")


class RedisTokenBucket:
    """Distributed token bucket shared by all workers via a Redis key.

    Aggregate rate across all processes on the same ``key`` is capped at
    ``rate``, so scaling out workers cannot exceed the service's limit.
    """

    def __init__(
        self, client, key: str, rate: float, capacity: float | None = None
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self._key = key
        self._rate = rate
        self._capacity = capacity if capacity is not None else rate
        self._script = client.register_script(_TOKEN_BUCKET_LUA)

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until ``tokens`` are available across all workers."""
        while True:
            wait_ms = int(
                await self._script(
                    keys=[self._key],
                    args=[self._rate, self._capacity, tokens],
                )
            )
            if wait_ms <= 0:
                return
            await asyncio.sleep(wait_ms / 1000)
