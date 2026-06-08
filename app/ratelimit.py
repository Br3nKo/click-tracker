"""Token-bucket rate limiters used to throttle calls to the slow user service.

``TokenBucket`` is in-process (one event loop). ``RedisTokenBucket`` is
distributed: all worker processes sharing a key are throttled to a single
*aggregate* rate, so scaling out workers does not multiply the request rate to
the service. The worker uses the Redis variant; the in-process one remains for
single-process use and tests.
"""

import asyncio
import time
from typing import Protocol


class RateLimiter(Protocol):
    """Anything with an awaitable ``acquire`` that blocks until permitted."""

    async def acquire(self, tokens: float = 1.0) -> None: ...


class TokenBucket:
    """In-process token bucket allowing ``rate`` operations per second.

    Tokens accrue continuously up to ``capacity``. ``acquire`` blocks until a
    token is available, so callers are smoothly throttled to ``rate``.
    """

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


# Atomic token-bucket step in Redis. Uses the server's clock (TIME) so all
# workers agree on elapsed time, and refills/consumes in a single round trip.
# Returns 0 when granted, otherwise the milliseconds to wait before retrying.
_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + (now - ts) * rate)

local wait_ms = 0
if tokens >= requested then
  tokens = tokens - requested
else
  wait_ms = math.ceil(((requested - tokens) / rate) * 1000)
  if wait_ms < 1 then wait_ms = 1 end
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil((capacity / rate) * 1000) + 1000)
return wait_ms
"""


class RedisTokenBucket:
    """Distributed token bucket shared by all workers via a Redis key.

    The aggregate rate across every process using the same ``key`` is capped at
    ``rate``; the per-process limit is therefore ``rate / number_of_workers``.
    This is what actually protects the user service's hard throughput limit.
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
