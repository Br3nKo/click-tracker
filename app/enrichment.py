"""Faked external user service.

In production this would be an HTTP client calling the real user service. Here
we generate deterministic fake data and simulate two real-world behaviours:

  * latency — each call sleeps for the configured duration;
  * a hard throughput limit — the service rejects calls beyond
    ``USER_SERVICE_CAPACITY`` requests per second with ``UserServiceOverloaded``
    (mimicking a 429). This makes the rate-mismatch constraint *testable*: a
    correctly throttled caller never trips it; a caller that exceeds the limit
    (e.g. a per-worker limiter run across many workers) does.

The capacity counter lives in Redis (a fixed window per wall-clock second) so it
is enforced globally across all worker processes — exactly like a real shared
service. When no Redis client is configured (standalone/unit context),
enforcement is skipped.
"""

import asyncio
from uuid import UUID

from app import queue
from app.config import settings


class UserServiceOverloaded(Exception):
    """Raised when the (faked) user service is called beyond its capacity."""


# Fixed-window counter keyed by the server's current second.
_WINDOW_LUA = """
local t = redis.call('TIME')
local key = KEYS[1] .. ':' .. t[1]
local n = redis.call('INCR', key)
if n == 1 then redis.call('EXPIRE', key, 2) end
return n
"""

_window_script = None


async def _incr_window(client) -> int:
    """Increment and return this second's call count for the service."""
    global _window_script
    if _window_script is None:
        _window_script = client.register_script(_WINDOW_LUA)
    return int(await _window_script(keys=[settings.user_service_window_key], args=[]))


async def _enforce_capacity() -> None:
    """Reject the call if the service's per-second capacity is exceeded."""
    try:
        client = queue.get_client()
    except RuntimeError:
        # No Redis configured (e.g. a unit test calling the service directly):
        # nothing to enforce against.
        return
    count = await _incr_window(client)
    if count > settings.user_service_capacity:
        raise UserServiceOverloaded(
            f"user service over capacity: {count} > {settings.user_service_capacity}"
        )


async def fetch_user_data(user_id: UUID) -> dict:
    """Return enrichment data for a user (username, email).

    Raises ``UserServiceOverloaded`` if called beyond the service's capacity.
    Simulates the slow external call by sleeping for the configured latency.
    Data is derived from the ``user_id`` so it is stable across calls.
    """
    await _enforce_capacity()
    await asyncio.sleep(settings.user_service_latency)
    short = str(user_id).replace("-", "")[:8]
    return {
        "username": f"user_{short}",
        "email": f"{short}@example.com",
    }
