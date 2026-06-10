"""Faked external user service with latency and a per-second capacity limit.

Capacity counter lives in Redis (fixed window per second) so it is enforced
globally across workers. With no Redis client configured, enforcement is
skipped.
"""

import asyncio
from uuid import UUID

from app import queue
from app.config import settings
from app.lua import load_script


class UserServiceOverloaded(Exception):
    """Raised when the (faked) user service is called beyond its capacity."""


# Fixed-window counter; see app/lua/user_service_window.lua.
_WINDOW_LUA = load_script("user_service_window")

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
        return
    count = await _incr_window(client)
    if count > settings.user_service_capacity:
        raise UserServiceOverloaded(
            f"user service over capacity: {count} > {settings.user_service_capacity}"
        )


async def fetch_user_data(user_id: UUID) -> dict:
    """Return enrichment data derived from user_id (stable across calls)."""
    await _enforce_capacity()
    await asyncio.sleep(settings.user_service_latency)
    short = str(user_id).replace("-", "")[:8]
    return {
        "username": f"user_{short}",
        "email": f"{short}@example.com",
    }
