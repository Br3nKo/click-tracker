"""Enrichment cache: remembers user lookups so repeat users don't hit the
slow user service again.

Since the same user clicks many times and their username/email rarely change,
caching by ``user_id`` lets effective throughput far exceed the 200 req/s
service limit — only cache *misses* spend the service budget. Entries expire
after ``cache_ttl_seconds`` to bound staleness (see README "Cache staleness").

The cache reuses the Redis connection owned by ``app.queue``.
"""

import json
from typing import Optional
from uuid import UUID

from app import queue
from app.config import settings


def _key(user_id: UUID | str) -> str:
    return f"user:{user_id}"


async def get_user(user_id: UUID | str) -> Optional[dict]:
    """Return cached enrichment data for a user, or None on a cache miss."""
    client = queue.get_client()
    raw = await client.get(_key(user_id))
    if raw is None:
        return None
    return json.loads(raw)


async def set_user(user_id: UUID | str, data: dict) -> None:
    """Cache enrichment data for a user with the configured TTL."""
    client = queue.get_client()
    await client.set(
        _key(user_id), json.dumps(data), ex=settings.cache_ttl_seconds
    )
