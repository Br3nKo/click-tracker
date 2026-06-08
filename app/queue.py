"""Redis Streams helpers for buffering clicks between ingest and workers.

The stream decouples the ~1000 req/s ingest rate from the ~200 req/s
enrichment throughput. Redis durably holds the backlog; a consumer group
lets multiple workers share the load and re-deliver unacknowledged messages
if a worker crashes.
"""

from typing import Optional

import redis.asyncio as redis
from redis.exceptions import ResponseError

from app.config import settings

_client: Optional[redis.Redis] = None


async def init_client() -> redis.Redis:
    """Create the shared Redis client and ensure the consumer group exists."""
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
        await ensure_group(_client)
    return _client


async def close_client() -> None:
    """Close the shared Redis client."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def get_client() -> redis.Redis:
    """Return the initialised Redis client, raising if it is missing."""
    if _client is None:
        raise RuntimeError("Redis client is not initialised")
    return _client


async def ensure_group(client: redis.Redis) -> None:
    """Create the consumer group, tolerating the case where it exists."""
    try:
        await client.xgroup_create(
            name=settings.stream_key,
            groupname=settings.consumer_group,
            id="0",
            mkstream=True,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def publish_click(fields: dict) -> str:
    """Append a click to the stream and return its message id.

    The stream is trimmed approximately to ``stream_max_len`` entries to bound
    Redis memory use under sustained overload.
    """
    client = get_client()
    return await client.xadd(
        settings.stream_key,
        fields,
        maxlen=settings.stream_max_len,
        approximate=True,
    )
