"""Enrichment worker: drains the Redis stream, enriches, and persists.

Rate limiter is a distributed token bucket: aggregate request rate to the
service is capped across all worker processes. Only cache misses spend tokens.
"""

import asyncio
import signal
from datetime import datetime

from app import cache, db, queue
from app.config import settings
from app.enrichment import fetch_user_data
from app.ratelimit import RateLimiter, RedisTokenBucket


_shutdown = asyncio.Event()


async def _resolve_user(
    user_id: str, bucket: RateLimiter, sem: asyncio.Semaphore
) -> tuple[str, dict | None]:
    """Resolve a user's enrichment data, returning (user_id, data | None).

    On service failure returns None so messages stay unacked for redelivery.
    """
    cached = await cache.get_user(user_id)
    if cached is not None:
        return user_id, cached

    async with sem:
        await bucket.acquire()
        try:
            data = await fetch_user_data(user_id)
        except Exception:
            return user_id, None

    await cache.set_user(user_id, data)
    return user_id, data


async def _process_batch(
    messages: list[tuple[str, dict]],
    bucket: RateLimiter,
    sem: asyncio.Semaphore,
) -> None:
    """Enrich a batch (coalesced by user), persist it, then acknowledge it."""
    # Coalesce: resolve each distinct user once per batch.
    unique_user_ids = {fields["user_id"] for _, fields in messages}
    resolved = await asyncio.gather(
        *(_resolve_user(uid, bucket, sem) for uid in unique_user_ids)
    )
    user_data = {uid: data for uid, data in resolved if data is not None}

    rows: list[dict] = []
    ack_ids: list[str] = []
    for message_id, fields in messages:
        data = user_data.get(fields["user_id"])
        if data is None:
            # Enrichment failed; leave unacked for redelivery.
            continue
        rows.append(
            {
                "timestamp": datetime.fromisoformat(fields["timestamp"]),
                "user_id": fields["user_id"],
                "shop_url": fields["shop_url"],
                "username": data["username"],
                "email": data["email"],
            }
        )
        ack_ids.append(message_id)

    if not rows:
        return

    await db.insert_clicks(rows)
    client = queue.get_client()
    await client.xack(settings.stream_key, settings.consumer_group, *ack_ids)


async def run() -> None:
    """Main worker loop. Runs until a shutdown signal is received."""
    await db.init_engine()
    client = await queue.init_client()

    bucket = RedisTokenBucket(
        client,
        key=settings.rate_limit_key,
        rate=settings.user_service_rate_limit,
    )
    sem = asyncio.Semaphore(settings.worker_concurrency)
    consumer_name = f"worker-{id(asyncio.current_task())}"

    while not _shutdown.is_set():
        response = await client.xreadgroup(
            groupname=settings.consumer_group,
            consumername=consumer_name,
            streams={settings.stream_key: ">"},
            count=settings.batch_size,
            block=int(settings.batch_flush_seconds * 1000),
        )
        if not response:
            continue

        # response: [(stream_key, [(message_id, {field: value}), ...])]
        _, messages = response[0]
        await _process_batch(messages, bucket, sem)

    await queue.close_client()
    await db.close_engine()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown.set)


def main() -> None:
    """Entry point for ``python -m app.worker``."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)
    loop.run_until_complete(run())


if __name__ == "__main__":
    main()
