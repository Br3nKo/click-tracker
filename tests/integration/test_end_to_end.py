"""End-to-end: POST /clicks -> stream -> worker -> Postgres -> GET, unmocked."""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from app import main, queue, worker
from app.config import settings
from app.ratelimit import RedisTokenBucket

pytestmark = pytest.mark.integration


async def _drain_once(consumer: str = "test-worker") -> None:
    """Run a single worker batch: read pending, enrich, persist."""
    client = queue.get_client()
    response = await client.xreadgroup(
        groupname=settings.consumer_group,
        consumername=consumer,
        streams={settings.stream_key: ">"},
        count=settings.batch_size,
        block=500,
    )
    assert response, "expected at least one message on the stream"
    _, messages = response[0]
    bucket = RedisTokenBucket(
        client, key=settings.rate_limit_key, rate=settings.user_service_rate_limit
    )
    await worker._process_batch(messages, bucket, asyncio.Semaphore(10))


@pytest.mark.asyncio
async def test_post_then_worker_then_get(pg, redis_client):
    # Empty the stream so we only see this test's message.
    await redis_client.delete(settings.stream_key)

    user_id = str(uuid4())
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "shop_url": "https://shop.example.com/item/42",
    }

    transport = httpx.ASGITransport(app=main.app)
    # Real lifespan opens DB/Redis and creates the consumer group.
    async with main.app.router.lifespan_context(main.app):
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            # 1. Ingest — accepted and enqueued, not yet enriched.
            resp = await c.post("/clicks", json=payload)
            assert resp.status_code == 202

            # 2. Not in the DB yet (enrichment is asynchronous).
            assert (await c.get(f"/clicks/{user_id}")).json() == []

            # 3. Run the worker once.
            await _drain_once()

            # 4. Now the enriched row is readable.
            rows = (await c.get(f"/clicks/{user_id}")).json()
            assert len(rows) == 1
            assert rows[0]["user_id"] == user_id
            assert rows[0]["shop_url"] == payload["shop_url"]
            assert rows[0]["username"].startswith("user_")
            assert "@" in rows[0]["email"]
