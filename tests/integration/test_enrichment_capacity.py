"""Integration test for the mock service's capacity enforcement (real Redis).

Drives the fixed-window Lua counter for real: firing more calls than the
configured capacity within one second must yield rejections.
"""

import asyncio
from uuid import uuid4

import pytest

from app import enrichment, queue
from app.config import settings
from app.enrichment import UserServiceOverloaded, fetch_user_data

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_rejects_calls_beyond_capacity(redis_client, monkeypatch):
    capacity = 5
    monkeypatch.setattr(settings, "user_service_capacity", capacity)
    monkeypatch.setattr(settings, "user_service_latency", 0.0)
    # Point the service's capacity counter at the live client, and reset the
    # cached script so it binds to this client.
    monkeypatch.setattr(queue, "_client", redis_client)
    monkeypatch.setattr(enrichment, "_window_script", None)

    async def call():
        try:
            await fetch_user_data(uuid4())
            return "ok"
        except UserServiceOverloaded:
            return "rejected"

    results = await asyncio.gather(*(call() for _ in range(20)))

    # Some calls succeed, the excess is rejected — capacity is actually enforced.
    assert results.count("ok") >= 1
    assert results.count("rejected") >= 1
