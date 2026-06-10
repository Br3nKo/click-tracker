"""Integration test for capacity enforcement against real Redis."""

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
    # Reset cached script so it rebinds to the live client.
    monkeypatch.setattr(queue, "_client", redis_client)
    monkeypatch.setattr(enrichment, "_window_script", None)

    async def call():
        try:
            await fetch_user_data(uuid4())
            return "ok"
        except UserServiceOverloaded:
            return "rejected"

    results = await asyncio.gather(*(call() for _ in range(20)))

    # Excess beyond capacity must be rejected.
    assert results.count("ok") >= 1
    assert results.count("rejected") >= 1
