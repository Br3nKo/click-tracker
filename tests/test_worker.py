"""Worker tests: coalescing, caching, persistence, ack."""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app import worker
from app.ratelimit import TokenBucket


class FakeCache:
    """In-memory stand-in for the Redis enrichment cache."""

    def __init__(self) -> None:
        self.store: dict[str, dict] = {}

    async def get_user(self, user_id):
        return self.store.get(str(user_id))

    async def set_user(self, user_id, data):
        self.store[str(user_id)] = data


class FakeClient:
    """Captures XACK calls instead of talking to Redis."""

    def __init__(self) -> None:
        self.acked: list[tuple] = []

    async def xack(self, stream, group, *ids):
        self.acked.append(ids)


@pytest.fixture
def harness(monkeypatch):
    """Wire fake cache, fake client, and insert spy into the worker."""
    cache = FakeCache()
    client = FakeClient()
    inserted: list[list[dict]] = []
    calls: list[str] = []  # user_ids the service was actually asked for

    async def fake_insert(rows):
        inserted.append(rows)

    async def counting_fetch(user_id):
        calls.append(user_id)
        return {"username": f"user_{user_id[:4]}", "email": f"{user_id[:4]}@x.io"}

    monkeypatch.setattr(worker.cache, "get_user", cache.get_user)
    monkeypatch.setattr(worker.cache, "set_user", cache.set_user)
    monkeypatch.setattr(worker.queue, "get_client", lambda: client)
    monkeypatch.setattr(worker.db, "insert_clicks", fake_insert)
    monkeypatch.setattr(worker, "fetch_user_data", counting_fetch)

    return {
        "cache": cache,
        "client": client,
        "inserted": inserted,
        "service_calls": calls,
    }


def _msg(message_id: str, user_id: str) -> tuple[str, dict]:
    ts = datetime.now(timezone.utc).isoformat()
    return message_id, {
        "timestamp": ts,
        "user_id": user_id,
        "shop_url": f"https://shop.example/{message_id}",
    }


async def _run(messages, rate=1000.0):
    bucket = TokenBucket(rate=rate, capacity=rate)
    sem = asyncio.Semaphore(10)
    await worker._process_batch(messages, bucket, sem)


@pytest.mark.asyncio
async def test_enriches_persists_and_acks(harness):
    user_id = str(uuid4())
    await _run([_msg("0-1", user_id), _msg("0-2", user_id)])

    assert len(harness["inserted"]) == 1
    rows = harness["inserted"][0]
    assert len(rows) == 2
    assert all("username" in r and "email" in r for r in rows)
    assert harness["client"].acked == [("0-1", "0-2")]


@pytest.mark.asyncio
async def test_coalesces_duplicate_users_in_batch(harness):
    """Duplicate users in a batch are coalesced to one service call."""
    user_a, user_b = str(uuid4()), str(uuid4())
    messages = [_msg("0-1", user_a), _msg("0-2", user_a), _msg("0-3", user_b)]

    await _run(messages)

    assert sorted(harness["service_calls"]) == sorted([user_a, user_b])
    assert len(harness["service_calls"]) == 2  # not 3 — user_a coalesced
    assert len(harness["inserted"][0]) == 3  # all clicks persisted
    assert harness["client"].acked == [("0-1", "0-2", "0-3")]


@pytest.mark.asyncio
async def test_cache_hit_skips_service(harness):
    """A pre-cached user is enriched without calling the service."""
    user_id = str(uuid4())
    harness["cache"].store[user_id] = {"username": "cached", "email": "c@x.io"}

    await _run([_msg("0-1", user_id)])

    assert harness["service_calls"] == []
    assert harness["inserted"][0][0]["username"] == "cached"
    assert harness["client"].acked == [("0-1",)]


@pytest.mark.asyncio
async def test_service_result_is_cached_for_next_batch(harness):
    """A miss populates the cache so the next batch is a hit."""
    user_id = str(uuid4())

    await _run([_msg("0-1", user_id)])
    await _run([_msg("0-2", user_id)])

    assert harness["service_calls"] == [user_id]  # called once across both runs
    assert harness["client"].acked == [("0-1",), ("0-2",)]


@pytest.mark.asyncio
async def test_failed_enrichment_is_not_acked(harness, monkeypatch):
    """On enrichment failure the message is not acked and not stored."""

    async def boom(user_id):
        raise RuntimeError("user service down")

    monkeypatch.setattr(worker, "fetch_user_data", boom)

    await _run([_msg("0-1", str(uuid4()))])

    assert harness["inserted"] == []
    assert harness["client"].acked == []
