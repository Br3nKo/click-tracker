"""Integration tests for the API, with the queue and DB faked in memory."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio

from app import db, main, queue


class FakeQueue:
    """Captures published clicks instead of talking to Redis."""

    def __init__(self) -> None:
        self.published: list[dict] = []
        self._counter = 0

    async def publish_click(self, fields: dict) -> str:
        self.published.append(fields)
        self._counter += 1
        return f"0-{self._counter}"


@pytest.fixture
def fake_queue(monkeypatch):
    fake = FakeQueue()
    monkeypatch.setattr(main.queue, "publish_click", fake.publish_click)
    return fake


@pytest.fixture
def fake_db(monkeypatch):
    """In-memory stand-in for the click store keyed by user_id."""
    store: dict[UUID, list[dict]] = {}

    async def get_clicks_by_user(user_id, limit=100, offset=0):
        rows = store.get(user_id, [])
        return rows[offset : offset + limit]

    monkeypatch.setattr(main.db, "get_clicks_by_user", get_clicks_by_user)
    return store


@pytest_asyncio.fixture
async def client(monkeypatch, fake_queue, fake_db):
    # Skip real connection setup during lifespan startup/shutdown.
    monkeypatch.setattr(db, "init_engine", _noop)
    monkeypatch.setattr(db, "close_engine", _noop)
    monkeypatch.setattr(queue, "init_client", _noop)
    monkeypatch.setattr(queue, "close_client", _noop)

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        async with main.app.router.lifespan_context(main.app):
            yield c


async def _noop(*args, **kwargs):
    return None


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_ingest_accepts_and_enqueues(client, fake_queue):
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": str(uuid4()),
        "shop_url": "https://shop.example.com/item/42",
    }
    resp = await client.post("/clicks", json=payload)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["message_id"]
    assert len(fake_queue.published) == 1
    assert fake_queue.published[0]["shop_url"] == payload["shop_url"]


@pytest.mark.asyncio
async def test_ingest_rejects_invalid_uuid(client):
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": "not-a-uuid",
        "shop_url": "https://shop.example.com",
    }
    resp = await client.post("/clicks", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_user_clicks_returns_stored_rows(client, fake_db):
    user_id = uuid4()
    now = datetime.now(timezone.utc)
    fake_db[user_id] = [
        {
            "id": 1,
            "timestamp": now,
            "user_id": user_id,
            "shop_url": "https://shop.example.com",
            "username": "user_abc",
            "email": "abc@example.com",
            "created_at": now,
        }
    ]
    resp = await client.get(f"/clicks/{user_id}")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 1
    assert rows[0]["username"] == "user_abc"


@pytest.mark.asyncio
async def test_get_user_clicks_empty(client):
    resp = await client.get(f"/clicks/{uuid4()}")
    assert resp.status_code == 200
    assert resp.json() == []
