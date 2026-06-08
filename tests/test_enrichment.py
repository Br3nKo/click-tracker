"""Tests for the faked user-service enrichment."""

from uuid import UUID, uuid4

import pytest

from app import enrichment
from app.config import settings
from app.enrichment import UserServiceOverloaded, fetch_user_data


@pytest.mark.asyncio
async def test_returns_username_and_email():
    user_id = UUID("12345678-1234-1234-1234-1234567890ab")
    data = await fetch_user_data(user_id)
    assert set(data) == {"username", "email"}
    assert data["username"].startswith("user_")
    assert "@" in data["email"]


@pytest.mark.asyncio
async def test_is_deterministic_for_same_user():
    user_id = UUID("12345678-1234-1234-1234-1234567890ab")
    assert await fetch_user_data(user_id) == await fetch_user_data(user_id)


@pytest.mark.asyncio
async def test_rejects_calls_over_capacity(monkeypatch):
    """The mock raises once per-second calls exceed its capacity (a 429)."""
    monkeypatch.setattr(enrichment.queue, "get_client", lambda: object())

    async def over_capacity(client):
        return settings.user_service_capacity + 1

    monkeypatch.setattr(enrichment, "_incr_window", over_capacity)

    with pytest.raises(UserServiceOverloaded):
        await fetch_user_data(uuid4())


@pytest.mark.asyncio
async def test_serves_calls_within_capacity(monkeypatch):
    monkeypatch.setattr(enrichment.queue, "get_client", lambda: object())

    async def within_capacity(client):
        return 1

    monkeypatch.setattr(enrichment, "_incr_window", within_capacity)

    data = await fetch_user_data(uuid4())
    assert "username" in data
