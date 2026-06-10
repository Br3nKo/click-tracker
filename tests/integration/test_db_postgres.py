"""DB layer integration tests against a real, migrated PostgreSQL."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def _row(user_id, ts, shop="https://shop.example", name="alice", email="a@x.io"):
    return {
        "timestamp": ts,
        "user_id": user_id,
        "shop_url": shop,
        "username": name,
        "email": email,
    }


@pytest.mark.asyncio
async def test_insert_and_read_roundtrip(pg):
    user_id = uuid4()
    now = datetime.now(timezone.utc)

    await pg.insert_clicks([_row(user_id, now, name="alice", email="alice@x.io")])
    rows = await pg.get_clicks_by_user(user_id)

    assert len(rows) == 1
    row = rows[0]
    assert row["user_id"] == user_id
    assert row["username"] == "alice"
    assert row["email"] == "alice@x.io"
    # Identity column and server default populated by Postgres.
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.asyncio
async def test_bulk_insert_orders_newest_first_and_paginates(pg):
    user_id = uuid4()
    base = datetime.now(timezone.utc)
    # Three clicks at increasing timestamps.
    await pg.insert_clicks(
        [_row(user_id, base + timedelta(seconds=i), shop=f"s{i}") for i in range(3)]
    )

    rows = await pg.get_clicks_by_user(user_id)
    assert [r["shop_url"] for r in rows] == ["s2", "s1", "s0"]  # newest first

    page = await pg.get_clicks_by_user(user_id, limit=1, offset=1)
    assert len(page) == 1
    assert page[0]["shop_url"] == "s1"


@pytest.mark.asyncio
async def test_filters_by_user(pg):
    user_a, user_b = uuid4(), uuid4()
    now = datetime.now(timezone.utc)
    await pg.insert_clicks([_row(user_a, now), _row(user_b, now)])

    rows = await pg.get_clicks_by_user(user_a)
    assert len(rows) == 1
    assert rows[0]["user_id"] == user_a


@pytest.mark.asyncio
async def test_empty_for_unknown_user(pg):
    assert await pg.get_clicks_by_user(uuid4()) == []
