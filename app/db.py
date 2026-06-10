"""PostgreSQL access layer on SQLAlchemy's async engine.

Writes use Core bulk INSERT to keep per-row overhead off the hot path.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings
from app.models import Click

_engine: Optional[AsyncEngine] = None
_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None


async def init_engine() -> AsyncEngine:
    """Create the shared async engine and session factory if absent."""
    global _engine, _sessionmaker
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
        )
        _sessionmaker = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def close_engine() -> None:
    """Dispose of the shared engine."""
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _sessionmaker = None


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """Return the initialised session factory, raising if it is missing."""
    if _sessionmaker is None:
        raise RuntimeError("Database engine is not initialised")
    return _sessionmaker


async def insert_clicks(rows: list[dict]) -> None:
    """Bulk-insert enriched clicks."""
    if not rows:
        return
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        await session.execute(insert(Click), rows)
        await session.commit()


async def get_clicks_by_user(
    user_id: UUID, limit: int = 100, offset: int = 0
) -> list[dict]:
    """Return enriched clicks for a user, newest first."""
    session_factory = get_sessionmaker()
    stmt = (
        select(
            Click.id,
            Click.timestamp,
            Click.user_id,
            Click.shop_url,
            Click.username,
            Click.email,
            Click.created_at,
        )
        .where(Click.user_id == user_id)
        .order_by(Click.timestamp.desc())
        .limit(limit)
        .offset(offset)
    )
    async with session_factory() as session:
        result = await session.execute(stmt)
        return [dict(row) for row in result.mappings().all()]
