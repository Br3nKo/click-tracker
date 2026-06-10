"""SQLAlchemy ORM models.

Postgres requires the partition key in every PK, hence composite (id,
timestamp); ``clicks`` is range-partitioned by day on ``timestamp``.
"""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Identity, Index, Text, Uuid, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base; ``Base.metadata`` is the Alembic target."""


class Click(Base):
    """An enriched click as stored in PostgreSQL."""

    __tablename__ = "clicks"
    __table_args__ = (
        # Primary lookup: by user, newest first.
        Index("clicks_user_id_timestamp_idx", "user_id", text("timestamp DESC")),
        {"postgresql_partition_by": "RANGE (timestamp)"},
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True
    )
    user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    shop_url: Mapped[str] = mapped_column(Text, nullable=False)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
