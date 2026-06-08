"""Pydantic schemas for request and response payloads."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class ClickIn(BaseModel):
    """A raw click as posted by the website."""

    timestamp: datetime
    user_id: UUID
    shop_url: str = Field(min_length=1, max_length=2048)


class ClickOut(BaseModel):
    """An enriched click as stored and returned by the read endpoint."""

    id: int
    timestamp: datetime
    user_id: UUID
    shop_url: str
    username: str
    email: str
    created_at: datetime


class IngestAccepted(BaseModel):
    """Acknowledgement returned by the ingest endpoint."""

    status: str = "accepted"
    message_id: str
