"""FastAPI application: click ingestion and retrieval.

The ingest endpoint does the minimum possible work — validate and enqueue —
and returns ``202 Accepted`` so it can sustain the ~1000 req/s incoming rate
without waiting on the slow enrichment pipeline.
"""

from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query

from app import db, queue
from app.schemas import ClickIn, ClickOut, IngestAccepted


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open and close the DB pool and Redis client with the app lifecycle."""
    await db.init_engine()
    await queue.init_client()
    yield
    await queue.close_client()
    await db.close_engine()


app = FastAPI(title="Click Tracker", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    """Liveness probe."""
    return {"status": "ok"}


@app.post("/clicks", response_model=IngestAccepted, status_code=202)
async def ingest_click(click: ClickIn) -> IngestAccepted:
    """Accept a click and enqueue it for asynchronous enrichment."""
    message_id = await queue.publish_click(
        {
            "timestamp": click.timestamp.isoformat(),
            "user_id": str(click.user_id),
            "shop_url": click.shop_url,
        }
    )
    return IngestAccepted(message_id=message_id)


@app.get("/clicks/{user_id}", response_model=list[ClickOut])
async def get_user_clicks(
    user_id: UUID,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[ClickOut]:
    """Return enriched clicks for a user, newest first."""
    try:
        records = await db.get_clicks_by_user(user_id, limit=limit, offset=offset)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Database unavailable")
    return [ClickOut(**dict(record)) for record in records]
