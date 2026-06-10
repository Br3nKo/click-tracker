# Click Tracker

A high-throughput API for ingesting link clicks, enriching them via a slow
external user service, and persisting them to PostgreSQL.

## The problem

- **~1000 req/s** of incoming clicks.
- Each click must be enriched with `username` + `email` from an external user
  service that only sustains **~200 req/s**.
- Incoming rate ≫ enrichment rate, so clicks **cannot** be enriched inline.

## Architecture

```
        1000 req/s                buffered                ≤200 req/s (misses only)
client ───────────▶ Ingest API ───────────▶ Redis Stream ───────────▶ Workers ──▶ PostgreSQL
                    (202 Accepted,          (durable backlog,         (cache + coalesce,  ▲
                     validate + enqueue)     consumer group)           rate-limited        │
                                                                       enrichment,         │
                                                                       batch insert)       │
                                                            ▲ cache hit                    │
                                              Redis cache ──┘                              │
client ◀───────────────────────────────────────────────── GET /clicks/{user_id} ─────────┘
```

### Why these choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| API | FastAPI (async) | Non-blocking I/O; ingest does minimal work. |
| Buffer | Redis Streams | Durable, consumer groups for scaling + crash redelivery. |
| Store | PostgreSQL (partitioned) | Daily range partitioning → cheap archiving + fast `user_id` lookups. |
| ORM / migrations | SQLAlchemy 2.0 (async) + Alembic | ORM model is the schema source of truth; Alembic versions the DDL. |
| Throttle | Distributed token bucket (Redis) | Caps the *aggregate* enrichment rate at 200 req/s across all workers. |
| Cache | Redis (TTL) | Repeat users skip the slow service → effective throughput ≫ 200/s. |

## API

### `POST /clicks`
Accepts a click and enqueues it.

```json
{
  "timestamp": "2026-06-08T10:00:00Z",
  "user_id": "12345678-1234-1234-1234-1234567890ab",
  "shop_url": "https://shop.example.com/item/42"
}
```
Returns `202 Accepted` with `{ "status": "accepted", "message_id": "..." }`.

### `GET /clicks/{user_id}`
Returns enriched clicks for a user, newest first. Supports `limit` (1–1000,
default 100) and `offset`.

### `GET /health`
Liveness probe.

## Running

```bash
docker compose up --build
# API on http://localhost:8000  (docs at /docs)
# 2 workers by default; scale with:
docker compose up --scale worker=4
```

A one-off `migrate` service runs `alembic upgrade head` once Postgres is
healthy; the `api` and `worker` services wait for it to finish before starting.

To run migrations manually (e.g. for local dev against a running Postgres):

```bash
alembic upgrade head      # apply
alembic downgrade -1      # roll back one revision
```

The schema is defined as a SQLAlchemy ORM model in [app/models.py](app/models.py)
(the source of truth); the partitioning and DEFAULT partition are applied by the
hand-written initial migration in
[alembic/versions/0001_initial.py](alembic/versions/0001_initial.py).

### Try it

```bash
curl -X POST http://localhost:8000/clicks \
  -H 'Content-Type: application/json' \
  -d '{"timestamp":"2026-06-08T10:00:00Z","user_id":"12345678-1234-1234-1234-1234567890ab","shop_url":"https://shop.example.com"}'

curl http://localhost:8000/clicks/12345678-1234-1234-1234-1234567890ab
```

### Load / overload testing

[scripts/load_test.py](scripts/load_test.py) sustains a target request rate so
you can watch the backlog build and drain. `--unique-ratio 1.0` makes every
click a new user (all cache misses → paced at ~200/s → backlog grows);
`--unique-ratio 0.0` reuses a small pool (mostly cache hits → drains fast).

```bash
# 800 req/s for 20s, all unique users (maximum backpressure), with live backlog
python scripts/load_test.py --rate 800 --duration 20 --unique-ratio 1.0 \
    --redis-url redis://localhost:6379/0
```

Note: `XACK` does not shrink a stream's `XLEN`, so watch the consumer-group
**lag** (printed with `--redis-url`, or `redis-cli XINFO GROUPS clicks:incoming`),
not `XLEN`.

## Tests

There are two tiers:

- **Unit tests** — fake Redis and PostgreSQL in memory, so no infrastructure is
  required. They cover the rate limiter wrapper, enrichment, the API endpoints,
  caching + in-batch coalescing, and the worker's enrich → persist → ack cycle
  (including the "don't ack on failure" path).
- **Integration tests** (marked `integration`) — run against a **real Redis and
  PostgreSQL**. They exercise what the unit tests mock out: the Lua distributed
  token bucket (including the aggregate-rate *safety* bound), the mock's
  capacity enforcement, the Alembic migration + partitioned schema, the DB
  layer, and the full `POST -> worker -> GET` path end-to-end. Each test skips
  automatically if its service is unreachable.

```bash
pip install -r requirements.txt

pytest -m "not integration"          # unit tests only (default-safe)
pytest                               # all; integration tests skip without services

# Integration tests against throwaway services:
docker run -d --rm -p 6379:6379 redis:7-alpine
docker run -d --rm -p 5432:5432 \
  -e POSTGRES_USER=clicks -e POSTGRES_PASSWORD=clicks -e POSTGRES_DB=clicks \
  postgres:16-alpine
REDIS_URL=redis://localhost:6379/0 \
  DATABASE_URL=postgresql+asyncpg://clicks:clicks@localhost:5432/clicks \
  pytest -m integration
```

### CI

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push to
`main` and every pull request, in two jobs: **unit-tests** (no services) and
**integration-tests** (with `redis:7-alpine` and `postgres:16-alpine` service
containers).

## Configuration

All settings are environment variables (see [app/config.py](app/config.py)):
`DATABASE_URL`, `REDIS_URL`, `STREAM_KEY`, `CONSUMER_GROUP`,
`USER_SERVICE_RATE_LIMIT`, `WORKER_CONCURRENCY`, `BATCH_SIZE`,
`BATCH_FLUSH_SECONDS`, `USER_SERVICE_LATENCY`, `CACHE_TTL_SECONDS`.

## Archiving old data (design)

Daily range partitioning (in the schema) makes archiving cheap:

1. Pre-create daily partitions (`pg_partman` or a cron job).
2. `ALTER TABLE clicks DETACH PARTITION` once past the hot window (e.g. 30 days).
3. Export the detached partition to Parquet in object storage (S3/GCS).
4. `DROP TABLE` the detached partition.
