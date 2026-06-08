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

The key idea is **decoupling**: the ingest API accepts clicks as fast as they
arrive and drops them into a durable Redis Stream, returning `202 Accepted`
immediately. A pool of workers drains the stream, enriches each click, and
bulk-inserts the results.

During sustained overload the stream backlog grows — that's expected and
acceptable for click analytics (eventual consistency). The backlog is bounded
by `STREAM_MAX_LEN` to cap Redis memory.

### Beating the 200 req/s ceiling

Raw enrichment is limited to the user service's **200 req/s**. Two
optimisations let the *effective* click throughput far exceed that without ever
overrunning the service — only genuine cache misses spend the budget:

- **Cache** ([app/cache.py](app/cache.py)) — the same user clicks repeatedly
  and their username/email rarely change, so results are cached in Redis by
  `user_id` (TTL `CACHE_TTL_SECONDS`). Repeat users become free cache hits.
  With an 80% hit rate, effective throughput is `200 / (1 − 0.8) = 1000/s`.
- **In-batch coalescing** ([app/worker.py](app/worker.py)) — within one batch,
  duplicate `user_id`s are resolved with a single lookup, then applied to all
  their clicks.

**Cache staleness:** a cached entry can lag reality if a user changes their
email mid-TTL, so the stored email is a point-in-time snapshot, fresh to within
`CACHE_TTL_SECONDS`. For click analytics this is an acceptable trade of
freshness for throughput; if strict freshness were required, the fix is
event-based invalidation (the user service publishes change events that evict
the key). Note the cache only narrows a staleness window the queue backlog
already introduces.

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

## Tests

There are two tiers:

- **Unit tests** — fake Redis and PostgreSQL in memory, so no infrastructure is
  required. They cover the rate limiter wrapper, enrichment, the API endpoints,
  caching + in-batch coalescing, and the worker's enrich → persist → ack cycle
  (including the "don't ack on failure" path).
- **Integration tests** (marked `integration`) — run against a **real Redis**
  to exercise the Lua distributed token bucket and the mock's capacity
  enforcement. They skip automatically if Redis is unreachable.

```bash
pip install -r requirements.txt

pytest -m "not integration"          # unit tests only (default-safe)
pytest                               # all; integration tests skip without Redis

# Integration tests against a throwaway Redis:
docker run -d --rm -p 6379:6379 redis:7-alpine
REDIS_URL=redis://localhost:6379/0 pytest -m integration
```

### CI

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push to
`main` and every pull request, in two jobs: **unit-tests** (no services) and
**integration-tests** (with a `redis:7-alpine` service container).

## Configuration

All settings are environment variables (see [app/config.py](app/config.py)):
`DATABASE_URL`, `REDIS_URL`, `STREAM_KEY`, `CONSUMER_GROUP`,
`USER_SERVICE_RATE_LIMIT`, `WORKER_CONCURRENCY`, `BATCH_SIZE`,
`BATCH_FLUSH_SECONDS`, `USER_SERVICE_LATENCY`, `CACHE_TTL_SECONDS`.

## Reliability notes

- **At-least-once delivery.** Messages are `XACK`-ed only after a successful
  DB insert. A worker crash mid-batch leaves messages pending; they are
  redelivered (a startup `XAUTOCLAIM` sweep, noted as a follow-up, would
  reclaim messages from dead consumers). Duplicates are possible — add a unique
  constraint or idempotency key if exactly-once matters.
- **Horizontal scaling.** Both the API and workers are stateless; run more
  replicas behind a load balancer / the shared consumer group. The rate limit
  to the user service is enforced by a *distributed* token bucket
  ([app/ratelimit.py](app/ratelimit.py)) keyed in Redis, so the aggregate stays
  at 200 req/s no matter how many workers run — each just gets a smaller share.
- **Capacity is tested, not assumed.** The faked user service
  ([app/enrichment.py](app/enrichment.py)) enforces its own 200/s ceiling and
  raises `UserServiceOverloaded` (like a 429) if exceeded. With the distributed
  limiter the service is never overrun; a per-worker limiter run across N
  workers would trip it — which is exactly the bug the shared limiter fixes.
- **Backpressure.** If the user service degrades, the backlog grows but ingest
  stays healthy. The stream `MAXLEN` bounds memory; beyond that, oldest
  unprocessed entries are trimmed (a dead-letter stream is the production fix).

## Archiving old data (design)

Click data is append-only and loses query value with age, so the goal is to
keep the hot table small while retaining history cheaply. Daily range
partitioning (already in the schema) makes this clean:

1. **Partition by day.** Each day's clicks live in their own partition
   (`clicks_2026_06_08`, …). A scheduled job (e.g. `pg_partman` or a nightly
   cron) pre-creates tomorrow's partition and is the only thing writing DDL.

2. **Detach + export cold partitions.** Once a partition ages past the hot
   window (say 30 days), `ALTER TABLE clicks DETACH PARTITION` removes it from
   the live table instantly (metadata-only, no row scan). Export it to columnar
   files (Parquet) in object storage (S3/GCS) for cheap long-term retention and
   ad-hoc analytics (Athena / BigQuery / DuckDB).

3. **Drop the partition.** After the export is verified, `DROP TABLE` the
   detached partition. This reclaims space instantly with no `DELETE` bloat,
   no `VACUUM` pressure, and no lock on the live table.

4. **Tiering by age (optional).** hot (Postgres, indexed) → warm (compressed
   partition or a separate cheaper Postgres) → cold (Parquet in S3, queried on
   demand). Lifecycle policies on the bucket can expire truly old data.

Why partitioning over bulk `DELETE`: deleting millions of rows is slow, bloats
the table, and demands aggressive vacuuming. Detaching/dropping a partition is
an `O(1)` metadata operation. Range partitioning on `timestamp` also lets the
planner prune partitions for time-bounded queries.
