"""Application configuration, sourced from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings shared by the API and the worker."""

    model_config = SettingsConfigDict(env_prefix="", env_file=".env")

    # PostgreSQL. The async SQLAlchemy engine requires the +asyncpg dialect.
    database_url: str = "postgresql+asyncpg://clicks:clicks@postgres:5432/clicks"
    db_pool_size: int = 10
    db_max_overflow: int = 5

    # Redis
    redis_url: str = "redis://redis:6379/0"
    stream_key: str = "clicks:incoming"
    consumer_group: str = "enrichers"
    # Cap the unacknowledged backlog Redis holds in memory. The stream is
    # trimmed approximately to this many entries on write.
    stream_max_len: int = 5_000_000

    # Worker
    # The user service tolerates only 200 req/s. This is the aggregate ceiling
    # enforced by the *distributed* token bucket across all worker processes
    # (shared via rate_limit_key), so adding workers does not multiply the rate.
    user_service_rate_limit: float = 200.0
    rate_limit_key: str = "ratelimit:user_service"
    worker_concurrency: int = 50
    batch_size: int = 100
    batch_flush_seconds: float = 1.0

    # Faked user service behaviour.
    # Simulated latency of the external call, in seconds.
    user_service_latency: float = 0.25
    # Hard server-side throughput the mock enforces (rejects beyond this/sec).
    user_service_capacity: int = 200
    user_service_window_key: str = "userservice:window"

    # Enrichment cache: how long a user's looked-up data stays valid before we
    # re-fetch it. Bounds staleness if a user changes their email; see README
    # "Cache staleness".
    cache_ttl_seconds: int = 1800


settings = Settings()
