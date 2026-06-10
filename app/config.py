"""Application configuration, sourced from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings shared by the API and the worker."""

    model_config = SettingsConfigDict(env_prefix="", env_file=".env")

    # Async SQLAlchemy engine requires the +asyncpg dialect.
    database_url: str = "postgresql+asyncpg://clicks:clicks@postgres:5432/clicks"
    db_pool_size: int = 10
    db_max_overflow: int = 5

    redis_url: str = "redis://redis:6379/0"
    stream_key: str = "clicks:incoming"
    consumer_group: str = "enrichers"
    # Caps in-memory backlog; stream trimmed approximately to this on write.
    stream_max_len: int = 5_000_000

    # Aggregate ceiling enforced by the distributed token bucket across all
    # workers (shared via rate_limit_key); adding workers does not raise it.
    user_service_rate_limit: float = 200.0
    rate_limit_key: str = "ratelimit:user_service"
    worker_concurrency: int = 50
    batch_size: int = 100
    batch_flush_seconds: float = 1.0

    user_service_latency: float = 0.25
    user_service_capacity: int = 200
    user_service_window_key: str = "userservice:window"

    # Bounds cache staleness when a user changes their email before re-fetch.
    cache_ttl_seconds: int = 1800


settings = Settings()
