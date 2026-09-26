from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Read from environment variables (and `.env` locally)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "event-analytics"
    environment: Literal["local", "test", "production"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_json: bool = True

    # Metadata store (projects, keys). Events never go here: see ADR 0001.
    database_url: str = "postgresql+asyncpg://analytics:analytics@localhost:5433/analytics"
    db_pool_size: int = 5
    db_max_overflow: int = 10

    # Kafka API (Redpanda). The host-facing listener is 19092.
    kafka_bootstrap_servers: str = "localhost:19092"
    raw_events_topic: str = "events.raw"
    raw_events_partitions: int = 6
    kafka_replication_factor: int = 1  # 3 in production, across brokers
    kafka_send_timeout_seconds: float = 10.0

    # Ingestion limits
    max_request_bytes: int = 1_000_000
    max_batch_events: int = 500
    max_properties_bytes: int = 32_000
    # Client clocks can be wrong; see app/services/ingest.py
    max_future_skew_seconds: int = 60

    # How long a validated write key is cached in-process. Trade-off: a revoked
    # key keeps working for up to this long, in exchange for no database lookup
    # on the hot ingestion path.
    write_key_cache_ttl_seconds: float = 60.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
