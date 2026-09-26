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
    dead_letter_topic: str = "events.dlq"
    dead_letter_partitions: int = 1

    # ClickHouse (event storage)
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "analytics"
    clickhouse_password: str = "analytics"
    clickhouse_database: str = "analytics"
    clickhouse_migrations_dir: str = "clickhouse/migrations"

    # Redis: the processor's window of recently stored event_ids (dedup)
    redis_url: str = "redis://localhost:6381/0"
    dedup_window_seconds: int = 86_400

    # Processor (Kafka consumer group -> ClickHouse)
    processor_group_id: str = "event-processor"
    processor_max_batch: int = 5_000
    processor_poll_timeout_ms: int = 1_000
    processor_retry_max_backoff_seconds: float = 30.0
    processor_heartbeat_path: str = "/tmp/processor-heartbeat"

    # Query API: guard rails that keep one expensive query from hurting everyone
    query_max_execution_seconds: int = 10
    query_max_memory_bytes: int = 500_000_000
    query_max_range_days: int = 366
    query_max_buckets: int = 1_000
    query_breakdown_limit: int = 10
    # Result cache: short TTL when the range touches recent data (still
    # changing), long TTL for purely historical ranges.
    query_cache_ttl_recent_seconds: int = 30
    query_cache_ttl_historical_seconds: int = 3_600

    # Per-project quotas (token buckets in Redis). Ingestion is charged PER
    # EVENT, not per request, so batching can't be used to dodge the limit.
    quota_enabled: bool = True
    ingest_quota_events_per_second: float = 5_000.0
    ingest_quota_burst: int = 50_000
    query_quota_per_second: float = 2.0
    query_quota_burst: int = 20

    # Observability
    processor_metrics_port: int = 9100
    otel_enabled: bool = False
    otel_service_name: str = "event-api"
    otel_exporter_otlp_endpoint: str = "http://localhost:4318/v1/traces"

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
