"""Prometheus metrics for the API and the processor.

Labels are low-cardinality only: routes by TEMPLATE, never raw paths or
project ids. A label per project would create one time series per tenant per
metric, which is how monitoring systems get knocked over. Per-project detail
belongs in logs, traces and ClickHouse, not in metric labels.
"""

from prometheus_client import Counter, Gauge, Histogram

# --- API -----------------------------------------------------------------------
HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
INGEST_EVENTS = Counter(
    "ingest_events_total",
    "Events received, by outcome",
    ["outcome"],  # accepted | rejected_validation | rejected_plan | rejected_quota
)
INGEST_BATCH_SIZE = Histogram(
    "ingest_batch_events",
    "Events per ingestion request",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500),
)
KAFKA_PRODUCE_DURATION = Histogram(
    "ingest_kafka_ack_duration_seconds",
    "Time from enqueueing a batch to all broker acknowledgements",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
)
QUOTA_REJECTIONS = Counter("quota_rejections_total", "Requests refused by a quota", ["kind"])
QUOTA_BACKEND_ERRORS = Counter(
    "quota_backend_errors_total", "Quota checks skipped because Redis failed (fail-open)"
)
QUERY_DURATION = Histogram(
    "query_clickhouse_duration_seconds",
    "ClickHouse execution time of analytics queries",
    ["kind"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
QUERY_CACHE = Counter(
    "query_cache_total",
    "Query cache outcomes",
    ["result"],  # hit | miss | coalesced
)

# --- Processor ---------------------------------------------------------------------
PROCESSOR_BATCHES = Counter("processor_batches_total", "Batches processed")
PROCESSOR_ROWS = Counter("processor_rows_inserted_total", "Rows inserted into ClickHouse")
PROCESSOR_DUPLICATES = Counter("processor_duplicates_total", "Duplicate events dropped")
PROCESSOR_DEAD_LETTERS = Counter(
    "processor_dead_letters_total", "Messages sent to the dead-letter topic", ["reason"]
)
PROCESSOR_INSERT_DURATION = Histogram(
    "processor_insert_duration_seconds",
    "ClickHouse INSERT latency per batch",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
PROCESSOR_SINK_FAILURES = Counter("processor_sink_failures_total", "Failed ClickHouse inserts")
PROCESSOR_LAG = Gauge("processor_consumer_lag", "Messages behind the partition end", ["partition"])
# Freshness: how long after the API accepted an event it became queryable.
# This is the number users actually feel ("why isn't my event showing up?").
END_TO_END_LATENCY = Histogram(
    "event_ingest_to_stored_seconds",
    "received_at -> inserted into ClickHouse",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
)
