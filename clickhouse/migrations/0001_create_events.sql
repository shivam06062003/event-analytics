-- ClickHouse DDL is not transactional, so every migration must be safe to
-- re-run (IF NOT EXISTS) in case a previous run died halfway.
CREATE TABLE IF NOT EXISTS events
(
    project_id        UUID,
    event_id          UUID,
    -- LowCardinality: dictionary-encodes values with few distinct entries
    -- (event names), shrinking storage and speeding up filters/grouping.
    event             LowCardinality(String),
    distinct_id       String,
    user_id           Nullable(String),
    anonymous_id      Nullable(String),
    timestamp         DateTime64(3, 'UTC'),
    client_timestamp  Nullable(DateTime64(3, 'UTC')),
    received_at       DateTime64(3, 'UTC'),
    properties        String CODEC(ZSTD(3)),
    context           String CODEC(ZSTD(3)),
    ip                Nullable(String),
    -- Lineage: exactly where in Kafka each row came from. Invaluable when
    -- debugging "why is this event here twice?" or replaying a range.
    kafka_partition   UInt16,
    kafka_offset      UInt64,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- Rows with an identical sorting key are collapsed during background merges,
-- keeping the latest ingested_at. This removes exact redeliveries (the
-- processor crashed after inserting but before committing its offsets).
ENGINE = ReplacingMergeTree(ingested_at)
-- Monthly partitions: old months can be dropped in O(1) for retention.
PARTITION BY toYYYYMM(timestamp)
-- The sort key is the primary index. Queries almost always filter by project,
-- then a time range, then event name, so that is the order. event_id last
-- makes the key unique per event (needed for the dedup above).
ORDER BY (project_id, toDate(timestamp), event, cityHash64(distinct_id), event_id)
