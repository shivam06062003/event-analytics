# ADR 0002: Stream processing, delivery guarantees and deduplication

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Events sit durably in `events.raw` (ADR 0001). A processor must move them
into ClickHouse so that:

- no event is ever lost
- duplicates don't inflate counts
- one bad message can't stop the pipeline
- a ClickHouse outage causes delay, not data loss or a crash loop

## Decision

A Kafka consumer group (`python -m app.processor`) runs this loop per batch
(up to 5,000 messages):

```
poll → parse → dedup → INSERT (ClickHouse) → dead letters → mark ids (Redis) → COMMIT offsets
```

### 1. At-least-once: commit offsets last

Auto-commit is off. Offsets are committed **only after** the insert and the
dead-letter writes succeed. If anything fails, or the process dies, the batch
is consumed again after restart. Nothing is marked as processed without
having been stored.

- The trade-off is that replays create duplicates, which the next section
  handles.
- **Verified:** a test stops the processor while ClickHouse is down and
  asserts that the committed offsets did not move. It then restarts from them
  and finds the event. Moving the commit before the insert makes that test
  fail: the offset advanced while the data was nowhere.

### 2. Two layers of deduplication

Duplicates arrive in two forms, and one mechanism can't catch both:

| Source | What the copy looks like | Caught by |
|---|---|---|
| **Kafka redelivery**: processor crashed after INSERT, before COMMIT | Byte-identical message, so an identical row | `ReplacingMergeTree`: rows with an identical sort key collapse at merge time |
| **Client retry**: SDK resent after a 503 or timeout | Same `event_id`, but a **new `received_at`**, so a slightly different corrected timestamp, which is part of the sort key | **Redis window** of recently stored `event_id`s |

**Why ReplacingMergeTree alone isn't enough:** it only merges rows whose
sorting key matches exactly. The key includes `toDate(timestamp)` so that
time-range queries can use the primary index. A client retry's timestamp
differs by milliseconds, and it can even fall on a different day, so the two
rows never merge. Putting `event_id` first in the key would fix dedup but
wreck query performance.

**Redis ordering is the subtle part.** Ids are marked **after** the insert
succeeds:

- *Mark first, then insert:* a crash in between means the replay sees the
  event as "already seen" and skips it, so **the event is lost**. That's not
  acceptable.
- *Insert first, then mark:* a crash in between means the replay re-inserts
  the identical row, which is a redelivery duplicate that the table engine
  collapses. That's acceptable.

**No race between consumers:** a retry carries the same partition key as the
original (`project:distinct_id`), so both land on the same partition. One
consumer sees them in order. Duplicates within a single batch are removed in
memory.

**Reading correct counts:** until a background merge runs, redelivery
duplicates can briefly coexist. Queries that need exact counts use `FINAL`
or `uniqExact(event_id)`. Phase 3 chooses per query.

**Window and memory:** 24 hours by default. At our scale, one Redis key per
event is fine. At 20k events per second, it would be about 1.7 billion keys a
day, which is too many. The known alternatives are a shorter window, a Bloom
filter (probabilistic, with rare false positives meaning dropped events), or
per-partition local state such as RocksDB (Segment's approach). This is noted
as a scaling limit.

**Fail-open:** if Redis is down, processing continues and only in-batch
dedup applies. Client-retry duplicates during an outage get through, which we
prefer to stalling ingestion into ClickHouse. There's a test for this.

### 3. Poison messages go to a dead-letter topic

Parsing never raises. Invalid JSON, an unknown `schema_version`, or failed
validation becomes a `DeadLetter`, which is written to `events.dlq` with:

- the **original bytes and key**
- headers: `error`, `source_topic`, `source_partition`, `source_offset`

Without this, one bad message would be retried forever on every restart and
**block its partition**, because a consumer can't move past an offset it
hasn't committed. The DLQ keeps the stream moving and preserves the evidence
for a fix and replay. `schema_version` routing also allows new message
formats to roll out while old ones are still in the topic.

### 4. Backpressure when ClickHouse is down: pause, don't stop polling

On an insert failure, the processor retries with exponential backoff, capped
at 30 seconds. While it waits, it **pauses its partitions but keeps calling
poll**:

- A consumer that stops polling for longer than `max.poll.interval.ms` is
  removed from the group. That triggers a rebalance, other consumers take its
  partitions and hit the same broken ClickHouse, and the group churns.
- Polling paused partitions returns nothing, but it proves the consumer is
  alive.
- Kafka keeps buffering new events. Ingestion is unaffected and only lag
  grows.

A test simulates two failed inserts and checks that the partitions resume on
their own.

### 5. Batching for ClickHouse

There is one `INSERT` per batch. Each insert creates a data part that
ClickHouse later merges, so thousands of tiny inserts per second cause "too
many parts" errors. Batching per poll (up to 5,000 rows) fits ClickHouse's
preference for few, large inserts.

### 6. Table design (`clickhouse/migrations/0001_create_events.sql`)

- `ORDER BY (project_id, toDate(timestamp), event, cityHash64(distinct_id), event_id)`
  matches the common query shape: *this project, this time range, this event*.
- `PARTITION BY toYYYYMM(timestamp)`: retention can drop a whole month at once.
- `LowCardinality(event)` and ZSTD compression on the JSON columns.
- **Lineage columns** `kafka_partition` and `kafka_offset` record exactly
  where each row came from, which is invaluable when debugging duplicates or
  replays.
- Properties are stored as a JSON string for now. Phase 3 evaluates
  ClickHouse's native `JSON` type or materialized columns for hot properties.
- ClickHouse DDL isn't transactional, so every migration uses
  `IF NOT EXISTS` and can safely re-run.

### 7. Consumer lag is the health signal

Each batch logs `lag`, the sum of (high-water mark − position) over the
processor's partitions. `make lag` shows it per partition via
`rpk group describe`. Growing lag means the processors can't keep up: scale
out, up to one replica per partition (6). Phase 5 exports it as a metric, and
Phase 6 autoscales on it with KEDA.

## A bug found along the way

`ensure_topics` (Phase 1) treated every topic creation as successful.
aiokafka does not raise on per-topic errors; it **returns** error codes in
the response. So an existing topic was logged as "created", and a real
failure would also have looked like success. The Phase 1 test only asserted
that the call didn't crash. It now asserts the actual outcome
(`created` or `exists`), and any other error raises.

## Consequences

- Events typically land in ClickHouse within about a second of ingestion
  (one poll interval). Consumer lag is the metric to watch.
- Exact counts need `FINAL` or distinct counting until merges run, which is
  a Phase 3 query-design concern.
- Redis becomes a runtime dependency of the processor, but not of ingestion.
  Losing Redis only weakens dedup.
