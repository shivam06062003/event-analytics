# ADR 0001: System architecture and ingestion design

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

We are building a product analytics platform, a small version of Mixpanel,
Amplitude or Segment. Client apps send user events (page views, signups,
purchases). The platform must:

1. accept events from many devices at high volume, cheaply and reliably
2. process them (validate, de-duplicate, enrich, group into sessions)
3. answer analytical questions over millions of events in milliseconds:
   funnels, retention, segmentation

These goals pull in different directions. Ingestion needs to be fast and
always available. Processing needs to be correct and able to replay.
Queries need a storage engine built for scanning and aggregating.

## Decision: separate the write path, the processing and the read path

```
SDK → Ingestion API → Kafka (events.raw) → Processor → ClickHouse ← Query API
                          (durable log)      (consumer group)
Postgres: projects, keys, schemas (metadata only)
```

- **Ingestion API** (Phase 1): validates, fixes timestamps, appends to the log,
  and answers `202`. It does no slow work.
- **Kafka as the backbone.** It's a durable, replayable log that decouples
  producers from consumers. If ClickHouse or the processors are down or slow,
  ingestion keeps working and events wait in Kafka. Re-processing, for
  example after a bug fix, means resetting a consumer group's offsets.
- **ClickHouse for events** (Phase 2). It's a column-oriented analytical
  database: aggregating a few columns over billions of rows reads only those
  columns, compressed. Postgres, which stores whole rows, would scan far more.
- **Postgres for metadata** (projects, keys, tracking plans): small,
  relational and transactional. Each workload gets the database suited to it.

## Phase 1 decisions: ingestion

### Redpanda instead of Apache Kafka

Redpanda speaks the Kafka protocol, so any Kafka client works unchanged, and
switching to managed Kafka later is a config change. It runs as one binary
with no ZooKeeper or separate KRaft controllers, and it can be capped at
512 MB, which matters on an 8 GB laptop.

### `202 Accepted` only after a durable write

The producer uses `acks=all`, so every in-sync replica must have the message.
It is also **idempotent**: the broker de-duplicates retried sends using a
producer ID and sequence number. The API awaits every acknowledgement before
answering. So `202` means "durably in the log", not "in a memory buffer".

A test enforces this: a broker that accepts a send but never acknowledges it
must produce a `503`. When the code was changed to fire-and-forget, that test
failed with a `202`.

**Trade-off:** each request pays one broker round trip. `linger_ms=5` batches
messages across concurrent requests to limit the cost.

### At-least-once end to end, with client-generated event IDs

On a `503`, or a timeout the client can't interpret, the SDK resends the
**same batch with the same `event_id`s**. Some of those events may already be
in the log. Duplicates are expected by design and removed downstream by
`event_id` (Phase 2). At-least-once delivery plus idempotent processing gives
effectively-once counting, without distributed transactions.

### Partition key = `project_id:distinct_id`

Kafka orders messages only within a partition, and a key always maps to the
same partition. With this key:

- **each user's events stay in order**, which sessionization in Phase 4
  depends on
- **a large project spreads across all partitions.** Keying by project alone
  would put a big customer's entire traffic on one partition, and so on one
  consumer: the "hot partition" problem.

Tests check that a user's events always land on one partition. With keys
removed, that test fails.

**Known limit:** when an anonymous user logs in, their `distinct_id` changes,
so events before and after login may sit on different partitions. Identity
merging is handled at processing and query time (Phase 4).

### Partitions are the unit of parallelism

`events.raw` has 6 partitions. A consumer group can run at most one active
consumer per partition, so the partition count caps processing parallelism.
Increasing it later changes which partition a key maps to, which breaks
per-key ordering during the change. So we choose a count with headroom. Topics
are created by code (`ensure-topics`, run alongside migrations), not by broker
auto-creation, so partition counts are deliberate.

### Clock-skew correction

Device clocks are often wrong, by minutes or even years. The absolute time is
unreliable, but the difference between two readings of the *same* clock is
accurate. The client sends `sent_at` along with each event's `timestamp`:

```
timestamp = received_at - (sent_at - client_timestamp)
```

The original client timestamp is also kept, for debugging. Timestamps still
in the future after correction are clamped to `received_at`.

### Partial batch acceptance

Each event is validated individually. Valid events are accepted, and invalid
ones are returned in `rejected` with their index and reasons. One malformed
event, typically an SDK bug, shouldn't make the client drop or endlessly retry
the 499 good events in the same batch. Unknown fields are rejected, not
ignored (`extra="forbid"`), so a typo like `propertes` is caught instead of
silently losing data. A batch with no valid events gets a `400`.

### Limits

- request body 1 MB, checked from `Content-Length` **before reading**; chunked
  uploads are refused
- 500 events per batch
- 32 KB of properties per event

The ingestion endpoint is open to every device, so unbounded input is a
memory-exhaustion risk.

### Write keys

A write key can only *append* events to its own project. Write keys are
embedded in apps and web pages, so we treat them as semi-public. Read access
will use separate keys (Phase 3).

- **Stored as SHA-256 hashes.** The keys are high-entropy random values, not
  passwords, and are checked on every request.
- **Cached in-process for 60 seconds.** This removes a Postgres lookup from
  the hottest path. The cost is that a revoked key keeps working for up to 60
  seconds on each API instance. That's acceptable for append-only write keys,
  and a test documents the behaviour. It would not be acceptable for read
  keys.

## Consequences

- Events are queryable shortly after ingestion, not instantly (eventual
  consistency). Kafka consumer lag is the key health metric (Phase 5).
- Consumers must be idempotent, because duplicates are part of the contract.
- The stack is heavier (Kafka, ClickHouse, Postgres), in exchange for
  independent scaling and failure isolation of ingestion, processing and
  queries.
