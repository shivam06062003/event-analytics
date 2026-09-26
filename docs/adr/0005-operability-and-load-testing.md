# ADR 0005: Operability (metrics, tracing, quotas) and load testing

- **Status:** Accepted
- **Date:** 2026-09-27

## Metrics (Prometheus)

- The API exposes `/metrics`. It runs several uvicorn processes, so it uses
  Prometheus **multiprocess mode**: each process writes shared files that the
  endpoint merges. The processor serves its own metrics on port 9100.
- **Only low-cardinality labels.** Routes are labelled by template, never by
  raw path, and there is **no per-project label**. A label per tenant
  multiplies every metric by the number of tenants, which is how monitoring
  systems fall over. Per-project detail belongs in logs, traces and
  ClickHouse. A test checks that no project id appears in `/metrics`.
- **The numbers users actually feel:**
  - `processor_consumer_lag{partition}`: how far behind the processors are
  - `event_ingest_to_stored_seconds`: how long after the API accepted an
    event it became queryable (freshness)
- **Other metrics:**
  - ingestion outcomes: accepted, rejected for validation, rejected by the
    tracking plan, rejected by quota
  - time spent waiting for Kafka acknowledgements
  - query latency per kind, and cache hit/miss/coalesced counts
  - processor batches, rows, duplicates, dead letters by reason, insert
    latency and sink failures
- **Six alert rules**, validated with `promtool`: lag growing, events not
  becoming queryable, ClickHouse inserts failing, dead letters arriving, an
  ingestion error rate above 1%, and the quota backend being down.
- The Grafana dashboard is provisioned automatically.

## Tracing across Kafka

- The API injects the request's W3C `traceparent` into each Kafka message's
  **headers**. Headers are metadata, so the event payload is unchanged, and a
  test asserts that.
- The processor handles a *batch* of messages that came from many requests. A
  span has only one parent, so the batch's CONSUMER span carries **span
  links** to each producer context (up to 64). This is the standard
  OpenTelemetry pattern for batch consumers. The ClickHouse insert is a child
  of the batch span.
- **Verified in Jaeger:** a real request's trace ID appears as a link on the
  processor batch span that stored its events.
- The sampler starts traces only at SERVER and CONSUMER spans, so background
  polling doesn't flood Jaeger. That lesson came from the ledger project.

## Per-project quotas

- Quotas use an atomic Lua token bucket in Redis, the same script as the
  ledger's rate limiter, with **cost > 1**: ingestion is charged **per
  event**, not per request. Otherwise a client could send 500-event batches
  and multiply its allowance by 500.
- The quota is charged before any other work, so a tenant over quota costs
  almost nothing.
- If a batch is larger than the whole burst, it can never succeed. It gets a
  429 **without** `Retry-After`, because waiting wouldn't help.
- There's a separate query bucket. Both fail open if Redis is down, and a
  metric plus an alert track that.

## Load testing: what the numbers taught us

k6 (`make loadtest`) ran on an M1 MacBook with **8 GB RAM**, with Docker
limited to 8 vCPUs and 3.8 GB. The load generator, API, broker, processors,
ClickHouse, Postgres, Redis and the observability stack all share that
machine.

| Step | Setup | Result | Bottleneck found |
|---|---|---|---|
| 1 | 100-event batches, broker 1 core / 512 MB, 1 processor | ~8.3k events/s avg (9.3k peak), 0 errors | **API CPU** (4 processes at 451%) |
| 2 | Profile of one request path | ~160 µs CPU per event in-process; **13 µs in aiokafka's pure-Python murmur2** | Partitioner hash, per-event timestamp formatting |
| 3 | 500-event batches | 13.9k events/s peak | **The load generator**: k6 used as much CPU as the API, making random UUIDs |
| 4 | After fixes, broker still 1 core | ~6.6k avg; **p95 Kafka ack ≥ 1 s** | **The broker** |
| 5 | **Broker 2 cores / 1 GB, 1 processor** | **~17.4k events/s sustained, 0 failures** (1,409 × 500-event requests) | One processor (~5k rows/s) drains the backlog in about 3.5 minutes |
| 6 | Same, with **3 processors** | 16–22% of requests failed; Redpanda logged reactor stalls | **The laptop itself**: consumers starve the broker of CPU |

**Fixes kept in the code:**

- **A memoized partitioner.** It keeps Kafka's exact default murmur2
  key-to-partition mapping, so other Kafka clients still agree with us. A
  test compares it against aiokafka's `DefaultPartitioner` over 2,000 keys.
- Timestamps formatted once per batch instead of once per event.
- Cheap unique IDs in the k6 script.
- Redpanda defaults to 2 cores and 1 GB.

**Conclusions:**

- **The 20k target was not reached on this laptop. The best was ~17.4k
  events/s** with durable, acknowledged writes (`acks=all`) and zero errors.
  Each step moved the bottleneck, and each was measured rather than guessed.
- **Scaling consumers only helps when they don't starve the broker.** On one
  shared machine, three processors made things worse. In production, brokers
  run on dedicated nodes and Kubernetes resource requests/limits isolate
  workloads, which is what Phase 6 configures.
- **Known next levers:**
  - A **faster producer client**. `confluent-kafka` (librdkafka, written in
    C) avoids the per-message Python overhead.
  - **One Kafka message per batch** instead of one per event. That's a
    potential 5–10× gain, but it gives up per-user partitioning, so retries
    would no longer land on the same partition as the original and the
    in-partition dedup guarantee from ADR 0002 would be lost. It's a
    deliberate trade-off we haven't made.
  - Horizontal API replicas behind a load balancer.
