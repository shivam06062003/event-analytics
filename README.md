# Event Analytics

[![CI](https://github.com/shivam06062003/event-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/shivam06062003/event-analytics/actions/workflows/ci.yml)

A real-time product analytics platform, a small Mixpanel or Segment. Apps
send user events. The platform ingests them at high volume through Kafka,
processes them, and answers funnel, retention and segmentation queries from
ClickHouse.

> **Status:** Phase 3 (query API) complete. See [Roadmap](#roadmap) and [benchmarks](#benchmarks).

## Architecture

```mermaid
flowchart LR
    SDK[App / SDK] -- "POST /v1/batch<br/>write key" --> API[Ingestion API]
    API -- "acks=all, idempotent<br/>key = project:user" --> K[(Redpanda<br/>events.raw)]
    K --> P[Processor<br/>consumer group]
    P -- "batched INSERT<br/>commit after write" --> CH[(ClickHouse<br/>ReplacingMergeTree)]
    P -- "poison messages" --> DLQ[(events.dlq)]
    P <-. "dedup window" .-> R[(Redis)]
    Q[Query API<br/>read keys] -- "bound params, readonly,<br/>time/memory limits" --> CH
    Q <-. "result cache +<br/>coalescing" .-> R
    API -. "write keys (cached)" .-> PG[(Postgres<br/>metadata)]
```

Dashed components arrive in later phases. Design rationale:
[ADR 0001](docs/adr/0001-architecture-and-ingestion.md) (architecture and ingestion) and
[ADR 0002](docs/adr/0002-stream-processing-and-deduplication.md) (processing and dedup),
[ADR 0003](docs/adr/0003-query-api.md) (query API).

## Highlights

### Query API (Phase 3)

- **Segmentation, funnels, retention.** Segmentation covers counts or unique
  users by hour/day/week with breakdowns and property filters. Funnels use
  `windowFunnel`, so order matters and a window applies, and conversions may
  finish after the range ends. Retention builds weekly or daily cohorts.
- **Injection-proof.** Every user value, including property names, is a
  server-side bound parameter. Queries also run `readonly=2`, which a query
  can't lift. A test fires `') OR 1=1; DROP TABLE events; --` and the table
  survives.
- **Guard rails.** Range and bucket limits are checked before ClickHouse.
  Per-query time and memory limits are enforced inside it (tested against
  real ClickHouse). Breakdowns show the top 10 plus `$other`.
- **Read keys** are separate from the embeddable write keys, never cached,
  and revoked instantly.
- **Redis cache with recency-based TTL** (30 s for live data, 1 h for
  history). Identical concurrent queries are **coalesced** into one
  ClickHouse execution.
- **Two real bugs caught by tests:** a missing property counted as `0`
  (`value <= 50` matched events with no value), and a shared-result mutation
  that crashed coalesced requests.

### Processing (Phase 2)

- **At-least-once, never lossy.** Offsets are committed only after the
  ClickHouse insert succeeds. A test stops the processor mid-outage and
  checks the committed offsets didn't move. Committing first makes the test
  fail.
- **Two-layer deduplication.** Kafka redeliveries (identical rows) are
  collapsed by `ReplacingMergeTree`. Client retries (same `event_id`, new
  `received_at`, so a different sort key) are caught by a Redis window that
  is marked *after* the insert, so a crash can never drop an event. Removing
  the Redis layer makes the retry test fail.
- **Poison messages don't block partitions.** Unparseable or unknown-version
  messages go to `events.dlq` with the original bytes and their source
  partition and offset.
- **Backpressure without rebalance storms.** While ClickHouse is down, the
  processor pauses its partitions but keeps polling, so it stays in the
  consumer group. Kafka buffers, and it resumes automatically.
- **ClickHouse-friendly writes.** One batched insert per poll, a sort key
  matched to query patterns, monthly partitions, and Kafka lineage columns on
  every row.

### Ingestion (Phase 1)

- **Durable ingestion.** `202 Accepted` is returned only after the broker
  acknowledges the write (`acks=all`, idempotent producer). A test with a
  broker that never acknowledges proves the API returns `503`; changing the
  code to fire-and-forget makes that test fail.
- **Effectively-once by design.** Client-generated `event_id`s stay stable
  across retries. Retries are safe because duplicates are removed downstream.
- **Order where it matters.** The partition key `project:user` keeps each
  user's events in order and spreads large projects across partitions (no hot
  partitions).
- **Clock-skew correction.** Wrong device clocks are fixed using `sent_at`
  vs `received_at`.
- **Partial batch acceptance.** Bad events are rejected individually with
  reasons, and good ones still get in. Unknown fields are rejected, which
  catches SDK typos.
- **Abuse limits.** Body size is checked before reading; events per batch
  and property size are capped.
- **Write keys** are hashed, scoped per project, and cached for the hot path
  (with a documented revocation trade-off).
- **Topics as code** (`ensure-topics`, alongside migrations). CI tests run
  against real Postgres and Redpanda.

## Quick start

Prerequisites: Docker Desktop and Python 3.12+.

```bash
make up                                   # postgres, redpanda, migrations + topics, api on :8001
KEY=$(make -s project name="Demo app")    # create a project; prints its write key
make send key=$KEY                        # {"accepted":2,"rejected":[]}
make events                               # the events, now queryable in ClickHouse
make read-key project=<id>                # a key for the query API (id printed by `make project`)
make seed project=<id>                    # 200k users / ~1.1M realistic events in ~20s
make lag                                  # consumer lag per partition
make console                              # browse topics/messages at http://localhost:8081
```

### Local development

```bash
cp .env.example .env
make install && make infra && make migrate
make check                                # lint + typecheck + tests (same as CI)
```

## Ingestion API

`POST /v1/batch` with `Authorization: Bearer <write key>`:

```json
{
  "sent_at": "2026-09-26T10:00:05Z",
  "batch": [
    {
      "event_id": "5f2c7c1e-3d1a-4c5e-9d8e-1a2b3c4d5e6f",
      "event": "checkout_started",
      "user_id": "u-42",
      "timestamp": "2026-09-26T10:00:00Z",
      "properties": {"plan": "pro", "value": 49},
      "context": {"locale": "en-IN"}
    }
  ]
}
```

| Response | Meaning | What the client should do |
|---|---|---|
| `202` `{"accepted": n, "rejected": [...]}` | Accepted events are durably stored | Drop the rejected ones; don't retry them unchanged |
| `400 no_valid_events` | Every event failed validation | Fix the SDK |
| `401` | Missing or invalid write key | |
| `413` / `411` / `422 batch_too_large` | Over a size limit | Send smaller batches |
| `503 ingest_unavailable` + `Retry-After` | Event log unavailable | **Retry the same batch** (same `event_id`s) |

Rules:

- each event needs `event_id` (a UUID, stable across retries), `event`, and
  either `user_id` or `anonymous_id`
- limits: 1 MB body, 500 events, 32 KB of properties per event

## Query API

All endpoints take `Authorization: Bearer <read key>`. Times are ISO 8601; buckets are UTC.

```bash
# Funnel: signup → checkout → purchase within 2 days
curl -s localhost:8001/v1/query/funnel -H "Authorization: Bearer $READ_KEY" -H 'content-type: application/json' -d '{
  "from": "2026-06-01T00:00:00Z", "to": "2026-07-27T00:00:00Z", "window_seconds": 172800,
  "steps": [{"event": "signup"}, {"event": "checkout_started"},
            {"event": "purchase", "filters": [{"property": "value", "operator": "gte", "value": 100}]}]
}' | jq '.steps'
```

| Endpoint | Answers |
|---|---|
| `POST /v1/query/segmentation` | `event`, `interval` (hour/day/week), `measure` (total/unique_users), `breakdown`, `filters` |
| `POST /v1/query/funnel` | `steps` (each with optional `filters`), `window_seconds` |
| `POST /v1/query/retention` | `start_event`, `return_event` (or any), `period` (day/week), `periods` |
| `GET /v1/event-names` | Recent event names by frequency |

Filter operators: `eq`, `neq`, `contains`, `gt`, `gte`, `lt`, `lte`, `is_set`, `is_not_set`.
Every response includes `meta.cached` and `meta.computed_at`.

## Benchmarks

The dataset is 200k users and **1.15M events** over 8 weeks (`make seed`), on an
M1 laptop stack with ClickHouse capped at 1.2 GB:

| Query (8 weeks) | Cold | Cached |
|---|---|---|
| Segmentation: unique users by path, daily | 798 ms | 66 ms |
| Funnel: 3 steps | 366 ms | 46 ms |
| Retention: 8 × 8 weekly cohorts | 1,350 ms | 65 ms |

- The funnel returns 40.1% → 54.95%, matching the generator's 40% and 55%
  probabilities.
- For one project, one week and one event, the sort key reads **18 of 147
  granules**.
- Storage compresses 2.9×.

Details are in [ADR 0003](docs/adr/0003-query-api.md#measured).

## Project layout

```
app/
  api/            Routes, auth (write keys), middleware (request id, size limit), errors
  services/       Ingestion (validation, skew correction, produce), projects/keys
  processor/      Kafka consumer -> ClickHouse: parse, dedup, sink, DLQ, commit
  query/          Safe SQL builders, segmentation/funnel/retention, executor limits, cache
  core/           Config, logging, Postgres, ClickHouse (+ migration runner), Kafka
  models/         Postgres metadata tables
  schemas/        Event and batch schemas
migrations/       Alembic (Postgres)
clickhouse/       ClickHouse migrations, demo-data generator, low-memory server config
tests/            Against real Postgres, Redpanda, ClickHouse and Redis (isolated per session)
docs/adr/         Architecture Decision Records
```

## Roadmap

- [x] **Phase 1: Ingestion.** Batch API, write keys, durable idempotent producer, partitioning, skew correction, partial acceptance, limits.
- [x] **Phase 2: Processing.** Consumer group writes to ClickHouse, two-layer dedup, dead-letter topic, commit after write, pause-based backpressure, lag.
- [x] **Phase 3: Query API.** Segmentation, funnels, retention, read keys, injection-proof SQL, guard rails, caching with coalescing.
- [ ] **Phase 4: Sessions and schemas.** Sessionization, late events, identity merge, tracking plans with schema evolution.
- [ ] **Phase 5: Operability.** Metrics, tracing, per-project quotas, load test (target: 20k+ events/s).
- [ ] **Phase 6: Kubernetes.** kind + Helm, KEDA autoscaling on consumer lag.
