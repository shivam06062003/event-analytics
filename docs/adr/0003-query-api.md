# ADR 0003: Query API (segmentation, funnels, retention)

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Product teams ask three kinds of questions:

1. *How often does X happen?* (segmentation)
2. *Where do users drop off?* (funnels)
3. *Do users come back?* (retention)

The query API must answer these interactively over millions of events. It
must not leak data between projects, must be safe from user input, and must
not let one heavy query degrade the system for everyone.

## Decisions

### Read keys, separate from write keys

Write keys are embedded in apps and web pages, so they're effectively public
and must never read data. Queries require a **read key** (`rk_...`) scoped to
one project.

Read keys are **not cached**, unlike write keys, which are cached for 60
seconds. A leaked read key exposes data, so revocation has to be immediate.
One indexed Postgres lookup is negligible next to the analytical query that
follows. Tests show that a write key can't query, a read key can't ingest,
and a revoked read key fails on the very next request.

### SQL safety: user input never becomes SQL text

Every user-supplied value is a **server-side bound parameter**
(`{p0:String}`), and that includes property *names* used inside
`JSONExtract*`. ClickHouse parses the SQL first and binds values afterwards,
so a value can't change the query's structure. The only things interpolated
into SQL are operators and expressions from fixed allow-lists. A test sends
`plan') OR 1=1; DROP TABLE events; --` as a property name and value: it is
treated as a literal (non-existent) property, and the table survives.

As **defence in depth**, every query runs with `readonly=2`: no writes and no
DDL. The query can't lift that restriction itself; ClickHouse rejects
`SETTINGS readonly=0` in read-only mode, which I checked against the running
server.

### Guard rails

- **Before ClickHouse** (validation, returns 422):
  - range of at most 366 days
  - at most 1,000 time buckets (so "hourly over a year" is rejected)
  - at most 10 filters
  - 2–10 funnel steps
  - funnel window of at most 90 days
  - at most 52 retention periods
- **Inside ClickHouse** (per query): `max_execution_time` of 10 seconds,
  mapped to 504 `query_timeout`, and `max_memory_usage` of 500 MB, mapped to
  422 `query_too_expensive`. A test sets the memory limit absurdly low and
  checks that ClickHouse really enforces it.
- **Breakdown cardinality**: segmentation keeps the top 10 segments and folds
  the rest into `$other`. Breaking down by something like user agent can't
  return 50,000 series.

### Query semantics worth knowing

- **Deduplicated reads.** Queries read `events FINAL`, so Kafka-redelivery
  duplicates that haven't been merged yet are collapsed at read time
  (ADR 0002). Modern ClickHouse runs `FINAL` in parallel per partition. The
  alternative, `uniqExact(event_id)` in every aggregate, costs more.
- **Zero-filled buckets.** An empty day is data (0), not a missing point on
  the chart.
- **Unique users** use `uniq()`, an adaptive approximate counter. It is exact
  for small sets and has about 1% error at scale, with bounded memory. This
  is standard in product analytics. For `unique_users` with a breakdown,
  `$other` and totals **sum per-segment uniques**, so a user in two segments
  counts twice. Exact cross-segment uniques would need a second query; this
  is documented rather than hidden.
- **Missing properties don't match comparisons.** `JSONExtractFloat` returns
  0 for a missing key, so without a guard `value <= 50` would match every
  event that has no `value`. A test caught this bug. Comparisons now require
  `JSONHas`, which matches SQL NULL semantics. Use `is_not_set` to find
  missing properties.
- **Funnels** use `windowFunnel`: steps must happen in order, within
  `window_seconds` of the first step. Users must *enter* (step 1) inside the
  range but may *finish* up to one window after it ends. Otherwise everyone
  who entered near the end would look like a drop-off. Order is enforced: a
  purchase before signup doesn't count as converting.
- **Retention** cohorts users by the period of their *first* `start_event`
  **within the range**. A true "first ever" would need a first-seen table,
  which is noted as future work. Offset 0 is the cohort's own period.
- **Timezone**: everything is in UTC for now. Per-project timezones would
  change bucket boundaries, and that's a known gap.
- **Identity**: `distinct_id` changes when an anonymous user logs in, so a
  funnel that spans login undercounts. Identity merging is Phase 4.

### Caching: bounded staleness, plus request coalescing

- Results are cached in Redis, keyed by `(project, query kind, SHA-256 of the
  canonical request)`.
- The **TTL depends on recency**: 30 seconds if the range touches the last
  hour, where data is still arriving, and 1 hour for purely historical
  ranges. There is no invalidation: late events for an old range appear
  within the TTL. That staleness is bounded, and a test documents it.
- **Coalescing (single flight).** Identical queries in flight at the same
  time run once; the rest await the same result. A test fires 5 concurrent
  identical requests and gets exactly 1 ClickHouse execution.
- That test also found a **shared-state bug**: the route mutated the shared
  result dict, so every waiter after the first crashed. Results are now
  treated as immutable.
- If Redis is down, the cache fails open and queries simply compute.

## Measured

On the demo dataset: 200k users and **1.15M events** over 8 weeks, generated
inside ClickHouse by `clickhouse/seed/demo.sql` in 18 seconds. Measured on the
laptop stack, with ClickHouse capped at 1.2 GB.

| Query (8 weeks) | Cold (ClickHouse) | Warm (cache) |
|---|---|---|
| Segmentation: unique users by path, daily | 798 ms | 66 ms |
| Funnel: signup → checkout → purchase | 366 ms | 46 ms |
| Retention: 8 weekly cohorts × 8 periods | 1,350 ms | 65 ms |

- **Correctness:** the funnel returned 40.1% → 54.95%, matching the 40% and
  55% probabilities the generator used.
- **Index pruning** for one project, one week and one event: partition
  pruning plus the primary key read **18 of 147 granules (about 12%)**.
- **Storage:** 123 MB raw → 42 MB on disk (2.9× compression).

**Retention is the slowest query** because it joins cohorts to activity. The
next optimizations, if needed:

- a `user_first_seen` table maintained by a materialized view, which would
  remove the cohort scan
- materialized columns for hot properties (`plan`, `path`), avoiding
  `JSONExtract` per row
- ClickHouse projections for common breakdowns

## Consequences

- The API adds Redis as a query dependency, but it degrades gracefully.
- Results can be up to 1 hour stale for historical ranges.
- UTC-only bucketing and range-scoped "first" events are known
  simplifications, documented for users of the API.
