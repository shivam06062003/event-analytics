# ADR 0004: Identity merge, sessionization and tracking plans

- **Status:** Accepted
- **Date:** 2026-09-27

## 1. Identity merge

**Problem.** A visitor browses anonymously (`anonymous_id`), signs up, then
acts as a known user (`user_id`). Counting by the raw id splits one human
into two. A funnel from "landing page viewed" to "purchase" then shows 0
conversions for someone who clearly converted.

**Decision: record links at write time, resolve people at query time.**

- **Recording links.** An event carrying *both* ids (the SDK sends one at
  signup or login) is an identity link. A ClickHouse **materialized view**
  copies those pairs into `identity_links` on every insert. A materialized
  view is effectively an INSERT trigger, so the processor has no identity
  code at all.
- **Resolving people.** Every people-based query (unique users, funnels,
  retention, sessions) computes
  `person_id = user_id ?? first user linked to anonymous_id ?? anonymous_id`.
  That's a `LEFT JOIN` against the project's links, in `app/query/sql.py:person_events`.

**Why resolve at query time.** Rewriting stored rows is expensive in
ClickHouse, since updates are asynchronous mutations that rewrite whole data
parts. Query-time resolution is **retroactive for free**: the moment a link
exists, all earlier anonymous activity belongs to the user. A test shows a
visitor's anonymous day and identified day becoming one person without any
row being rewritten. The cost is one join per query, against a table that
holds only link pairs.

**Rules:**

- **The first link wins** (`argMin(user_id, linked_at)`). On a shared device,
  where Alice logs in and later Bob logs in on the same browser, the
  anonymous history stays with Alice rather than fusing two people.
- **Two `user_id`s are never merged** with each other. Only anonymous ids get
  attached.
- A test covers the shared-device case. Its first version only *counted*
  people, and it still passed when I switched the code to "last link wins",
  because Alice and Bob both count as one. It now asserts *who* owns the
  history, and it fails on the sabotaged version.

**Known limit:** links chain only one level (anonymous → user). Merging
across devices through a common user works. Merging two identified users is
deliberately unsupported.

### A ClickHouse gotcha, fixed with a corrective migration

The first version of the view was:

```sql
SELECT assumeNotNull(anonymous_id) AS anonymous_id ... WHERE anonymous_id IS NOT NULL
```

ClickHouse resolves names in `WHERE` to **SELECT aliases first**, so the
filter tested the alias, which is never NULL, and let every row through. That
wrote half-empty links like `('anon-1', '')`, which then broke resolution.

Migration 0003 was already applied locally, so it was **not edited**: editing
applied migrations makes environments diverge. Migration 0004 instead drops
and recreates the view with the filter in a subquery, and deletes the bad
rows. The resolver also ignores empty ids defensively.

## 2. Sessionization

**Decision: compute sessions at query time.** A session ends after
`inactivity_minutes` (default 30) without an event from that person. It's
computed with window functions:

1. `lagInFrame` flags each event whose gap since the person's previous event
   exceeds the limit
2. a running `sum()` of those flags numbers each person's sessions
3. aggregation produces per-session start, end and event count

**Why not in the stream processor?** Late events. A mobile client flushes a
batch from hours ago, and those events must merge into, or split, sessions
that already "ended". A streaming sessionizer has to hold per-person state
with timeouts, and it still gets late data wrong. Recomputing from stored
events is always correct. Using `person_id` also keeps a session that spans a
login as one session, and a test covers that.

**Range edges:**

- Events are loaded from one inactivity gap **before** `from`, so a session
  already running at midnight isn't counted as new. Tested; removing the
  look-back makes that test fail.
- Only sessions that **start** inside the range are reported.
- Sessions are followed up to 24 hours past `to`, so durations aren't cut
  short.

**Metrics:** sessions, unique users, average duration, bounce rate
(single-event sessions) and events per session. Totals are weighted by
session count. Unique users aren't additive across buckets, so there's no
total for them.

**Cost:** 3.8 seconds cold over 1.15M events and 953k sessions on the laptop,
the slowest query so far. Sessionizing sorts every person's events, and the
demo data spreads page views randomly over two days, so most "sessions" are
single events. The standard next step is to **materialize sessions for
closed days** in a daily rollup, recompute only the last day or two (where
late events land), and sessionize just the open window at query time.

## 3. Tracking plans (schema governance)

**Problem.** Analytics data rots quietly. An SDK release renames `value` to
`amount`, sends `"49"` instead of `49`, or invents `purchase_completed_v2`,
and dashboards drift for weeks before anyone notices.

**Decision.** Each project can have a **versioned tracking plan** that
declares, per event:

- property types: string, number, boolean, object or array
- whether each property is required
- optional allowed values (`enum`)
- whether unlisted properties are allowed

**Enforcement at ingestion:**

- `warn`: the event is accepted, and its violations travel with it (in a new
  `violations` field) into ClickHouse, where
  `GET /v1/tracking-plan/violations` aggregates them.
- `block`: the event is returned in `rejected` with the reasons. The rest of
  the batch still gets in, since per-event acceptance comes from ADR 0001.
- Booleans are not numbers (`True` fails `type: number`), which catches a
  classic Python and JavaScript bug.

**Schema evolution.** Plans are **append-only versions** in Postgres, with
`UNIQUE(project_id, version)` so concurrent saves can't both become
version N. A new version is compared with the current one, and **breaking
changes are refused (409)** unless the caller sets
`allow_breaking_changes=true`. A change is breaking if some event that passed
the old plan would fail the new one:

- a new required property
- a changed type
- removed enum values
- a newly added enum
- disallowing additional properties
- removing a planned event when unplanned events are disallowed
- switching warn → block

This is the same idea as BACKWARD compatibility in Avro and Protobuf schema
registries. It's unit-tested case by case.

**Evolving our own Kafka message format.** Adding `violations` to the
message did **not** bump `schema_version`. An optional field with a default
is backward compatible, since old messages still parse, and a test checks
that. It's also forward compatible, since an older processor ignores the
unknown field. The ClickHouse column was added with `ADD COLUMN ... DEFAULT []`,
a metadata-only change that rewrites no data.

**Permissions and caching:**

- Changing a plan changes what ingestion accepts, so it needs a read key
  created with `--manage`. Ordinary read keys get 403.
- Ingestion caches each project's plan for 30 seconds per API instance, like
  write keys. The saving instance evicts its cache entry immediately, and
  other instances converge within 30 seconds.

## A test-harness bug found along the way

The session-wide Kafka collector used by the tests crashed on the poison
messages that the Phase 2 dead-letter test deliberately produces. It died
silently, so every later test waiting for messages saw 0. That went
unnoticed until Phase 4 added Kafka-waiting tests that run after the poison
test. The collector now skips messages it can't index, like the real
processor does.

## Consequences

- People-based queries cost one extra join. Plain event counts skip it.
- Sessions are always correct under late data, but they're the most
  expensive query. A daily rollup is the documented next step.
- Plan changes reach all ingestion instances within 30 seconds.
