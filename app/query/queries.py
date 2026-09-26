"""The three core product-analytics queries.

All read `events FINAL`: ReplacingMergeTree may still hold not-yet-merged
Kafka-redelivery duplicates, and FINAL collapses them at read time. Modern
ClickHouse runs FINAL in parallel per partition, so the cost is acceptable;
the alternative (counting distinct event_ids) is costlier on every aggregate.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any

from app.core.config import get_settings
from app.query import executor
from app.query.sql import (
    Params,
    base_where,
    ch_datetime,
    filters_sql,
    person_events,
    property_value_sql,
)
from app.schemas.queries import (
    INTERVAL_SECONDS,
    FunnelQuery,
    RetentionQuery,
    SegmentationQuery,
    SessionsQuery,
)

OTHER_SEGMENT = "$other"
NONE_SEGMENT = "$none"
_BUCKET_SQL = {
    "hour": "toStartOfHour(timestamp)",
    "day": "toStartOfDay(timestamp)",
    "week": "toDateTime(toStartOfWeek(timestamp, 1), 'UTC')",  # mode 1: weeks start Monday
}


def _as_utc(value: datetime | date) -> datetime:
    if not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def buckets(start: datetime, end: datetime, interval: str) -> list[datetime]:
    """Every bucket in [start, end), so gaps show as zeros instead of vanishing
    from the chart (an empty day is data, not a missing point)."""
    start = start.astimezone(UTC)
    if interval == "hour":
        current = start.replace(minute=0, second=0, microsecond=0)
    else:
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        if interval == "week":
            current -= timedelta(days=current.weekday())
    step = timedelta(seconds=INTERVAL_SECONDS[interval])
    result = []
    while current < end:
        result.append(current)
        current += step
    return result


# --- Segmentation --------------------------------------------------------------


async def segmentation(project_id: str, q: SegmentationQuery) -> dict[str, Any]:
    params = Params()
    segment = property_value_sql(params.add(q.breakdown, "String")) if q.breakdown else "''"
    event_filter = f"AND event = {params.add(q.event, 'String')} {filters_sql(q.filters, params)}"
    if q.measure == "total":
        # Plain counts don't need identity: skip the join.
        measure = "count()"
        source = (
            f"(SELECT * FROM events FINAL "
            f"WHERE {base_where(project_id, q.from_, q.to, params)} {event_filter})"
        )
    else:
        measure = "uniq(person_id)"  # a visitor who later signed up counts once
        source = person_events(project_id, q.from_, q.to, params, event_filter)
    sql = f"""
        SELECT {_BUCKET_SQL[q.interval]} AS bucket, {segment} AS segment, {measure} AS value
        FROM {source}
        GROUP BY bucket, segment
    """
    rows = await executor.run(sql, params.values)

    all_buckets = buckets(q.from_, q.to, q.interval)
    per_segment: dict[str, dict[datetime, int]] = {}
    for bucket, seg, value in rows:
        label = (seg or NONE_SEGMENT) if q.breakdown else ""
        points = per_segment.setdefault(label, {})
        points[_as_utc(bucket)] = points.get(_as_utc(bucket), 0) + int(value)

    # Keep the top N segments by total; fold the long tail into "$other" so a
    # high-cardinality breakdown (e.g. user agent) can't return 50k series.
    limit = get_settings().query_breakdown_limit
    ranked = sorted(per_segment.items(), key=lambda item: -sum(item[1].values()))
    if len(ranked) > limit:
        other: dict[datetime, int] = {}
        for _, points in ranked[limit:]:
            for bucket, value in points.items():
                other[bucket] = other.get(bucket, 0) + value
        ranked = [*ranked[:limit], (OTHER_SEGMENT, other)]
    if not ranked:
        ranked = [("", {})]

    # Note: for unique_users, "$other" and totals sum per-segment uniques
    # (a user in two segments counts twice). Exact cross-segment uniques would
    # need a second query; documented rather than hidden.
    series = [
        {
            "segment": label if q.breakdown else None,
            "total": sum(points.values()),
            "values": [{"bucket": b, "value": points.get(b, 0)} for b in all_buckets],
        }
        for label, points in ranked
    ]
    return {"series": series}


# --- Funnel ----------------------------------------------------------------------


async def funnel(project_id: str, q: FunnelQuery) -> dict[str, Any]:
    params = Params()
    window = timedelta(seconds=q.window_seconds)
    end_placeholder = params.add(ch_datetime(q.to), "DateTime64(3)")
    conditions = []
    for index, step in enumerate(q.steps):
        condition = f"event = {params.add(step.event, 'String')}{filters_sql(step.filters, params)}"
        if index == 0:
            # Users must ENTER the funnel inside the range...
            condition += f" AND timestamp < {end_placeholder}"
        conditions.append(condition)
    events_placeholder = params.add(sorted({s.event for s in q.steps}), "Array(String)")
    window_placeholder = params.add(q.window_seconds, "UInt64")
    # ...but may finish up to window_seconds after the range ends; otherwise
    # everyone who entered near the end would look like they dropped off.
    source = person_events(
        project_id, q.from_, q.to + window, params, f"AND has({events_placeholder}, event)"
    )
    # Grouped by PERSON: a funnel that starts anonymous (landing page) and
    # finishes identified (purchase after login) is one conversion, not zero.
    sql = f"""
        SELECT level, count() AS users
        FROM (
            SELECT person_id,
                   windowFunnel({window_placeholder})(
                       toDateTime(timestamp), {", ".join(conditions)}
                   ) AS level
            FROM {source}
            GROUP BY person_id
        )
        WHERE level > 0
        GROUP BY level
    """
    rows = await executor.run(sql, params.values)
    users_at_level = {int(level): int(users) for level, users in rows}

    # windowFunnel returns the deepest step reached; users reaching step i are
    # everyone whose level >= i.
    steps = []
    reached = [
        sum(n for level, n in users_at_level.items() if level >= i + 1) for i in range(len(q.steps))
    ]
    for i, step in enumerate(q.steps):
        steps.append(
            {
                "event": step.event,
                "users": reached[i],
                "conversion_from_start": _rate(reached[i], reached[0]),
                "conversion_from_previous": _rate(reached[i], reached[i - 1] if i else reached[0]),
            }
        )
    return {"steps": steps, "window_seconds": q.window_seconds}


# --- Retention -------------------------------------------------------------------


async def retention(project_id: str, q: RetentionQuery) -> dict[str, Any]:
    params = Params()
    period_fn = "toStartOfWeek(timestamp, 1)" if q.period == "week" else "toDate(timestamp)"
    period_length = timedelta(weeks=1) if q.period == "week" else timedelta(days=1)
    return_filter = f"AND event = {params.add(q.return_event, 'String')}" if q.return_event else ""
    start_filter = f"AND event = {params.add(q.start_event, 'String')}"
    cohort_source = person_events(project_id, q.from_, q.to, params, start_filter)
    activity_source = person_events(
        project_id, q.from_, q.to + period_length * q.periods, params, return_filter
    )
    periods = params.add(q.periods, "UInt32")
    sql = f"""
        WITH
            cohorts AS (
                -- Each person's cohort: the period of their FIRST start_event in range.
                SELECT person_id, min({period_fn}) AS cohort
                FROM {cohort_source}
                GROUP BY person_id
            ),
            activity AS (
                SELECT DISTINCT person_id, {period_fn} AS period
                FROM {activity_source}
            )
        SELECT c.cohort,
               dateDiff('{q.period}', c.cohort, a.period) AS offset,
               uniqExact(c.person_id) AS users
        FROM cohorts AS c
        INNER JOIN activity AS a ON a.person_id = c.person_id
        WHERE offset BETWEEN 0 AND {periods}
        GROUP BY c.cohort, offset
        UNION ALL
        -- offset -1 carries each cohort's size.
        SELECT cohort, -1, count() FROM cohorts GROUP BY cohort
    """
    rows = await executor.run(sql, params.values)

    sizes: dict[datetime, int] = {}
    retained: dict[datetime, dict[int, int]] = {}
    for cohort, offset, users in rows:
        key = _as_utc(cohort)
        if int(offset) == -1:
            sizes[key] = int(users)
        else:
            retained.setdefault(key, {})[int(offset)] = int(users)
    cohorts = []
    for cohort in sorted(sizes):
        counts = [retained.get(cohort, {}).get(k, 0) for k in range(q.periods + 1)]
        cohorts.append(
            {
                "cohort": cohort,
                "size": sizes[cohort],
                "retained": counts,
                "rates": [_rate(n, sizes[cohort]) for n in counts],
            }
        )
    return {"period": q.period, "cohorts": cohorts}


# --- Sessions --------------------------------------------------------------------


async def sessions(project_id: str, q: SessionsQuery) -> dict[str, Any]:
    """Sessionization at query time: a new session starts when a person has
    been inactive for more than `inactivity_minutes`.

    Why at query time instead of in the stream processor: late events. An
    event that arrives hours late must merge into (or split) the right
    session. A streaming sessionizer must hold per-person state and still gets
    late data wrong; recomputing from stored events is always correct.

    Events are loaded from one inactivity gap BEFORE `from`, so a session
    already running at `from` isn't mistaken for a new one; only sessions
    STARTING in [from, to) are reported. Sessions are followed up to 24h past
    `to` so their duration isn't cut short.
    """
    params = Params()
    gap = timedelta(minutes=q.inactivity_minutes)
    source = person_events(project_id, q.from_ - gap, q.to + timedelta(hours=24), params)
    gap_seconds = params.add(int(gap.total_seconds()), "UInt32")
    start = params.add(ch_datetime(q.from_), "DateTime64(3)")
    end = params.add(ch_datetime(q.to), "DateTime64(3)")
    bucket = _BUCKET_SQL[q.interval].replace("timestamp", "started")
    sql = f"""
        SELECT {bucket} AS bucket,
               count() AS sessions,
               uniq(person_id) AS users,
               sum(dateDiff('millisecond', started, ended)) / 1000 AS total_duration_seconds,
               countIf(events = 1) AS bounces,
               sum(events) AS total_events
        FROM (
            SELECT person_id, session_seq,
                   min(timestamp) AS started, max(timestamp) AS ended, count() AS events
            FROM (
                -- Running count of session starts = session number per person.
                SELECT person_id, timestamp,
                       sum(is_start) OVER (
                           PARTITION BY person_id ORDER BY timestamp
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS session_seq
                FROM (
                    -- 1 when this is the person's first event, or the gap since
                    -- their previous event exceeds the inactivity limit.
                    SELECT person_id, timestamp,
                           if(row_number() OVER w = 1
                              OR dateDiff('second', lagInFrame(timestamp) OVER w, timestamp)
                                 > {gap_seconds}, 1, 0) AS is_start
                    FROM {source}
                    WINDOW w AS (PARTITION BY person_id ORDER BY timestamp
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                )
            )
            GROUP BY person_id, session_seq
        )
        WHERE started >= {start} AND started < {end}
        GROUP BY bucket
    """
    rows = await executor.run(sql, params.values)
    by_bucket = {_as_utc(r[0]): r for r in rows}

    points = []
    total_sessions = total_bounces = total_events = 0
    total_duration = 0.0
    for b in buckets(q.from_, q.to, q.interval):
        _, n, users, duration, bounces, events = by_bucket.get(b, (b, 0, 0, 0.0, 0, 0))
        n, users, bounces, events, duration = (
            int(n),
            int(users),
            int(bounces),
            int(events),
            float(duration),
        )
        points.append(
            {
                "bucket": b,
                "sessions": n,
                "users": users,
                "avg_duration_seconds": round(duration / n, 1) if n else 0.0,
                "bounce_rate": _rate(bounces, n),
                "events_per_session": round(events / n, 2) if n else 0.0,
            }
        )
        total_sessions += n
        total_bounces += bounces
        total_events += events
        total_duration += duration
    return {
        "values": points,
        # Session-weighted. (Unique users aren't additive across buckets, so
        # there is deliberately no total "users".)
        "totals": {
            "sessions": total_sessions,
            "avg_duration_seconds": round(total_duration / total_sessions, 1)
            if total_sessions
            else 0.0,
            "bounce_rate": _rate(total_bounces, total_sessions),
            "events_per_session": round(total_events / total_sessions, 2)
            if total_sessions
            else 0.0,
        },
    }


async def event_names(project_id: str, days: int) -> list[dict[str, Any]]:
    params = Params()
    now = datetime.now(UTC)
    where = base_where(project_id, now - timedelta(days=days), now + timedelta(minutes=5), params)
    sql = f"""
        SELECT event, count() AS n FROM events
        WHERE {where}
        GROUP BY event ORDER BY n DESC LIMIT 500
    """
    return [{"event": e, "count": int(n)} for e, n in await executor.run(sql, params.values)]


def _rate(part: int, whole: int) -> float:
    return round(part / whole, 4) if whole else 0.0


async def violation_counts(project_id: str, days: int) -> list[dict[str, Any]]:
    params = Params()
    now = datetime.now(UTC)
    where = base_where(project_id, now - timedelta(days=days), now + timedelta(minutes=5), params)
    sql = f"""
        SELECT event, violation, count() AS n
        FROM events
        ARRAY JOIN violations AS violation
        WHERE {where}
        GROUP BY event, violation
        ORDER BY n DESC
        LIMIT 200
    """
    rows = await executor.run(sql, params.values)
    return [{"event": e, "violation": v, "count": int(n)} for e, v, n in rows]
