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
from app.query.sql import Params, base_where, ch_datetime, filters_sql, property_value_sql
from app.schemas.queries import (
    INTERVAL_SECONDS,
    FunnelQuery,
    RetentionQuery,
    SegmentationQuery,
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
    measure = "count()" if q.measure == "total" else "uniq(distinct_id)"
    segment = property_value_sql(params.add(q.breakdown, "String")) if q.breakdown else "''"
    sql = f"""
        SELECT {_BUCKET_SQL[q.interval]} AS bucket, {segment} AS segment, {measure} AS value
        FROM events FINAL
        WHERE {base_where(project_id, q.from_, q.to, params)}
          AND event = {params.add(q.event, "String")}
          {filters_sql(q.filters, params)}
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
    # ...but may finish it up to window_seconds after the range ends; otherwise
    # everyone who entered near the end would look like they dropped off.
    sql = f"""
        SELECT level, count() AS users
        FROM (
            SELECT distinct_id,
                   windowFunnel({params.add(q.window_seconds, "UInt64")})(
                       toDateTime(timestamp), {", ".join(conditions)}
                   ) AS level
            FROM events FINAL
            WHERE {base_where(project_id, q.from_, q.to + window, params)}
              AND has({events_placeholder}, event)
            GROUP BY distinct_id
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
    cohort_where = base_where(project_id, q.from_, q.to, params)
    start_event = params.add(q.start_event, "String")
    activity_where = base_where(project_id, q.from_, q.to + period_length * q.periods, params)
    periods = params.add(q.periods, "UInt32")
    sql = f"""
        WITH
            cohorts AS (
                -- Each user's cohort: the period of their FIRST start_event in range.
                SELECT distinct_id, min({period_fn}) AS cohort
                FROM events FINAL
                WHERE {cohort_where} AND event = {start_event}
                GROUP BY distinct_id
            ),
            activity AS (
                SELECT DISTINCT distinct_id, {period_fn} AS period
                FROM events FINAL
                WHERE {activity_where} {return_filter}
            )
        SELECT c.cohort,
               dateDiff('{q.period}', c.cohort, a.period) AS offset,
               uniqExact(c.distinct_id) AS users
        FROM cohorts AS c
        INNER JOIN activity AS a ON a.distinct_id = c.distinct_id
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
