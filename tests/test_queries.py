import asyncio
from typing import Any

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from clickhouse_connect.driver.exceptions import DatabaseError
from httpx import AsyncClient

from app.core.config import get_settings
from app.core.db import SessionLocal
from app.query import executor
from app.query.queries import buckets
from app.services import projects as project_service
from tests.conftest import Reader, event, make_project
from tests.query_helpers import MONDAY, at, insert, row, time_range


def values(series: dict[str, Any]) -> list[int]:
    return [point["value"] for point in series["values"]]


async def segmentation(client: AsyncClient, reader: Reader, **body: Any) -> Any:
    response = await client.post("/v1/query/segmentation", json=body, headers=reader.headers)
    assert response.status_code == 200, response.text
    return response.json()


# --- Segmentation ------------------------------------------------------------------


async def test_daily_counts_include_zero_days(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(ch, [row(p, "signup", f"u{i}", at(0, i)) for i in range(3)])
    await insert(ch, [row(p, "signup", "u9", at(2, 5)), row(p, "other_event", "u1", at(1))])

    result = await segmentation(client, reader, event="signup", **time_range(at(0), at(3)))

    [series] = result["series"]
    assert values(series) == [3, 0, 1]  # the empty day is a 0, not a missing point
    assert series["total"] == 4
    assert result["meta"]["cached"] is False


async def test_unique_users_measure(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(ch, [row(p, "page_viewed", "same-user", at(0, h)) for h in range(5)])

    result = await segmentation(
        client, reader, event="page_viewed", measure="unique_users", **time_range(at(0), at(1))
    )

    assert values(result["series"][0]) == [1]


async def test_breakdown_keeps_top_segments_and_folds_the_rest(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "query_breakdown_limit", 2)
    p = reader.project.id
    plans = ["pro"] * 3 + ["free"] * 2 + ["team"] + [None]
    await insert(
        ch,
        [
            row(p, "signup", f"u{i}", at(0), {"plan": plan} if plan else {})
            for i, plan in enumerate(plans)
        ],
    )

    result = await segmentation(
        client, reader, event="signup", breakdown="plan", **time_range(at(0), at(1))
    )

    assert [(s["segment"], s["total"]) for s in result["series"]] == [
        ("pro", 3),
        ("free", 2),
        ("$other", 2),  # team + the event without a plan ($none)
    ]


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ([{"property": "plan", "operator": "eq", "value": "pro"}], 2),
        ([{"property": "plan", "operator": "neq", "value": "pro"}], 2),
        ([{"property": "value", "operator": "gt", "value": 100}], 2),
        # d has no "value": it must NOT count as 0 <= 50 (regression test).
        ([{"property": "value", "operator": "lte", "value": 50}], 1),
        ([{"property": "value", "operator": "neq", "value": 50}], 2),
        ([{"property": "coupon", "operator": "is_set"}], 1),
        ([{"property": "coupon", "operator": "is_not_set"}], 3),
        ([{"property": "path", "operator": "contains", "value": "PRIC"}], 1),
        ([{"property": "trial", "operator": "eq", "value": True}], 1),
        (
            [
                {"property": "plan", "operator": "eq", "value": "pro"},
                {"property": "value", "operator": "gt", "value": 150},
            ],
            1,
        ),
    ],
)
async def test_property_filters(
    client: AsyncClient,
    reader: Reader,
    ch: ClickHouseClient,
    filters: list[dict[str, Any]],
    expected: int,
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            row(p, "purchase", "a", at(0), {"plan": "pro", "value": 200, "coupon": "X"}),
            row(p, "purchase", "b", at(0), {"plan": "pro", "value": 120, "trial": True}),
            row(p, "purchase", "c", at(0), {"plan": "free", "value": 50, "path": "/pricing"}),
            row(p, "purchase", "d", at(0), {"plan": "team"}),
        ],
    )

    result = await segmentation(
        client, reader, event="purchase", filters=filters, **time_range(at(0), at(1))
    )

    assert result["series"][0]["total"] == expected


async def test_other_projects_and_redelivered_duplicates_are_excluded(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    other = await make_project("Other")
    original = row(p, "signup", "u1", at(0))
    await insert(ch, [original])
    await insert(ch, [original])  # Kafka redelivery: byte-identical row
    await insert(ch, [row(other.id, "signup", "x", at(0))])

    result = await segmentation(client, reader, event="signup", **time_range(at(0), at(1)))

    assert result["series"][0]["total"] == 1  # FINAL collapses the duplicate


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"interval": "hour", **time_range(at(0), at(60))}, "buckets exceed"),
        (time_range(at(0), at(400)), "exceeds 366 days"),
        (time_range(at(1), at(0)), "must be after"),
        (
            {
                "filters": [{"property": "v", "operator": "gt", "value": "x"}],
                **time_range(at(0), at(1)),
            },
            "needs a number",
        ),
    ],
)
async def test_unreasonable_queries_are_rejected_before_reaching_clickhouse(
    client: AsyncClient, reader: Reader, body: dict[str, Any], message: str
) -> None:
    response = await client.post(
        "/v1/query/segmentation", json={"event": "e", **body}, headers=reader.headers
    )

    assert response.status_code == 422
    assert message in str(response.json()["error"]["details"])


async def test_sql_injection_in_property_names_and_values_is_inert(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(ch, [row(p, "signup", "u1", at(0), {"plan": "pro"})])
    payload = "plan') OR 1=1; DROP TABLE events; --"

    result = await segmentation(
        client,
        reader,
        event="signup",
        breakdown=payload,
        filters=[{"property": payload, "operator": "eq", "value": payload}],
        **time_range(at(0), at(1)),
    )

    # Treated as a (non-existent) property literally named with the payload.
    assert result["series"][0]["total"] == 0
    assert (await ch.query("EXISTS TABLE events")).result_rows == [(1,)]


# --- Funnels -------------------------------------------------------------------------


async def funnel(client: AsyncClient, reader: Reader, steps: list[Any], **body: Any) -> Any:
    response = await client.post(
        "/v1/query/funnel",
        json={"steps": [s if isinstance(s, dict) else {"event": s} for s in steps], **body},
        headers=reader.headers,
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_funnel_counts_users_completing_steps_in_order(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            # completes all three
            row(p, "signup", "a", at(0)),
            row(p, "checkout", "a", at(0, 1)),
            row(p, "purchase", "a", at(0, 2)),
            # drops after checkout
            row(p, "signup", "b", at(0)), row(p, "checkout", "b", at(1)),
            # only signs up
            row(p, "signup", "c", at(1)),
            # purchase BEFORE signup doesn't count as converting
            row(p, "purchase", "d", at(0)),
            row(p, "checkout", "d", at(0, 1)),
            row(p, "signup", "d", at(0, 2)),
        ],
    )  # fmt: skip

    result = await funnel(
        client, reader, ["signup", "checkout", "purchase"], **time_range(at(0), at(7))
    )

    assert [(s["event"], s["users"]) for s in result["steps"]] == [
        ("signup", 4),
        ("checkout", 2),
        ("purchase", 1),
    ]
    assert [s["conversion_from_start"] for s in result["steps"]] == [1.0, 0.5, 0.25]
    assert [s["conversion_from_previous"] for s in result["steps"]] == [1.0, 0.5, 0.5]


async def test_funnel_window_is_enforced(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            row(p, "signup", "slow", at(0)), row(p, "purchase", "slow", at(8)),  # 8 days later
            row(p, "signup", "fast", at(0)), row(p, "purchase", "fast", at(2)),
        ],
    )  # fmt: skip

    result = await funnel(
        client, reader, ["signup", "purchase"], window_seconds=7 * 86_400,
        **time_range(at(0), at(14)),
    )  # fmt: skip

    assert [s["users"] for s in result["steps"]] == [2, 1]


async def test_conversions_may_finish_after_the_range_but_must_start_inside_it(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            # enters on the last day, converts the day after the range: counts
            row(p, "signup", "late", at(6, 20)), row(p, "purchase", "late", at(7, 10)),
            # enters after the range: excluded entirely
            row(p, "signup", "outside", at(7, 1)), row(p, "purchase", "outside", at(7, 2)),
        ],
    )  # fmt: skip

    result = await funnel(client, reader, ["signup", "purchase"], **time_range(at(0), at(7)))

    assert [s["users"] for s in result["steps"]] == [1, 1]


async def test_funnel_step_filters(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            row(p, "signup", "big", at(0)), row(p, "purchase", "big", at(1), {"value": 500}),
            row(p, "signup", "small", at(0)), row(p, "purchase", "small", at(1), {"value": 5}),
        ],
    )  # fmt: skip

    big_purchase = {
        "event": "purchase",
        "filters": [{"property": "value", "operator": "gte", "value": 100}],
    }
    result = await funnel(client, reader, ["signup", big_purchase], **time_range(at(0), at(7)))

    assert [s["users"] for s in result["steps"]] == [2, 1]


# --- Retention -----------------------------------------------------------------------


async def test_weekly_retention_cohorts(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    rows = [
        # Week 0 cohort: a, b, c sign up.
        row(p, "signup", "a", at(0)), row(p, "signup", "b", at(1)), row(p, "signup", "c", at(2)),
        row(p, "page_viewed", "a", at(0, 1)),                     # a active in week 0
        row(p, "page_viewed", "a", at(8)), row(p, "page_viewed", "b", at(9)),   # week 1: a, b
        row(p, "page_viewed", "a", at(15)),                       # week 2: a
        # Week 1 cohort: d signs up, returns in week 2.
        row(p, "signup", "d", at(7)), row(p, "page_viewed", "d", at(16)),
        # A second signup doesn't move a user to a later cohort.
        row(p, "signup", "a", at(10)),
    ]  # fmt: skip
    await insert(ch, rows)

    response = await client.post(
        "/v1/query/retention",
        json={
            "start_event": "signup",
            "return_event": "page_viewed",
            "period": "week",
            "periods": 2,
            **time_range(at(0), at(14)),
        },
        headers=reader.headers,
    )

    assert response.status_code == 200, response.text
    cohorts = response.json()["cohorts"]
    assert [(c["size"], c["retained"]) for c in cohorts] == [(3, [1, 2, 1]), (1, [0, 1, 0])]
    assert cohorts[0]["rates"] == [0.3333, 0.6667, 0.3333]
    assert cohorts[0]["cohort"].startswith("2026-06-01")


# --- Access control ----------------------------------------------------------------


async def test_write_keys_cannot_query_and_read_keys_cannot_ingest(
    client: AsyncClient, reader: Reader
) -> None:
    body = {"event": "signup", **time_range(at(0), at(1))}

    query_with_write_key = await client.post(
        "/v1/query/segmentation", json=body, headers=reader.project.headers
    )
    ingest_with_read_key = await client.post(
        "/v1/batch", json={"batch": [event()]}, headers=reader.headers
    )

    assert query_with_write_key.status_code == 401
    assert ingest_with_read_key.status_code == 401


async def test_revoked_read_key_is_refused_immediately(client: AsyncClient, reader: Reader) -> None:
    body = {"event": "signup", **time_range(at(0), at(1))}
    assert (
        await client.post("/v1/query/segmentation", json=body, headers=reader.headers)
    ).status_code == 200

    async with SessionLocal() as session:
        await project_service.revoke_read_key(session, reader.read_key_id)

    # No cache for read keys: the very next request fails.
    response = await client.post("/v1/query/segmentation", json=body, headers=reader.headers)
    assert response.status_code == 401


# --- Caching, coalescing, guard rails --------------------------------------------------


async def test_results_are_cached_with_bounded_staleness(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(ch, [row(p, "signup", "u1", at(0))])
    body = {"event": "signup", **time_range(at(0), at(1))}

    first = await segmentation(client, reader, **body)
    await insert(ch, [row(p, "signup", "late-arrival", at(0))])
    second = await segmentation(client, reader, **body)
    different = await segmentation(client, reader, measure="unique_users", **body)

    assert (first["meta"]["cached"], second["meta"]["cached"]) == (False, True)
    # Served from cache: the late event isn't visible until the TTL expires.
    assert second["series"] == first["series"]
    assert different["meta"]["cached"] is False
    assert different["series"][0]["total"] == 2


async def test_identical_concurrent_queries_run_once(
    client: AsyncClient, reader: Reader, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    real_run = executor.run

    async def slow_counting_run(sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.3)
        return await real_run(sql, params)

    monkeypatch.setattr(executor, "run", slow_counting_run)
    body = {"event": "signup", **time_range(at(0), at(1))}

    responses = await asyncio.gather(
        *(
            client.post("/v1/query/segmentation", json=body, headers=reader.headers)
            for _ in range(5)
        )
    )

    assert all(r.status_code == 200 for r in responses)
    assert calls == 1


async def test_memory_limit_is_enforced_by_clickhouse(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = reader.project.id
    await insert(ch, [row(p, "signup", f"u{i}", at(0)) for i in range(50)])
    monkeypatch.setattr(get_settings(), "query_max_memory_bytes", 1)  # absurdly low, on purpose

    response = await client.post(
        "/v1/query/segmentation",
        json={"event": "signup", **time_range(at(0), at(1))},
        headers=reader.headers,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "query_too_expensive"


async def test_timeouts_map_to_504(
    client: AsyncClient, reader: Reader, monkeypatch: pytest.MonkeyPatch
) -> None:
    class TimingOut:
        async def query(self, *args: Any, **kwargs: Any) -> None:
            raise DatabaseError("Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)")

    monkeypatch.setattr(executor, "get_client", lambda: TimingOut())

    response = await client.post(
        "/v1/query/segmentation",
        json={"event": "signup", **time_range(at(0), at(1))},
        headers=reader.headers,
    )

    assert response.status_code == 504
    assert response.json()["error"]["code"] == "query_timeout"


async def test_event_names(client: AsyncClient, reader: Reader, ch: ClickHouseClient) -> None:
    from datetime import UTC, datetime

    p = reader.project.id
    now = datetime.now(UTC)
    await insert(ch, [row(p, "page_viewed", "u", now)] * 1 + [row(p, "signup", "u", now)])
    await insert(ch, [row(p, "page_viewed", f"u{i}", now) for i in range(3)])

    response = await client.get("/v1/event-names", headers=reader.headers)

    assert response.json() == [
        {"event": "page_viewed", "count": 4},
        {"event": "signup", "count": 1},
    ]


def test_week_buckets_start_on_monday() -> None:
    wednesday = MONDAY.replace(day=3)
    assert buckets(wednesday, MONDAY.replace(day=16), "week") == [
        MONDAY, MONDAY.replace(day=8), MONDAY.replace(day=15),
    ]  # fmt: skip
