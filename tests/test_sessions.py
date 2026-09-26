from typing import Any

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from httpx import AsyncClient

from tests.conftest import Reader
from tests.query_helpers import MONDAY, at, insert, row, time_range


async def sessions(client: AsyncClient, reader: Reader, **body: Any) -> Any:
    response = await client.post("/v1/query/sessions", json=body, headers=reader.headers)
    assert response.status_code == 200, response.text
    return response.json()


def minutes(m: float):
    return at(0, 10 + m / 60)  # 10:00 on MONDAY + m minutes


async def test_inactivity_gap_splits_sessions(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            # Alice: 10:00, 10:10, 10:20 = one 20-minute session; 11:30 = a new
            # session (70 min gap), a single event, i.e. a bounce.
            row(p, "page_viewed", "alice", minutes(0)),
            row(p, "page_viewed", "alice", minutes(10)),
            row(p, "page_viewed", "alice", minutes(20)),
            row(p, "page_viewed", "alice", minutes(90)),
            # Bob: one 5-minute session with two events.
            row(p, "page_viewed", "bob", minutes(0)),
            row(p, "signup", "bob", minutes(5)),
        ],
    )

    result = await sessions(client, reader, **time_range(at(0), at(1)))

    [day] = result["values"]
    assert (day["sessions"], day["users"]) == (3, 2)
    assert day["bounce_rate"] == pytest.approx(1 / 3, abs=1e-4)
    assert day["avg_duration_seconds"] == pytest.approx((20 * 60 + 0 + 5 * 60) / 3, abs=0.1)
    assert day["events_per_session"] == 2.0
    assert result["totals"]["sessions"] == 3


async def test_inactivity_limit_is_configurable(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(ch, [row(p, "e", "u", minutes(0)), row(p, "e", "u", minutes(45))])

    default = await sessions(client, reader, **time_range(at(0), at(1)))
    lenient = await sessions(client, reader, inactivity_minutes=60, **time_range(at(0), at(1)))

    assert default["totals"]["sessions"] == 2  # 45 min > 30 min
    assert lenient["totals"]["sessions"] == 1


async def test_a_session_already_running_at_range_start_is_not_counted_as_new(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    # Started Sunday 23:50, continues past midnight into the queried Monday.
    await insert(
        ch,
        [
            row(p, "e", "night-owl", at(0, -10 / 60)),
            row(p, "e", "night-owl", at(0, 5 / 60)),
            row(p, "e", "morning", at(0, 9)),
        ],
    )

    result = await sessions(client, reader, **time_range(MONDAY, at(1)))

    assert result["totals"]["sessions"] == 1  # only "morning" starts inside the range


async def test_session_spanning_login_is_one_session(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            row(p, "page_viewed", None, minutes(0), anonymous_id="anon-s"),
            row(p, "signup", "u-s", minutes(3), anonymous_id="anon-s"),
            row(p, "purchase", "u-s", minutes(8)),
        ],
    )

    result = await sessions(client, reader, **time_range(at(0), at(1)))

    assert result["totals"]["sessions"] == 1
    assert result["values"][0]["events_per_session"] == 3.0


async def test_empty_days_are_zero_filled(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    await insert(ch, [row(reader.project.id, "e", "u", at(2, 12))])

    result = await sessions(client, reader, **time_range(at(0), at(3)))

    assert [v["sessions"] for v in result["values"]] == [0, 0, 1]
