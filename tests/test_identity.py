"""Identity merge: a visitor's anonymous activity joins their user once they
identify (an event carrying both anonymous_id and user_id)."""

from typing import Any

from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from httpx import AsyncClient

from tests.conftest import Reader
from tests.query_helpers import at, insert, row, time_range


async def post(client: AsyncClient, reader: Reader, path: str, body: dict[str, Any]) -> Any:
    response = await client.post(f"/v1/query/{path}", json=body, headers=reader.headers)
    assert response.status_code == 200, response.text
    return response.json()


def visitor_becomes_user(p: str) -> list:
    """Browses anonymously, signs up (the identify event carries both ids),
    then purchases as the known user."""
    return [
        row(p, "page_viewed", None, at(0, 1), anonymous_id="anon-1"),
        row(p, "signup", "u-1", at(0, 2), anonymous_id="anon-1"),  # the link
        row(p, "purchase", "u-1", at(1)),
    ]


async def test_materialized_view_records_links_on_insert(
    reader: Reader, ch: ClickHouseClient
) -> None:
    await insert(ch, visitor_becomes_user(reader.project.id))

    result = await ch.query(
        "SELECT anonymous_id, user_id FROM identity_links WHERE project_id = {p:UUID}",
        parameters={"p": reader.project.id},
    )

    assert result.result_rows == [("anon-1", "u-1")]


async def test_funnel_crossing_login_counts_as_one_conversion(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    await insert(ch, visitor_becomes_user(reader.project.id))

    result = await post(
        client,
        reader,
        "funnel",
        {"steps": [{"event": "page_viewed"}, {"event": "purchase"}], **time_range(at(0), at(7))},
    )

    # Without identity merge: anon-1 viewed, u-1 purchased -> 0 conversions.
    assert [s["users"] for s in result["steps"]] == [1, 1]


async def test_anonymous_history_is_merged_retroactively(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch, [row(p, "page_viewed", None, at(0, h), anonymous_id="anon-9") for h in range(3)]
    )
    body = {"event": "page_viewed", "measure": "unique_users", **time_range(at(0), at(2))}
    before = await post(client, reader, "segmentation", body)

    # Days later the visitor signs up. No stored row is rewritten...
    await insert(ch, [row(p, "page_viewed", "u-9", at(1), anonymous_id="anon-9")])
    # (a trivially-true filter only to get a fresh cache key)
    body["filters"] = [{"property": "x", "operator": "is_not_set"}]
    after = await post(client, reader, "segmentation", body)

    assert before["series"][0]["total"] == 1
    # ...yet the anonymous day and the identified day now belong to ONE person.
    assert [v["value"] for v in after["series"][0]["values"]] == [1, 1]


async def test_shared_device_keeps_people_apart(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            row(p, "page_viewed", None, at(0), anonymous_id="family-laptop"),
            row(p, "login", "alice", at(0, 1), anonymous_id="family-laptop"),
            row(p, "purchase", "alice", at(0, 2)),
            row(p, "login", "bob", at(0, 5), anonymous_id="family-laptop"),  # later, same browser
        ],
    )

    result = await post(
        client,
        reader,
        "segmentation",
        {"event": "login", "measure": "unique_users", **time_range(at(0), at(1))},
    )
    anonymous = await post(
        client,
        reader,
        "segmentation",
        {"event": "page_viewed", "measure": "unique_users", **time_range(at(0), at(1))},
    )

    ownership = await post(
        client,
        reader,
        "funnel",
        {"steps": [{"event": "page_viewed"}, {"event": "purchase"}], **time_range(at(0), at(1))},
    )

    # Alice and Bob stay two people (user_ids are never merged together)...
    assert result["series"][0]["total"] == 2
    assert anonymous["series"][0]["total"] == 1
    # ...and the anonymous browsing belongs to the FIRST user linked. Only
    # Alice purchased, so the funnel converts only if Alice owns the page view.
    # (Counting alone can't tell Alice from Bob; an earlier version of this
    # test passed even with "last link wins".)
    assert [s["users"] for s in ownership["steps"]] == [1, 1]


async def test_retention_follows_the_person_across_identify(
    client: AsyncClient, reader: Reader, ch: ClickHouseClient
) -> None:
    p = reader.project.id
    await insert(
        ch,
        [
            row(p, "signup", "u-5", at(0), anonymous_id="anon-5"),
            row(p, "page_viewed", None, at(8), anonymous_id="anon-5"),  # returns, logged out
        ],
    )

    result = await post(
        client,
        reader,
        "retention",
        {
            "start_event": "signup",
            "return_event": "page_viewed",
            "periods": 1,
            **time_range(at(0), at(7)),
        },
    )

    assert result["cohorts"][0]["retained"] == [0, 1]
