import pytest
from httpx import AsyncClient
from redis.asyncio import Redis

from app.core import quota
from app.core.quota import Quota
from tests.conftest import ProjectFixture, Reader, event, make_project
from tests.query_helpers import at, time_range


@pytest.fixture
async def strict_ingest(redis: Redis, client: AsyncClient) -> Quota:
    """10 events of burst, refilling one event per ~17 minutes."""
    await redis.flushdb()
    strict = Quota(redis, name="ingest", rate=0.001, burst=10)
    quota._quotas["ingest"] = strict
    return strict


async def send(client: AsyncClient, project: ProjectFixture, n: int) -> "object":
    return await client.post(
        "/v1/batch", json={"batch": [event() for _ in range(n)]}, headers=project.headers
    )


async def test_ingest_quota_is_charged_per_event_not_per_request(
    client: AsyncClient, project: ProjectFixture, strict_ingest: Quota
) -> None:
    first = await send(client, project, 10)  # exactly the burst
    second = await send(client, project, 1)

    assert first.status_code == 202
    assert second.status_code == 429
    assert second.json()["error"]["code"] == "quota_exceeded"
    assert int(second.headers["Retry-After"]) >= 1


async def test_a_batch_larger_than_the_burst_can_never_succeed(
    client: AsyncClient, project: ProjectFixture, strict_ingest: Quota
) -> None:
    response = await send(client, project, 11)

    assert response.status_code == 429
    # Waiting won't help, so no Retry-After promise is made.
    assert "Retry-After" not in response.headers


async def test_quotas_are_per_project(
    client: AsyncClient, project: ProjectFixture, strict_ingest: Quota
) -> None:
    other = await make_project("Other")
    await send(client, project, 10)

    assert (await send(client, project, 1)).status_code == 429
    assert (await send(client, other, 1)).status_code == 202


async def test_query_quota(client: AsyncClient, reader: Reader, redis: Redis) -> None:
    await redis.flushdb()
    quota._quotas["query"] = Quota(redis, name="query", rate=0.001, burst=2)
    body = {"event": "e", **time_range(at(0), at(1))}

    statuses = [
        (await client.post("/v1/query/segmentation", json=body, headers=reader.headers)).status_code
        for _ in range(3)
    ]

    assert statuses == [200, 200, 429]


async def test_quota_fails_open_when_redis_is_down() -> None:
    dead = Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)
    q = Quota(dead, name="ingest", rate=1, burst=1)

    results = [await q.charge("p", cost=1) for _ in range(3)]

    assert all(r.allowed for r in results)
    await dead.aclose()
