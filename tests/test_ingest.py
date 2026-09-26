import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError
from httpx import AsyncClient

from app.core.config import get_settings
from app.core.db import SessionLocal
from app.services import projects as project_service
from tests.conftest import ProjectFixture, TopicCollector, event, make_project


async def test_accepted_events_are_durably_written_to_the_topic(
    client: AsyncClient, project: ProjectFixture, kafka: TopicCollector
) -> None:
    events = [event(event="signup"), event(event="checkout_started", user_id="u-42")]

    response = await client.post("/v1/batch", json={"batch": events}, headers=project.headers)

    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": []}
    messages = await kafka.wait_for(project.id, 2)
    by_name = {m.value["event"]: m for m in messages}
    signup = by_name["signup"]
    assert signup.value["schema_version"] == 1
    assert signup.value["event_id"] == events[0]["event_id"]
    assert signup.value["distinct_id"] == "anon-1"
    assert signup.value["properties"] == {"path": "/pricing"}
    assert signup.key == f"{project.id}:anon-1"
    # A known user is preferred over the anonymous id for grouping.
    assert by_name["checkout_started"].value["distinct_id"] == "u-42"


async def test_invalid_events_are_rejected_individually(
    client: AsyncClient, project: ProjectFixture, kafka: TopicCollector
) -> None:
    bad = {"event_id": str(uuid.uuid4()), "event": "no_identity"}
    typo = event(propertes={"x": 1})  # unknown field: likely an SDK bug

    response = await client.post(
        "/v1/batch", json={"batch": [event(), bad, typo]}, headers=project.headers
    )

    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] == 1
    assert [r["index"] for r in body["rejected"]] == [1, 2]
    assert body["rejected"][0]["event_id"] == bad["event_id"]
    assert "user_id or anonymous_id" in body["rejected"][0]["errors"][0]
    assert body["rejected"][1]["errors"][0].startswith("propertes:")
    assert len(await kafka.wait_for(project.id, 1)) == 1


async def test_batch_with_no_valid_events_is_a_400(
    client: AsyncClient, project: ProjectFixture
) -> None:
    response = await client.post(
        "/v1/batch", json={"batch": [{"event": "x"}]}, headers=project.headers
    )

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "no_valid_events"
    assert error["details"][0]["index"] == 0


async def test_oversized_properties_are_rejected(
    client: AsyncClient, project: ProjectFixture
) -> None:
    huge = event(properties={"blob": "x" * (get_settings().max_properties_bytes + 1)})

    response = await client.post(
        "/v1/batch", json={"batch": [event(), huge]}, headers=project.headers
    )

    assert response.json()["accepted"] == 1
    assert "properties exceed" in response.json()["rejected"][0]["errors"][0]


async def test_too_many_events_in_one_batch(
    client: AsyncClient, project: ProjectFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "max_batch_events", 2)

    response = await client.post(
        "/v1/batch", json={"batch": [event(), event(), event()]}, headers=project.headers
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "batch_too_large"


async def test_oversized_body_is_refused_before_reading(
    client: AsyncClient, project: ProjectFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "max_request_bytes", 200)

    response = await client.post(
        "/v1/batch", json={"batch": [event() for _ in range(5)]}, headers=project.headers
    )

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "payload_too_large"


async def test_same_user_always_lands_on_the_same_partition(
    client: AsyncClient, project: ProjectFixture, kafka: TopicCollector
) -> None:
    batch = [event(anonymous_id=f"user-{i % 3}") for i in range(12)]

    await client.post("/v1/batch", json={"batch": batch}, headers=project.headers)

    messages = await kafka.wait_for(project.id, 12)
    partitions_by_user: dict[str, set[int]] = {}
    for m in messages:
        partitions_by_user.setdefault(m.value["distinct_id"], set()).add(m.partition)
    # Per-user ordering depends on this: all of a user's events in one partition.
    assert all(len(p) == 1 for p in partitions_by_user.values())


async def test_client_clock_skew_is_corrected(
    client: AsyncClient, project: ProjectFixture, kafka: TopicCollector
) -> None:
    # The device clock is 1 hour behind. The event happened 5s before sending.
    skew = timedelta(hours=1)
    now = datetime.now(UTC)
    sent_at = now - skew
    happened = sent_at - timedelta(seconds=5)

    await client.post(
        "/v1/batch",
        json={"batch": [event(timestamp=happened.isoformat())], "sent_at": sent_at.isoformat()},
        headers=project.headers,
    )

    [message] = await kafka.wait_for(project.id, 1)
    corrected = datetime.fromisoformat(message.value["timestamp"])
    received = datetime.fromisoformat(message.value["received_at"])
    assert received - corrected == timedelta(seconds=5)
    assert datetime.fromisoformat(message.value["client_timestamp"]) == happened


async def test_unauthenticated_requests_are_refused(client: AsyncClient) -> None:
    missing = await client.post("/v1/batch", json={"batch": [event()]})
    wrong = await client.post(
        "/v1/batch", json={"batch": [event()]}, headers={"Authorization": "Bearer wk_nope"}
    )

    assert missing.status_code == wrong.status_code == 401
    assert missing.headers["WWW-Authenticate"] == "Bearer"


async def test_revoked_key_stops_working_once_the_cache_expires(
    client: AsyncClient, project: ProjectFixture
) -> None:
    ok = await client.post("/v1/batch", json={"batch": [event()]}, headers=project.headers)
    assert ok.status_code == 202

    async with SessionLocal() as session:
        await project_service.revoke_write_key(session, project.write_key_id)

    # Documented trade-off: still accepted while the cached entry is fresh...
    cached = await client.post("/v1/batch", json={"batch": [event()]}, headers=project.headers)
    assert cached.status_code == 202
    # ...and refused once it expires.
    project_service.clear_write_key_cache()
    refused = await client.post("/v1/batch", json={"batch": [event()]}, headers=project.headers)
    assert refused.status_code == 401


async def test_projects_are_isolated(client: AsyncClient, kafka: TopicCollector) -> None:
    a, b = await make_project("A"), await make_project("B")

    await client.post("/v1/batch", json={"batch": [event()]}, headers=a.headers)
    await client.post("/v1/batch", json={"batch": [event(), event()]}, headers=b.headers)

    assert len(await kafka.wait_for(a.id, 1)) == 1
    b_messages = await kafka.wait_for(b.id, 2)
    assert all(m.key.startswith(f"{b.id}:") for m in b_messages)


async def test_kafka_outage_returns_503_with_retry_after(
    client: AsyncClient, project: ProjectFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    class DownProducer:
        async def send(self, *args: object, **kwargs: object) -> None:
            raise KafkaConnectionError("broker unreachable")

    monkeypatch.setattr("app.api.routes.ingest.get_producer", lambda: DownProducer())

    response = await client.post("/v1/batch", json={"batch": [event()]}, headers=project.headers)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "ingest_unavailable"
    assert response.headers["Retry-After"] == "5"


async def test_202_waits_for_broker_acknowledgement(
    client: AsyncClient, project: ProjectFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The broker accepts the send into the buffer but never acknowledges it.
    Answering 202 here would tell the client its events are safe when they
    are only in memory; it must get a 503 and retry instead."""

    class NeverAcks:
        async def send(self, *args: object, **kwargs: object) -> "asyncio.Future[None]":
            future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            future.set_exception(KafkaTimeoutError())
            return future

    monkeypatch.setattr("app.api.routes.ingest.get_producer", lambda: NeverAcks())

    response = await client.post("/v1/batch", json={"batch": [event()]}, headers=project.headers)

    assert response.status_code == 503


def test_cached_partitioner_matches_kafkas_default_partitioner() -> None:
    """Memoizing must not change the key -> partition mapping (other Kafka
    clients producing to the topic must agree with us)."""
    from aiokafka.partitioner import DefaultPartitioner

    from app.core.kafka import cached_partitioner

    partitions = list(range(6))
    for i in range(2_000):
        key = f"project-{i % 7}:user-{i}".encode()
        assert cached_partitioner(key, partitions, partitions) == DefaultPartitioner()(
            key, partitions, partitions
        )
