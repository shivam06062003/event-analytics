import pytest
from aiokafka.admin import AIOKafkaAdminClient
from httpx import AsyncClient

from app.core.config import get_settings
from app.core.kafka import ensure_topics
from tests.conftest import TEST_TOPIC


async def test_live(client: AsyncClient) -> None:
    assert (await client.get("/health/live")).json() == {"status": "ok"}


async def test_ready_checks_database_and_kafka(client: AsyncClient) -> None:
    response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok", "kafka": "ok"}


async def test_not_ready_when_kafka_is_unreachable(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Broken:
        async def partitions_for(self, topic: str) -> None:
            raise ConnectionError("no brokers")

    monkeypatch.setattr("app.api.routes.health.get_producer", lambda: Broken())

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["error"]["message"] == "event log unavailable"


async def test_ensure_topics_is_idempotent_and_sets_partitions(client: AsyncClient) -> None:
    settings = get_settings()
    await ensure_topics(settings)  # already exists: must not fail

    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        [description] = await admin.describe_topics([TEST_TOPIC])
    finally:
        await admin.close()
    assert len(description["partitions"]) == settings.raw_events_partitions
