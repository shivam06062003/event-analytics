import asyncio
import os
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

# Configure the app for tests BEFORE importing it (settings are read at import).
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://analytics:analytics@localhost:5433/analytics_test"
)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
# A fresh topic per test session: tests never see each other's leftovers.
TEST_TOPIC = f"test.events.raw.{uuid.uuid4().hex[:8]}"
os.environ["RAW_EVENTS_TOPIC"] = TEST_TOPIC
os.environ["RAW_EVENTS_PARTITIONS"] = "3"

import asyncpg  # noqa: E402
import orjson  # noqa: E402
from aiokafka import AIOKafkaConsumer  # noqa: E402
from aiokafka.admin import AIOKafkaAdminClient  # noqa: E402
from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.core.db import SessionLocal, engine  # noqa: E402
from app.core.kafka import ensure_topics, start_producer, stop_producer  # noqa: E402
from app.main import app  # noqa: E402
from app.services import projects as project_service  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]


async def _create_database_if_missing(url: str) -> None:
    parsed = make_url(url)
    conn = await asyncpg.connect(
        user=parsed.username,
        password=parsed.password,
        host=parsed.host,
        port=parsed.port,
        database="postgres",
    )
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", parsed.database):
            await conn.execute(f'CREATE DATABASE "{parsed.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> None:
    asyncio.run(_create_database_if_missing(TEST_DATABASE_URL))
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "migrations"))
    command.upgrade(config, "head")


@dataclass
class Message:
    key: str
    partition: int
    value: dict[str, Any]


class TopicCollector:
    """Consumes the test topic in the background and indexes messages by
    project, so each test can wait for exactly the events it produced."""

    def __init__(self) -> None:
        self.by_project: dict[str, list[Message]] = defaultdict(list)
        self._task: asyncio.Task[None] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TEST_TOPIC,
            bootstrap_servers=get_settings().kafka_bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        assert self._consumer is not None
        async for record in self._consumer:
            value = orjson.loads(record.value)
            self.by_project[value["project_id"]].append(
                Message((record.key or b"").decode(), record.partition, value)
            )

    async def wait_for(
        self, project_id: str, count: int, wait_seconds: float = 10
    ) -> list[Message]:
        deadline = asyncio.get_running_loop().time() + wait_seconds
        while len(self.by_project[project_id]) < count:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(
                    f"expected {count} messages for {project_id}, got "
                    f"{len(self.by_project[project_id])}"
                )
            await asyncio.sleep(0.05)
        return self.by_project[project_id]

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._consumer:
            await self._consumer.stop()


@pytest.fixture(scope="session")
async def kafka() -> AsyncIterator[TopicCollector]:
    settings = get_settings()
    await ensure_topics(settings)
    await start_producer()
    collector = TopicCollector()
    await collector.start()
    yield collector
    await collector.stop()
    await stop_producer()
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        await admin.delete_topics([TEST_TOPIC])
    finally:
        await admin.close()


@pytest.fixture(autouse=True)
async def clean_state() -> None:
    project_service.clear_write_key_cache()
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE write_keys, projects"))


@pytest.fixture
async def client(kafka: TopicCollector) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@dataclass
class ProjectFixture:
    id: str
    write_key: str
    write_key_id: uuid.UUID

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.write_key}"}


@pytest.fixture
async def project() -> ProjectFixture:
    return await make_project()


async def make_project(name: str = "Test project") -> ProjectFixture:
    async with SessionLocal() as session:
        created = await project_service.create_project(session, name)
    return ProjectFixture(str(created.project_id), created.write_key, created.write_key_id)


def event(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event": "page_viewed",
        "anonymous_id": "anon-1",
        "properties": {"path": "/pricing"},
    }
    body.update(overrides)
    return body
