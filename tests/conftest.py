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
SESSION = uuid.uuid4().hex[:8]
TEST_DLQ_TOPIC = f"test.events.dlq.{SESSION}"
os.environ["DEAD_LETTER_TOPIC"] = TEST_DLQ_TOPIC
os.environ["CLICKHOUSE_DATABASE"] = f"analytics_test_{SESSION}"
os.environ["CLICKHOUSE_MIGRATIONS_DIR"] = str(
    Path(__file__).resolve().parents[1] / "clickhouse/migrations"
)
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://localhost:6381/14")
os.environ["PROCESSOR_GROUP_ID"] = f"test-processor-{SESSION}"

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
from app.services import tracking_plans  # noqa: E402

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
            # The topic deliberately contains poison messages (see the DLQ
            # test). Skip anything we can't index, like the real processor
            # does, or this background task dies and every later wait_for()
            # times out with "got 0".
            try:
                value = orjson.loads(record.value)
                project_id = value["project_id"]
            except (orjson.JSONDecodeError, KeyError, TypeError):
                continue
            self.by_project[project_id].append(
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
        await admin.delete_topics([TEST_TOPIC, TEST_DLQ_TOPIC])
    finally:
        await admin.close()


@pytest.fixture(autouse=True)
async def clean_state() -> None:
    project_service.clear_write_key_cache()
    tracking_plans.clear_cache()
    async with engine.begin() as conn:
        # Every table except Alembic's bookkeeping, discovered rather than
        # listed, so a new table with a foreign key can't break cleanup.
        tables = (
            await conn.scalars(
                text(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
                )
            )
        ).all()
        await conn.execute(text(f"TRUNCATE {', '.join(tables)}"))


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


# --- Processor fixtures (ClickHouse + Redis) -----------------------------------

from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient  # noqa: E402
from redis.asyncio import Redis  # noqa: E402

from app.core import clickhouse as clickhouse_core  # noqa: E402
from app.core.kafka import build_producer  # noqa: E402
from app.processor.dedup import Deduplicator  # noqa: E402
from app.processor.processor import BatchResult, Processor, build_consumer  # noqa: E402
from app.processor.sink import ClickHouseSink  # noqa: E402


@pytest.fixture(scope="session")
async def ch(kafka: TopicCollector) -> AsyncIterator[ClickHouseClient]:
    """A throwaway ClickHouse database per session, built by the real migrations."""
    settings = get_settings()
    await clickhouse_core.migrate(settings)
    client = await clickhouse_core.create_client(settings)
    yield client
    await client.command(f"DROP DATABASE IF EXISTS `{settings.clickhouse_database}`")
    await client.close()


@pytest.fixture(scope="session")
async def redis() -> AsyncIterator[Redis]:
    client = Redis.from_url(get_settings().redis_url)
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture(scope="session")
async def processor(ch: ClickHouseClient, redis: Redis) -> AsyncIterator[Processor]:
    """One consumer group for the whole session (joining a group takes a few
    seconds). Each test drains whatever it produced."""
    settings = get_settings()
    consumer = build_consumer(settings)
    producer = build_producer(settings)
    await producer.start()
    await consumer.start()
    proc = Processor(
        consumer,
        producer,
        ClickHouseSink(ch),
        Deduplicator(redis, settings.dedup_window_seconds),
        settings,
    )
    yield proc
    await consumer.stop()
    await producer.stop()


@pytest.fixture(autouse=True)
async def clean_processor_state(request: pytest.FixtureRequest) -> None:
    if "processor" in request.fixturenames or "ch" in request.fixturenames:
        ch_client: ClickHouseClient = request.getfixturevalue("ch")
        await ch_client.command("TRUNCATE TABLE events")
        redis_client: Redis = request.getfixturevalue("redis")
        await redis_client.flushdb()


async def drain(processor: Processor, max_empty_polls: int = 3) -> list[BatchResult]:
    """Run the processor until the topic is caught up."""
    results: list[BatchResult] = []
    empty = 0
    while empty < max_empty_polls:
        result = await processor.run_once(poll_timeout_ms=300)
        if result.consumed:
            results.append(result)
            empty = 0
        else:
            empty += 1
    return results


# --- Query API fixtures ----------------------------------------------------------

from app.query import cache as query_cache  # noqa: E402
from app.query import executor as query_executor  # noqa: E402


@pytest.fixture(scope="session")
async def query_backends(ch: ClickHouseClient, redis: Redis) -> AsyncIterator[None]:
    query_executor.set_client(ch)
    query_cache.set_redis(redis)
    yield
    query_executor.set_client(None)
    query_cache.set_redis(None)


@dataclass
class Reader:
    project: ProjectFixture
    read_key: str
    read_key_id: uuid.UUID

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.read_key}"}


@pytest.fixture
async def reader(project: ProjectFixture, query_backends: None) -> Reader:
    async with SessionLocal() as session:
        created = await project_service.create_read_key(session, uuid.UUID(project.id))
    return Reader(project, created.read_key, created.read_key_id)
