import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import orjson
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from httpx import AsyncClient
from redis.asyncio import Redis

from app.core import clickhouse as clickhouse_core
from app.core.config import get_settings
from app.core.kafka import get_producer
from app.processor.dedup import Deduplicator
from app.processor.processor import Processor
from app.processor.sink import ClickHouseSink
from app.processor.transform import EventRow, parse
from tests.conftest import TEST_DLQ_TOPIC, TEST_TOPIC, ProjectFixture, drain, event


async def rows_for(ch: ClickHouseClient, project_id: str, *, final: bool = False) -> list[tuple]:
    result = await ch.query(
        f"SELECT event_id, event, distinct_id, properties, kafka_partition "
        f"FROM events {'FINAL' if final else ''} WHERE project_id = %(p)s ORDER BY event",
        parameters={"p": project_id},
    )
    return list(result.result_rows)


async def send(client: AsyncClient, project: ProjectFixture, *events: dict, sent_at=None) -> None:
    body: dict = {"batch": list(events)}
    if sent_at:
        body["sent_at"] = sent_at.isoformat()
    response = await client.post("/v1/batch", json=body, headers=project.headers)
    assert response.status_code == 202, response.text


async def test_ingested_events_land_in_clickhouse(
    client: AsyncClient, project: ProjectFixture, processor: Processor, ch: ClickHouseClient
) -> None:
    signup = event(event="signup", user_id="u-1", properties={"plan": "pro"})
    await send(client, project, signup, event(event="page_viewed"))

    await drain(processor)

    rows = await rows_for(ch, project.id)
    assert [(r[1], r[2]) for r in rows] == [("page_viewed", "anon-1"), ("signup", "u-1")]
    assert str(rows[1][0]) == signup["event_id"]
    assert orjson.loads(rows[1][3]) == {"plan": "pro"}


async def test_client_retry_duplicates_are_removed(
    client: AsyncClient, project: ProjectFixture, processor: Processor, ch: ClickHouseClient
) -> None:
    """The SDK resends a batch after a timeout. The retry gets a NEW received_at,
    so its corrected timestamp (part of the sort key) differs, so the
    ReplacingMergeTree alone would keep both. The Redis window catches it."""
    original = event(event="purchase", timestamp=datetime.now(UTC).isoformat())
    await send(client, project, original, sent_at=datetime.now(UTC))
    await drain(processor)
    await asyncio.sleep(0.01)
    await send(client, project, original, sent_at=datetime.now(UTC) + timedelta(seconds=3))

    results = await drain(processor)

    assert len(await rows_for(ch, project.id)) == 1  # no FINAL needed
    assert sum(r.duplicates for r in results) == 1


async def test_duplicates_within_one_batch_are_removed(
    client: AsyncClient, project: ProjectFixture, processor: Processor, ch: ClickHouseClient
) -> None:
    same = event()
    await send(client, project, same, same, same)

    await drain(processor)

    assert len(await rows_for(ch, project.id)) == 1


async def test_exact_redelivery_is_collapsed_by_replacing_merge_tree(
    ch: ClickHouseClient, processor: Processor
) -> None:
    """Crash after INSERT but before marking/committing: the replay inserts
    byte-identical rows. That's what the table engine is for."""
    now = datetime.now(UTC).isoformat()
    project_id = str(uuid.uuid4())
    raw = orjson.dumps({
        "schema_version": 1, "project_id": project_id, "event_id": str(uuid.uuid4()),
        "event": "signup", "distinct_id": "u", "user_id": "u", "anonymous_id": None,
        "timestamp": now, "client_timestamp": None, "received_at": now, "ip": None,
        "properties": {}, "context": {},
    })  # fmt: skip
    row = parse(raw, None, 0, 5)
    assert isinstance(row, EventRow)
    sink = ClickHouseSink(ch)

    await sink.insert([row])
    await sink.insert([row])  # the replay

    assert len(await rows_for(ch, project_id)) == 2  # before a merge...
    assert len(await rows_for(ch, project_id, final=True)) == 1  # ...FINAL dedups at read time
    await ch.command("OPTIMIZE TABLE events FINAL")  # force the background merge
    assert len(await rows_for(ch, project_id)) == 1


async def test_poison_messages_go_to_the_dead_letter_topic(
    client: AsyncClient, project: ProjectFixture, processor: Processor, ch: ClickHouseClient
) -> None:
    producer = get_producer()
    # Unique keys: other tests also produce poison messages into this session's
    # topic, so we only look at our own dead letters.
    keys = {uuid.uuid4().hex.encode(), uuid.uuid4().hex.encode()}
    broken_key, future_key = sorted(keys)
    await producer.send_and_wait(TEST_TOPIC, b"{broken json", key=broken_key)
    await producer.send_and_wait(TEST_TOPIC, orjson.dumps({"schema_version": 7}), key=future_key)
    await send(client, project, event(event="good"))

    await drain(processor)

    # The good event in the same stream is not held back by the bad ones...
    assert [r[1] for r in await rows_for(ch, project.id)] == ["good"]
    # ...and the bad ones are preserved with the reason, for inspection/replay.
    letters = [
        letter
        for letter in await read_topic(TEST_DLQ_TOPIC, expected=2, key_filter=keys)
        if letter.key in keys
    ]
    reasons = sorted(dict(letter.headers)["error"].decode() for letter in letters)
    assert reasons == ["invalid_json", "unsupported_schema_version:7"]
    assert {letter.value for letter in letters} == {
        b"{broken json",
        orjson.dumps({"schema_version": 7}),
    }


async def test_offsets_are_committed_only_after_the_insert_succeeds(
    client: AsyncClient,
    project: ProjectFixture,
    processor: Processor,
    ch: ClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await drain(processor)  # start from a caught-up position
    committed_before = await committed_offsets(processor)

    class DownSink:
        async def insert(self, rows: list[EventRow]) -> None:
            raise ConnectionError("clickhouse unreachable")

    real_sink = processor.sink
    processor.sink = DownSink()
    monkeypatch.setattr(get_settings(), "processor_retry_max_backoff_seconds", 0.2)
    await send(client, project, event(event="important"))

    # ClickHouse is down: the processor retries, pausing partitions, and must
    # NOT commit. Then we "crash" it (stop) mid-retry.
    task = asyncio.create_task(processor.run_once(poll_timeout_ms=2000))
    await asyncio.sleep(1.0)
    assert processor.consumer.paused(), "partitions should be paused while the sink is down"
    processor.stopping.set()
    result = await task
    processor.stopping.clear()

    assert not result.committed
    assert await committed_offsets(processor) == committed_before
    assert await rows_for(ch, project.id) == []

    # "Restart": rewind to the committed offsets (as a new consumer would) and recover.
    processor.sink = real_sink
    for tp in processor.consumer.assignment():
        processor.consumer.seek(tp, committed_before.get(tp, 0))
    processor.consumer.resume(*processor.consumer.paused())
    await drain(processor)

    assert [r[1] for r in await rows_for(ch, project.id)] == ["important"]


async def test_processing_resumes_by_itself_when_clickhouse_recovers(
    client: AsyncClient,
    project: ProjectFixture,
    processor: Processor,
    ch: ClickHouseClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await drain(processor)
    failures = {"left": 2}
    real_sink = processor.sink

    class FlakySink:
        async def insert(self, rows: list[EventRow]) -> None:
            if failures["left"]:
                failures["left"] -= 1
                raise ConnectionError("blip")
            await real_sink.insert(rows)

    processor.sink = FlakySink()
    monkeypatch.setattr(get_settings(), "processor_retry_max_backoff_seconds", 0.2)
    try:
        await send(client, project, event(event="a"), event(event="b"))
        await drain(processor)
    finally:
        processor.sink = real_sink

    assert [r[1] for r in await rows_for(ch, project.id)] == ["a", "b"]
    assert failures["left"] == 0
    assert processor.consumer.paused() == set()  # resumed after recovery


async def test_dedup_fails_open_when_redis_is_down(ch: ClickHouseClient) -> None:
    dead_redis = Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)
    dedup = Deduplicator(dead_redis, 60)
    now = datetime.now(UTC).isoformat()
    raw = orjson.dumps({
        "schema_version": 1, "project_id": str(uuid.uuid4()), "event_id": str(uuid.uuid4()),
        "event": "e", "distinct_id": "u", "user_id": None, "anonymous_id": "u",
        "timestamp": now, "client_timestamp": None, "received_at": now, "ip": None,
        "properties": {}, "context": {},
    })  # fmt: skip
    row = parse(raw, None, 0, 0)
    assert isinstance(row, EventRow)

    new, duplicates = await dedup.filter_new([row, row])
    await dedup.mark(new)  # must not raise either

    assert (len(new), duplicates) == (1, 1)  # in-batch dedup still works
    await dead_redis.aclose()


async def test_clickhouse_migrations_are_idempotent(ch: ClickHouseClient) -> None:
    assert await clickhouse_core.migrate(get_settings()) == []


# --- helpers ---------------------------------------------------------------------


async def committed_offsets(processor: Processor) -> dict[TopicPartition, int]:
    offsets = {}
    for tp in processor.consumer.assignment():
        committed = await processor.consumer.committed(tp)
        offsets[tp] = committed if committed is not None else 0
    return offsets


async def read_topic(
    topic: str, expected: int, wait_seconds: float = 10, key_filter: set[bytes] | None = None
) -> list:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=get_settings().kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    records: list = []
    try:
        deadline = asyncio.get_running_loop().time() + wait_seconds
        while len(records) < expected and asyncio.get_running_loop().time() < deadline:
            batch = await consumer.getmany(timeout_ms=500)
            for partition_records in batch.values():
                records.extend(
                    r for r in partition_records if key_filter is None or r.key in key_filter
                )
    finally:
        await consumer.stop()
    return records
