import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient

from app.processor.sink import ClickHouseSink
from app.processor.transform import EventRow

# A Monday well in the past: results are deterministic and cached with the
# "historical" TTL.
MONDAY = datetime(2026, 6, 1, tzinfo=UTC)


def at(days: float = 0, hours: float = 0) -> datetime:
    return MONDAY + timedelta(days=days, hours=hours)


def row(
    project_id: str,
    event: str,
    user: str,
    when: datetime,
    properties: dict[str, Any] | None = None,
    event_id: uuid.UUID | None = None,
) -> EventRow:
    return EventRow(
        project_id=uuid.UUID(project_id),
        event_id=event_id or uuid.uuid4(),
        event=event,
        distinct_id=user,
        user_id=user,
        anonymous_id=None,
        timestamp=when,
        client_timestamp=None,
        received_at=when,
        properties=orjson.dumps(properties or {}).decode(),
        context="{}",
        ip=None,
        kafka_partition=0,
        kafka_offset=0,
    )


async def insert(ch: ClickHouseClient, rows: list[EventRow]) -> None:
    await ClickHouseSink(ch).insert(rows)


def time_range(start: datetime, end: datetime) -> dict[str, str]:
    return {"from": start.isoformat(), "to": end.isoformat()}
