"""Kafka message -> ClickHouse row, or a dead letter explaining why not."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import orjson
from pydantic import AwareDatetime, BaseModel, ValidationError

# Column order for inserts; EventRow.values() must match it exactly.
COLUMNS = (
    "project_id", "event_id", "event", "distinct_id", "user_id", "anonymous_id",
    "timestamp", "client_timestamp", "received_at", "properties", "context", "ip",
    "kafka_partition", "kafka_offset", "violations",
)  # fmt: skip


class RawEventV1(BaseModel):
    """The message format the ingestion API writes (schema_version 1)."""

    schema_version: Literal[1]
    project_id: uuid.UUID
    event_id: uuid.UUID
    event: str
    distinct_id: str
    user_id: str | None
    anonymous_id: str | None
    timestamp: AwareDatetime
    client_timestamp: AwareDatetime | None
    received_at: AwareDatetime
    ip: str | None
    properties: dict[str, Any]
    context: dict[str, Any]
    # Added in Phase 4 WITHOUT bumping schema_version: an optional field with a
    # default is backward compatible (old messages still parse) and forward
    # compatible (an old processor ignores the unknown field).
    violations: list[str] = []


@dataclass(frozen=True)
class EventRow:
    project_id: uuid.UUID
    event_id: uuid.UUID
    event: str
    distinct_id: str
    user_id: str | None
    anonymous_id: str | None
    timestamp: datetime
    client_timestamp: datetime | None
    received_at: datetime
    properties: str
    context: str
    ip: str | None
    kafka_partition: int
    kafka_offset: int
    violations: tuple[str, ...] = ()

    @property
    def dedup_key(self) -> str:
        return f"dedup:{self.project_id}:{self.event_id}"

    def values(self) -> tuple[Any, ...]:
        return tuple(getattr(self, column) for column in COLUMNS)


@dataclass(frozen=True)
class DeadLetter:
    value: bytes
    key: bytes | None
    partition: int
    offset: int
    reason: str


def parse(value: bytes, key: bytes | None, partition: int, offset: int) -> EventRow | DeadLetter:
    """Never raises: a message we can't handle becomes a DeadLetter instead of
    crashing the processor. Otherwise one poison message would block its
    partition forever (the consumer would retry it on every restart)."""

    def dead(reason: str) -> DeadLetter:
        return DeadLetter(value, key, partition, offset, reason)

    try:
        data = orjson.loads(value)
    except orjson.JSONDecodeError:
        return dead("invalid_json")
    if not isinstance(data, dict):
        return dead("not_an_object")
    version = data.get("schema_version")
    if version != 1:
        return dead(f"unsupported_schema_version:{version}")
    try:
        message = RawEventV1.model_validate(data)
    except ValidationError as exc:
        first = exc.errors()[0]
        return dead(f"invalid_message:{'.'.join(str(p) for p in first['loc'])}:{first['msg']}")

    return EventRow(
        project_id=message.project_id,
        event_id=message.event_id,
        event=message.event,
        distinct_id=message.distinct_id,
        user_id=message.user_id,
        anonymous_id=message.anonymous_id,
        timestamp=message.timestamp,
        client_timestamp=message.client_timestamp,
        received_at=message.received_at,
        properties=orjson.dumps(message.properties).decode(),
        context=orjson.dumps(message.context).decode(),
        ip=message.ip,
        kafka_partition=partition,
        kafka_offset=offset,
        violations=tuple(message.violations),
    )
