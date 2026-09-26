"""Event ingestion: validate a batch, fix timestamps, append it to the event log.

The API does as little as possible: validate, stamp, write to Kafka, return.
Everything slow (enrichment, dedup, storage) happens downstream in consumers,
so the ingestion path stays fast and keeps accepting events even if ClickHouse
is down: Kafka buffers them until the processors catch up.
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import orjson
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from app.core.config import get_settings
from app.schemas.events import BatchRequest, RejectedEvent, TrackEvent
from app.services.errors import BatchTooLarge, IngestUnavailable, NoValidEvents

logger = structlog.get_logger()

MESSAGE_SCHEMA_VERSION = 1


@dataclass
class IngestResult:
    accepted: int
    rejected: list[RejectedEvent] = field(default_factory=list)


def corrected_timestamp(
    client_timestamp: datetime | None,
    sent_at: datetime | None,
    received_at: datetime,
    max_future_skew: timedelta,
) -> datetime:
    """When the event really happened, correcting for a wrong client clock.

    Device clocks are often off by minutes or hours. The absolute time is
    unreliable, but the DIFFERENCE between two readings of the same clock is
    fine. So if the client says it sent the batch 5s after the event, the
    event happened 5s before we received the batch:

        timestamp = received_at - (sent_at - client_timestamp)

    (Network latency is ignored; it is small compared with typical skew.)
    """
    if client_timestamp is None:
        return received_at
    timestamp = client_timestamp
    if sent_at is not None:
        # Without sent_at we can't measure the skew, so we trust the client.
        timestamp = received_at - (sent_at - client_timestamp)
    if timestamp > received_at + max_future_skew:
        # An event can't happen after we received it; the clock is just wrong.
        return received_at
    return timestamp


def partition_key(project_id: uuid.UUID, distinct_id: str) -> bytes:
    """Kafka guarantees order only within a partition, and a key always maps
    to the same partition. Keying by (project, user) keeps each user's events
    in order (sessionization needs that) while spreading a big project's
    traffic across all partitions, instead of one hot partition per tenant."""
    return f"{project_id}:{distinct_id}".encode()


def build_message(
    project_id: uuid.UUID,
    event: TrackEvent,
    *,
    sent_at: datetime | None,
    received_at: datetime,
    ip: str | None,
) -> bytes:
    settings = get_settings()
    message: dict[str, Any] = {
        # Consumers branch on this when the format evolves, so old and new
        # messages can coexist in the topic during a rollout.
        "schema_version": MESSAGE_SCHEMA_VERSION,
        "project_id": str(project_id),
        "event_id": str(event.event_id),
        "event": event.event,
        "distinct_id": event.distinct_id,
        "user_id": event.user_id,
        "anonymous_id": event.anonymous_id,
        "timestamp": corrected_timestamp(
            event.timestamp,
            sent_at,
            received_at,
            timedelta(seconds=settings.max_future_skew_seconds),
        ).isoformat(),
        "client_timestamp": event.timestamp.isoformat() if event.timestamp else None,
        "sent_at": sent_at.isoformat() if sent_at else None,
        "received_at": received_at.isoformat(),
        "ip": ip,
        "properties": event.properties,
        "context": event.context,
    }
    return orjson.dumps(message)


def _validate(batch: list[dict[str, Any]]) -> tuple[list[TrackEvent], list[RejectedEvent]]:
    valid: list[TrackEvent] = []
    rejected: list[RejectedEvent] = []
    for index, raw in enumerate(batch):
        try:
            valid.append(TrackEvent.model_validate(raw))
        except ValidationError as exc:
            event_id = raw.get("event_id") if isinstance(raw, dict) else None
            rejected.append(
                RejectedEvent(
                    index=index,
                    event_id=str(event_id) if event_id is not None else None,
                    errors=[
                        f"{'.'.join(str(p) for p in err['loc']) or 'event'}: {err['msg']}"
                        for err in exc.errors()
                    ],
                )
            )
    return valid, rejected


async def ingest_batch(
    producer: AIOKafkaProducer,
    project_id: uuid.UUID,
    request: BatchRequest,
    *,
    received_at: datetime,
    ip: str | None,
) -> IngestResult:
    settings = get_settings()
    if len(request.batch) > settings.max_batch_events:
        raise BatchTooLarge(len(request.batch), settings.max_batch_events)

    # Partial acceptance: one malformed event (an SDK bug, a bad property)
    # must not make the client drop or endlessly retry 499 good ones.
    valid, rejected = _validate(request.batch)
    if not valid:
        raise NoValidEvents([r.model_dump() for r in rejected])

    try:
        # send() only enqueues into the producer's buffer (and blocks if the
        # buffer is full: natural backpressure). The returned futures resolve
        # when the broker acknowledges. We answer 202 only after ALL acks, so
        # "accepted" means durably written, not "sitting in memory".
        pending = [
            await producer.send(
                settings.raw_events_topic,
                value=build_message(
                    project_id, event, sent_at=request.sent_at, received_at=received_at, ip=ip
                ),
                key=partition_key(project_id, event.distinct_id),
            )
            for event in valid
        ]
        await asyncio.wait_for(
            asyncio.gather(*pending), timeout=settings.kafka_send_timeout_seconds + 5
        )
    except (KafkaError, TimeoutError) as exc:
        # Some events may have been written before the failure. That's fine:
        # the client retries the whole batch with the same event_ids, and the
        # processor de-duplicates (Phase 2).
        logger.warning("ingest_unavailable", error=repr(exc), events=len(valid))
        raise IngestUnavailable("Event log unavailable; retry the batch shortly") from exc

    return IngestResult(accepted=len(valid), rejected=rejected)
