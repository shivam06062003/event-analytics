"""Event ingestion: validate a batch, fix timestamps, append it to the event log.

The API does as little as possible: validate, stamp, write to Kafka, return.
Everything slow (enrichment, dedup, storage) happens downstream in consumers,
so the ingestion path stays fast and keeps accepting events even if ClickHouse
is down: Kafka buffers them until the processors catch up.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import orjson
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from app.core import quota
from app.core.config import get_settings
from app.core.metrics import (
    INGEST_BATCH_SIZE,
    INGEST_EVENTS,
    KAFKA_PRODUCE_DURATION,
    QUOTA_REJECTIONS,
)
from app.core.tracing import kafka_headers
from app.schemas.events import BatchRequest, RejectedEvent, TrackEvent
from app.services import tracking_plans
from app.services.errors import BatchTooLarge, IngestUnavailable, NoValidEvents, QuotaExceeded

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
    violations: list[str] | None = None,
    received_at_iso: str | None = None,
    sent_at_iso: str | None = None,
) -> bytes:
    """received_at_iso/sent_at_iso: pre-formatted once per batch by the caller
    (identical for every event in it)."""
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
        "sent_at": sent_at_iso or (sent_at.isoformat() if sent_at else None),
        "received_at": received_at_iso or received_at.isoformat(),
        "ip": ip,
        "properties": event.properties,
        "context": event.context,
        "violations": violations or [],
    }
    return orjson.dumps(message)


def _validate(
    batch: list[dict[str, Any]],
) -> tuple[list[tuple[int, TrackEvent]], list[RejectedEvent]]:
    """Returns (index in batch, event) for valid events, so later rejections
    (e.g. by the tracking plan) can still report the client's original index."""
    valid: list[tuple[int, TrackEvent]] = []
    rejected: list[RejectedEvent] = []
    for index, raw in enumerate(batch):
        try:
            valid.append((index, TrackEvent.model_validate(raw)))
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
    INGEST_BATCH_SIZE.observe(len(request.batch))
    if len(request.batch) > settings.max_batch_events:
        raise BatchTooLarge(len(request.batch), settings.max_batch_events)

    # Charged per EVENT before any work: a tenant over quota costs us almost
    # nothing, and a 500-event batch counts as 500, not 1.
    ingest_quota = quota.get("ingest")
    if ingest_quota is not None:
        charge = await ingest_quota.charge(str(project_id), cost=len(request.batch))
        if not charge.allowed:
            INGEST_EVENTS.labels("rejected_quota").inc(len(request.batch))
            QUOTA_REJECTIONS.labels("ingest").inc()
            raise QuotaExceeded(
                "Project event quota exceeded; retry after the indicated delay",
                charge.retry_after_seconds,
            )

    # Partial acceptance: one malformed event (an SDK bug, a bad property)
    # must not make the client drop or endlessly retry 499 good ones.
    valid, rejected = _validate(request.batch)

    # Tracking plan: in "block" mode violating events join `rejected`; in
    # "warn" mode they go through, carrying their violations downstream.
    plan = await tracking_plans.active_plan(project_id)
    checked: list[tuple[TrackEvent, list[str]]] = []
    for index, event in valid:
        problems = tracking_plans.violations(event, plan) if plan else []
        if problems and plan is not None and plan.enforcement == "block":
            rejected.append(
                RejectedEvent(
                    index=index,
                    event_id=str(event.event_id),
                    errors=[f"tracking_plan: {p}" for p in problems],
                )
            )
        else:
            checked.append((event, problems))
    rejected.sort(key=lambda r: r.index)
    plan_rejections = len(valid) - len(checked)
    INGEST_EVENTS.labels("rejected_validation").inc(len(request.batch) - len(valid))
    INGEST_EVENTS.labels("rejected_plan").inc(plan_rejections)
    if not checked:
        raise NoValidEvents([r.model_dump() for r in rejected])

    headers = kafka_headers()  # the request's trace context, carried in Kafka headers
    received_at_iso = received_at.isoformat()
    sent_at_iso = request.sent_at.isoformat() if request.sent_at else None
    started = time.perf_counter()
    try:
        # send() only enqueues into the producer's buffer (and blocks if the
        # buffer is full: natural backpressure). The returned futures resolve
        # when the broker acknowledges. We answer 202 only after ALL acks, so
        # "accepted" means durably written, not "sitting in memory".
        pending = [
            await producer.send(
                settings.raw_events_topic,
                value=build_message(
                    project_id,
                    event,
                    sent_at=request.sent_at,
                    received_at=received_at,
                    ip=ip,
                    violations=problems,
                    received_at_iso=received_at_iso,
                    sent_at_iso=sent_at_iso,
                ),
                key=partition_key(project_id, event.distinct_id),
                headers=headers,
            )
            for event, problems in checked
        ]
        await asyncio.wait_for(
            asyncio.gather(*pending), timeout=settings.kafka_send_timeout_seconds + 5
        )
    except (KafkaError, TimeoutError) as exc:
        # Some events may have been written before the failure. That's fine:
        # the client retries the whole batch with the same event_ids, and the
        # processor de-duplicates (Phase 2).
        logger.warning("ingest_unavailable", error=repr(exc), events=len(checked))
        raise IngestUnavailable("Event log unavailable; retry the batch shortly") from exc

    KAFKA_PRODUCE_DURATION.observe(time.perf_counter() - started)
    INGEST_EVENTS.labels("accepted").inc(len(checked))
    return IngestResult(accepted=len(checked), rejected=rejected)
