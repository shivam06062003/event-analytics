"""Kafka (Redpanda) producer and topic management."""

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.core.config import Settings, get_settings

logger = structlog.get_logger()

_producer: AIOKafkaProducer | None = None


def build_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        # Durability: a send only succeeds once every in-sync replica has the
        # message. With acks=1 a leader crash right after acking loses data.
        acks="all",
        # Idempotent producer: the broker de-duplicates retried sends (it tracks
        # a producer ID + sequence number per partition), so a network retry
        # can't write the same message twice or reorder a partition.
        enable_idempotence=True,
        # Wait up to 5ms to batch messages bound for the same partition: far
        # fewer, larger requests under load, for a tiny latency cost.
        linger_ms=5,
        request_timeout_ms=int(settings.kafka_send_timeout_seconds * 1000),
    )


async def start_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = build_producer(get_settings())
        await _producer.start()
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def get_producer() -> AIOKafkaProducer:
    if _producer is None:
        raise RuntimeError("Kafka producer not started (it starts in the app lifespan)")
    return _producer


TOPIC_ALREADY_EXISTS = 36  # Kafka protocol error code


async def ensure_topics(settings: Settings) -> dict[str, str]:
    """Create topics if missing; returns {topic: "created" | "exists"}.

    Topics are managed as code, like migrations, instead of relying on broker
    auto-creation (which picks default partition counts nobody chose, and
    hides typos in topic names)."""
    topics = [
        NewTopic(
            name=settings.raw_events_topic,
            num_partitions=settings.raw_events_partitions,
            replication_factor=settings.kafka_replication_factor,
        ),
        # Messages the processor could not handle. Low volume; kept for
        # inspection and replay after a fix.
        NewTopic(
            name=settings.dead_letter_topic,
            num_partitions=settings.dead_letter_partitions,
            replication_factor=settings.kafka_replication_factor,
        ),
    ]
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    outcome: dict[str, str] = {}
    try:
        # Note: aiokafka does NOT raise on per-topic failures; it returns error
        # codes in the response. Unchecked, every failure looks like success.
        response = await admin.create_topics(topics)
        for name, error_code, error_message in response.topic_errors:
            if error_code == 0:
                outcome[name] = "created"
            elif error_code == TOPIC_ALREADY_EXISTS:
                outcome[name] = "exists"
            else:
                raise RuntimeError(f"creating topic {name} failed: {error_code} {error_message}")
            logger.info("topic_ensured", topic=name, outcome=outcome[name])
    finally:
        await admin.close()
    return outcome
