"""Run the processor: `python -m app.processor`. Scale by running more
replicas (up to one per partition); the consumer group splits partitions."""

import asyncio
import signal
from pathlib import Path

import structlog
from prometheus_client import start_http_server
from redis.asyncio import Redis

from app.core.clickhouse import create_client
from app.core.config import get_settings
from app.core.kafka import build_producer
from app.core.logging import configure_logging
from app.core.tracing import configure_tracing
from app.processor.dedup import Deduplicator
from app.processor.processor import Processor, build_consumer
from app.processor.sink import ClickHouseSink

logger = structlog.get_logger()


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    configure_tracing(settings)
    # The processor has no web framework; Prometheus scrapes this port.
    start_http_server(settings.processor_metrics_port)

    consumer = build_consumer(settings)
    producer = build_producer(settings)
    clickhouse = await create_client(settings)
    redis = Redis.from_url(settings.redis_url, socket_timeout=1, socket_connect_timeout=1)
    processor = Processor(
        consumer,
        producer,
        ClickHouseSink(clickhouse),
        Deduplicator(redis, settings.dedup_window_seconds),
        settings,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, processor.stopping.set)

    heartbeat = Path(settings.processor_heartbeat_path)
    await producer.start()
    await consumer.start()
    logger.info("processor_started", group=settings.processor_group_id)
    try:
        while not processor.stopping.is_set():
            try:
                await processor.run_once()
            except Exception:
                logger.exception("processor_iteration_failed")
                await asyncio.sleep(1)
            await asyncio.to_thread(heartbeat.touch)
    finally:
        # Leaving the group explicitly lets partitions move to other replicas
        # immediately instead of after a session timeout.
        await consumer.stop()
        await producer.stop()
        await clickhouse.close()
        await redis.aclose()
        logger.info("processor_stopped")


if __name__ == "__main__":
    asyncio.run(main())
