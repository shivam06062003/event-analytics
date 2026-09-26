"""Consumer loop: Kafka (events.raw) -> ClickHouse, at-least-once.

Per batch:
    poll -> parse -> dedup -> INSERT -> dead letters -> mark ids -> COMMIT offsets

Offsets are committed LAST. If anything fails or the process dies before the
commit, the batch is simply consumed again after restart; duplicates from that
replay are removed downstream (see dedup.py). An event is never marked as
processed without having been stored.
"""

import asyncio
from dataclasses import dataclass

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import CommitFailedError

from app.core.config import Settings
from app.processor.dedup import Deduplicator
from app.processor.sink import Sink
from app.processor.transform import DeadLetter, EventRow, parse

logger = structlog.get_logger()


def build_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.raw_events_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.processor_group_id,
        # We commit manually after the data is safely stored. Auto-commit
        # commits on a timer, possibly BEFORE the insert: a crash then loses data.
        enable_auto_commit=False,
        # A brand-new consumer group starts from the oldest retained message.
        auto_offset_reset="earliest",
    )


@dataclass(frozen=True)
class BatchResult:
    consumed: int = 0
    inserted: int = 0
    duplicates: int = 0
    dead_lettered: int = 0
    committed: bool = False


class Processor:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        sink: Sink,
        dedup: Deduplicator,
        settings: Settings,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.sink = sink
        self.dedup = dedup
        self.settings = settings
        self.stopping = asyncio.Event()

    async def run_once(self, poll_timeout_ms: int | None = None) -> BatchResult:
        batches = await self.consumer.getmany(
            timeout_ms=poll_timeout_ms or self.settings.processor_poll_timeout_ms,
            max_records=self.settings.processor_max_batch,
        )
        records = [record for partition_records in batches.values() for record in partition_records]
        if not records:
            return BatchResult()

        rows: list[EventRow] = []
        dead: list[DeadLetter] = []
        for record in records:
            parsed = parse(record.value, record.key, record.partition, record.offset)
            if isinstance(parsed, EventRow):
                rows.append(parsed)
            else:
                dead.append(parsed)

        new_rows, duplicates = await self.dedup.filter_new(rows)
        if new_rows and not await self._insert_with_retry(new_rows):
            return BatchResult(consumed=len(records))  # shutting down: don't commit
        if dead:
            await self._send_dead_letters(dead)
        await self.dedup.mark(new_rows)
        committed = await self._commit()

        result = BatchResult(len(records), len(new_rows), duplicates, len(dead), committed)
        logger.info(
            "batch_processed",
            consumed=result.consumed,
            inserted=result.inserted,
            duplicates=result.duplicates,
            dead_lettered=result.dead_lettered,
            lag=await self.lag(),
        )
        return result

    async def _insert_with_retry(self, rows: list[EventRow]) -> bool:
        """Retry until the insert succeeds (True) or we are asked to stop (False).

        While ClickHouse is down we PAUSE the partitions but keep polling. A
        consumer that stops polling for longer than max.poll.interval is kicked
        out of the group, triggering a rebalance storm. Polling paused
        partitions returns nothing but proves we're alive. Meanwhile Kafka
        buffers new events: that's the backpressure.
        """
        attempt = 0
        while True:
            try:
                await self.sink.insert(rows)
            except Exception as exc:
                attempt += 1
                delay = min(self.settings.processor_retry_max_backoff_seconds, 0.5 * 2**attempt)
                assigned = self.consumer.assignment()
                self.consumer.pause(*assigned)
                logger.warning(
                    "sink_insert_failed", error=repr(exc), attempt=attempt, retry_in=delay
                )
                await self.consumer.getmany(timeout_ms=int(delay * 1000))
                if self.stopping.is_set():
                    return False
                continue
            paused = self.consumer.paused()
            if paused:
                self.consumer.resume(*paused)
                logger.info("sink_recovered", attempts=attempt + 1)
            return True

    async def _send_dead_letters(self, dead: list[DeadLetter]) -> None:
        pending = [
            await self.producer.send(
                self.settings.dead_letter_topic,
                value=letter.value,
                key=letter.key,
                headers=[
                    ("error", letter.reason.encode()),
                    ("source_topic", self.settings.raw_events_topic.encode()),
                    ("source_partition", str(letter.partition).encode()),
                    ("source_offset", str(letter.offset).encode()),
                ],
            )
            for letter in dead
        ]
        await asyncio.gather(*pending)
        for letter in dead:
            logger.warning(
                "dead_lettered",
                reason=letter.reason,
                partition=letter.partition,
                offset=letter.offset,
            )

    async def _commit(self) -> bool:
        try:
            await self.consumer.commit()
            return True
        except CommitFailedError as exc:
            # A rebalance moved our partitions to another consumer mid-batch.
            # It will re-read from the last commit: duplicates, handled downstream.
            logger.warning("commit_failed_after_rebalance", error=str(exc))
            return False

    async def lag(self) -> int:
        """Messages in our partitions not yet consumed: the key health metric
        of any streaming pipeline (growing lag = we can't keep up)."""
        total = 0
        for partition in self.consumer.assignment():
            highwater = self.consumer.highwater(partition)
            if highwater is not None:
                total += max(0, highwater - await self.consumer.position(partition))
        return total
