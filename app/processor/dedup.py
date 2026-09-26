"""Windowed de-duplication by event_id (Redis).

Two kinds of duplicates reach the processor:

1. Kafka redelivery: the processor crashed after inserting but before
   committing offsets. The replayed message is byte-identical, so the row is
   identical, and ClickHouse's ReplacingMergeTree collapses it at merge time.
2. Client retries: the SDK resent a batch after a 503/timeout. The copy has a
   NEW received_at, so its corrected timestamp (part of the ClickHouse sort key)
   differs slightly, and ReplacingMergeTree would NOT collapse it.

This module handles case 2: remember recently stored event_ids for a window.

Ordering matters. Ids are marked AFTER the ClickHouse insert succeeds. Marking
first would lose events: crash between mark and insert, and on replay the
event looks "already seen" but was never stored. Marking after means a crash
between insert and mark causes a case-1 duplicate, which the table handles.

No cross-consumer race exists: a retry carries the same partition key as the
original, so both land on the same partition and one consumer sees them in order.
"""

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.processor.transform import EventRow

logger = structlog.get_logger()


class Deduplicator:
    def __init__(self, redis: Redis, window_seconds: int) -> None:
        self.redis = redis
        self.window_seconds = window_seconds

    async def filter_new(self, rows: list[EventRow]) -> tuple[list[EventRow], int]:
        """Drop rows already stored within the window, and repeats within the
        batch itself. Returns (new rows, number of duplicates dropped)."""
        unique: dict[str, EventRow] = {}
        for row in rows:
            unique.setdefault(row.dedup_key, row)
        if not unique:
            return [], len(rows)
        keys = list(unique)
        try:
            seen = await self.redis.mget(keys)
        except RedisError as exc:
            # Fail open: keep processing rather than stall the pipeline. The
            # cost is possible client-retry duplicates while Redis is down.
            logger.warning("dedup_unavailable", error=str(exc))
            return list(unique.values()), len(rows) - len(unique)
        new = [unique[key] for key, hit in zip(keys, seen, strict=True) if hit is None]
        return new, len(rows) - len(new)

    async def mark(self, rows: list[EventRow]) -> None:
        if not rows:
            return
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                for row in rows:
                    pipe.set(row.dedup_key, 1, ex=self.window_seconds)
                await pipe.execute()
        except RedisError as exc:
            logger.warning("dedup_mark_failed", error=str(exc), rows=len(rows))
