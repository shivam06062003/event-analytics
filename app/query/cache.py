"""Query result cache (Redis) with request coalescing.

Freshness: results whose time range reaches into the last hour are cached
briefly (data there is still arriving); purely historical ranges are cached
for an hour. Bounded staleness, not invalidation: late events for an old range
can take up to that TTL to show up.

Coalescing ("single flight"): a dashboard opened by ten people at once sends
ten identical queries. Only the first one runs; the rest await its result.
This is per process; across API replicas each process runs it once at most.
"""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
import structlog
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings

logger = structlog.get_logger()

_redis: Redis | None = None
_inflight: dict[str, asyncio.Future[dict[str, Any]]] = {}


def set_redis(redis: Redis | None) -> None:
    global _redis
    _redis = redis


def cache_key(project_id: str, kind: str, query: BaseModel) -> str:
    canonical = orjson.dumps(
        query.model_dump(mode="json", by_alias=True), option=orjson.OPT_SORT_KEYS
    )
    return f"q:{project_id}:{kind}:{hashlib.sha256(canonical).hexdigest()}"


def ttl_for(range_end: datetime) -> int:
    settings = get_settings()
    if range_end > datetime.now(UTC) - timedelta(hours=1):
        return settings.query_cache_ttl_recent_seconds
    return settings.query_cache_ttl_historical_seconds


async def get_or_compute(
    key: str, ttl: int, compute: Callable[[], Awaitable[dict[str, Any]]]
) -> tuple[dict[str, Any], bool]:
    """Returns (result, served_from_cache)."""
    if _redis is not None:
        try:
            raw = await _redis.get(key)
        except RedisError as exc:
            logger.warning("query_cache_unavailable", error=str(exc))  # fail open: just compute
            raw = None
        if raw is not None:
            return orjson.loads(raw), True

    inflight = _inflight.get(key)
    if inflight is not None:
        return await asyncio.shield(inflight), False

    future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
    _inflight[key] = future
    try:
        result = await compute()
    except BaseException as exc:
        future.set_exception(exc)
        future.exception()  # mark retrieved: no "never retrieved" warning if nobody waited
        raise
    finally:
        _inflight.pop(key, None)
    future.set_result(result)
    if _redis is not None:
        try:
            await _redis.set(key, orjson.dumps(result), ex=ttl)
        except RedisError as exc:
            logger.warning("query_cache_write_failed", error=str(exc))
    return result, False
