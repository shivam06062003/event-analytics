"""Per-project quotas: token buckets in Redis.

The same atomic Lua token bucket as the ledger project's rate limiter, with
one difference: a request can cost MORE than one token. Ingestion charges one
token per event, so a client sending 500-event batches is limited on events,
not requests. Without that, batching would be a free way around the quota.

Fail-open: if Redis is down, requests pass (and a metric counts it). A quota
outage must not become an ingestion outage.
"""

from dataclasses import dataclass

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import Settings
from app.core.metrics import QUOTA_BACKEND_ERRORS

logger = structlog.get_logger()

# KEYS[1] bucket; ARGV burst, refill rate (tokens/s), cost.
# Returns {allowed, tokens remaining (floored), retry after ms}.
TOKEN_BUCKET_LUA = """
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1]) or burst
local last_ms = tonumber(bucket[2]) or now_ms
tokens = math.min(burst, tokens + (now_ms - last_ms) / 1000 * rate)
local allowed = 0
local retry_after_ms = 0
if tokens >= cost then
    allowed = 1
    tokens = tokens - cost
else
    retry_after_ms = math.ceil((cost - tokens) / rate * 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, math.floor(tokens), retry_after_ms}
"""


@dataclass(frozen=True)
class QuotaResult:
    allowed: bool
    retry_after_seconds: float


class Quota:
    def __init__(self, redis: Redis, *, name: str, rate: float, burst: int) -> None:
        self.name = name
        self.rate = rate
        self.burst = burst
        self._script = redis.register_script(TOKEN_BUCKET_LUA)

    async def charge(self, project_id: str, cost: int = 1) -> QuotaResult:
        if cost > self.burst:
            # Can never succeed, however long the client waits.
            return QuotaResult(False, float("inf"))
        try:
            allowed, _, retry_ms = await self._script(
                keys=[f"quota:{self.name}:{project_id}"], args=[self.burst, self.rate, cost]
            )
        except RedisError as exc:
            QUOTA_BACKEND_ERRORS.inc()
            logger.warning("quota_unavailable", quota=self.name, error=str(exc))
            return QuotaResult(True, 0.0)
        return QuotaResult(bool(allowed), retry_ms / 1000)


_quotas: dict[str, Quota] = {}


def configure(redis: Redis | None, settings: Settings) -> None:
    """Called from the app lifespan (and tests)."""
    _quotas.clear()
    if redis is None or not settings.quota_enabled:
        return
    _quotas["ingest"] = Quota(
        redis,
        name="ingest",
        rate=settings.ingest_quota_events_per_second,
        burst=settings.ingest_quota_burst,
    )
    _quotas["query"] = Quota(
        redis, name="query", rate=settings.query_quota_per_second, burst=settings.query_quota_burst
    )


def get(kind: str) -> Quota | None:
    return _quotas.get(kind)
