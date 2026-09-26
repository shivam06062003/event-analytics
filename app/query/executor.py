"""Runs analytical queries against ClickHouse with guard rails.

Every query gets hard per-query limits. One user asking for a year of
unfiltered data must not starve every other tenant on the cluster.
"""

import time
from typing import Any

import structlog
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError

from app.core.config import get_settings
from app.core.metrics import QUERY_DURATION
from app.services.errors import QueryTimeout, QueryTooExpensive

logger = structlog.get_logger()

_client: AsyncClient | None = None


def set_client(client: AsyncClient | None) -> None:
    global _client
    _client = client


def get_client() -> AsyncClient:
    if _client is None:
        raise RuntimeError("ClickHouse client not initialised (done in the app lifespan)")
    return _client


async def run(sql: str, params: dict[str, Any], kind: str = "other") -> list[tuple[Any, ...]]:
    settings = get_settings()
    started = time.perf_counter()
    try:
        result = await get_client().query(
            sql,
            parameters=params,
            settings={
                "max_execution_time": settings.query_max_execution_seconds,
                "max_memory_usage": settings.query_max_memory_bytes,
                # readonly=2: no writes or DDL, and the query itself can't turn
                # readonly off (a SETTINGS clause can't lift it). Defence in depth
                # on top of parameter binding.
                "readonly": 2,
            },
        )
    except DatabaseError as exc:
        message = str(exc)
        if "TIMEOUT_EXCEEDED" in message:
            raise QueryTimeout(
                f"Query exceeded {settings.query_max_execution_seconds}s; narrow the time range "
                "or add filters"
            ) from exc
        if "MEMORY_LIMIT_EXCEEDED" in message:
            raise QueryTooExpensive(
                "Query needs too much memory; narrow the time range or reduce breakdowns"
            ) from exc
        raise
    finally:
        QUERY_DURATION.labels(kind).observe(time.perf_counter() - started)
    return [tuple(row) for row in result.result_rows]
