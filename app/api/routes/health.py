import asyncio

import structlog
from fastapi import APIRouter, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.core.db import SessionDep
from app.core.kafka import get_producer

router = APIRouter(prefix="/health", tags=["health"])
logger = structlog.get_logger()


@router.get("/live")
async def live() -> dict[str, str]:
    """The process is up. Checks no dependencies (a Kafka outage must not
    trigger restarts of healthy API containers)."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(session: SessionDep) -> dict[str, str]:
    """Can we accept events? Needs the metadata DB (write keys) and Kafka."""
    try:
        await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError) as exc:
        logger.warning("readiness_database_failed", error=str(exc))
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "database unavailable") from exc
    try:
        topic = get_settings().raw_events_topic
        partitions = await asyncio.wait_for(get_producer().partitions_for(topic), timeout=3)
        if not partitions:
            raise RuntimeError(f"topic {topic} has no partitions")
    except Exception as exc:
        logger.warning("readiness_kafka_failed", error=repr(exc))
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "event log unavailable") from exc
    return {"status": "ok", "database": "ok", "kafka": "ok"}
