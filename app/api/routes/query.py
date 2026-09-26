from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.api.auth import ReadKeyProject
from app.query import cache, queries
from app.schemas.queries import (
    EventNameCount,
    FunnelQuery,
    FunnelResult,
    RetentionQuery,
    RetentionResult,
    SegmentationQuery,
    SegmentationResult,
    SessionsQuery,
    SessionsResult,
)

router = APIRouter(prefix="/v1", tags=["queries"])


async def _cached(
    project_id: str, kind: str, query: BaseModel, range_end: datetime, compute: Any
) -> dict[str, Any]:
    async def run() -> dict[str, Any]:
        result: dict[str, Any] = await compute(project_id, query)
        result["computed_at"] = datetime.now(UTC).isoformat()
        return result

    result, hit = await cache.get_or_compute(
        cache.cache_key(project_id, kind, query), cache.ttl_for(range_end), run
    )
    # Never mutate `result`: coalesced requests all receive the SAME object.
    data = {key: value for key, value in result.items() if key != "computed_at"}
    return {**data, "meta": {"cached": hit, "computed_at": result["computed_at"]}}


@router.post("/query/segmentation")
async def segmentation(body: SegmentationQuery, project_id: ReadKeyProject) -> SegmentationResult:
    """Counts (or unique users) of one event over time, optionally broken down
    by a property and filtered by properties."""
    data = await _cached(str(project_id), "segmentation", body, body.to, queries.segmentation)
    return SegmentationResult.model_validate(data)


@router.post("/query/funnel")
async def funnel(body: FunnelQuery, project_id: ReadKeyProject) -> FunnelResult:
    """How many users completed each step, in order, within the window."""
    data = await _cached(str(project_id), "funnel", body, body.to, queries.funnel)
    return FunnelResult.model_validate(data)


@router.post("/query/retention")
async def retention(body: RetentionQuery, project_id: ReadKeyProject) -> RetentionResult:
    """Cohorts by first start_event; share of each cohort returning in later periods."""
    data = await _cached(str(project_id), "retention", body, body.to, queries.retention)
    return RetentionResult.model_validate(data)


@router.post("/query/sessions")
async def sessions(body: SessionsQuery, project_id: ReadKeyProject) -> SessionsResult:
    """Sessions per bucket (a new session after `inactivity_minutes` idle): count,
    unique users, average duration, bounce rate and events per session."""
    data = await _cached(str(project_id), "sessions", body, body.to, queries.sessions)
    return SessionsResult.model_validate(data)


@router.get("/event-names")
async def event_names(
    project_id: ReadKeyProject, days: Annotated[int, Query(ge=1, le=90)] = 30
) -> list[EventNameCount]:
    """Event names seen recently, most frequent first (for query builders)."""
    rows = await queries.event_names(str(project_id), days)
    return [EventNameCount.model_validate(r) for r in rows]
