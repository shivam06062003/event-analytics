from typing import Annotated

from fastapi import APIRouter, Query

from app.api.auth import ManagerProject, ReadKeyProject
from app.core.db import SessionDep
from app.query import queries
from app.schemas.tracking_plan import (
    TrackingPlanRead,
    TrackingPlanSaved,
    TrackingPlanUpdate,
    ViolationCount,
)
from app.services import tracking_plans
from app.services.errors import NotFound

router = APIRouter(prefix="/v1/tracking-plan", tags=["tracking plan"])


@router.get("")
async def get_plan(project_id: ReadKeyProject, session: SessionDep) -> TrackingPlanRead:
    row = await tracking_plans.get_latest(session, project_id)
    if row is None:
        raise NotFound("This project has no tracking plan yet")
    return TrackingPlanRead.model_validate(tracking_plans.plan_payload(row))


@router.put("")
async def put_plan(
    body: TrackingPlanUpdate, project_id: ManagerProject, session: SessionDep
) -> TrackingPlanSaved:
    """Save a new version. Refused with 409 if it would reject events the current
    version accepts, unless allow_breaking_changes is true."""
    row, changes = await tracking_plans.save(
        session, project_id, body.plan, body.allow_breaking_changes
    )
    return TrackingPlanSaved(**tracking_plans.plan_payload(row), breaking_changes=changes)


@router.get("/violations")
async def violations(
    project_id: ReadKeyProject, days: Annotated[int, Query(ge=1, le=90)] = 7
) -> list[ViolationCount]:
    """Recorded violations (warn mode), most frequent first."""
    rows = await queries.violation_counts(str(project_id), days)
    return [ViolationCount.model_validate(r) for r in rows]
