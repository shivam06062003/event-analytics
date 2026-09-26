"""Tracking plans: per-project event schemas, enforced at ingestion.

Two pure functions do the real work and are unit-tested exhaustively:
  violations(event, plan)     -> what's wrong with one event
  breaking_changes(old, new)  -> which plan changes would fail events that
                                 used to be valid (schema compatibility)
"""

import time
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import SessionLocal
from app.models import TrackingPlan
from app.schemas.events import TrackEvent
from app.schemas.tracking_plan import PropertySpec, TrackingPlanSpec
from app.services.errors import BreakingChange

_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "number": lambda v: isinstance(v, int | float) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
}


def violations(event: TrackEvent, plan: TrackingPlanSpec) -> list[str]:
    spec = plan.events.get(event.event)
    if spec is None:
        return [] if plan.allow_unplanned_events else ["unplanned_event"]
    problems: list[str] = []
    for name, prop in spec.properties.items():
        if name not in event.properties:
            if prop.required:
                problems.append(f"missing_required:{name}")
            continue
        value = event.properties[name]
        if not _TYPE_CHECKS[prop.type](value):
            problems.append(f"wrong_type:{name}:expected_{prop.type}")
        elif prop.enum is not None and value not in prop.enum:
            problems.append(f"value_not_allowed:{name}")
    if not spec.additional_properties:
        problems += [
            f"unexpected_property:{n}" for n in event.properties if n not in spec.properties
        ]
    return problems


def breaking_changes(old: TrackingPlanSpec, new: TrackingPlanSpec) -> list[str]:
    """Changes after which some event that passed the OLD plan fails the NEW
    one. Same idea as BACKWARD compatibility in Avro/Protobuf schema
    registries. Non-breaking: new events, new optional properties, widened
    enums, relaxed requirements."""
    changes: list[str] = []
    if old.enforcement == "warn" and new.enforcement == "block":
        changes.append("enforcement:warn->block (violating events will now be dropped)")
    if old.allow_unplanned_events and not new.allow_unplanned_events:
        changes.append("unplanned_events:now_disallowed")
    for event_name, old_event in old.events.items():
        new_event = new.events.get(event_name)
        if new_event is None:
            if not new.allow_unplanned_events:
                changes.append(f"{event_name}:removed")
            continue
        if old_event.additional_properties and not new_event.additional_properties:
            changes.append(f"{event_name}:additional_properties_disallowed")
        for prop_name, new_prop in new_event.properties.items():
            old_prop = old_event.properties.get(prop_name)
            changes += _property_changes(event_name, prop_name, old_prop, new_prop)
    return changes


def _property_changes(
    event: str, name: str, old: PropertySpec | None, new: PropertySpec
) -> list[str]:
    where = f"{event}.{name}"
    if old is None:
        return [f"{where}:new_required_property"] if new.required else []
    changes = []
    if new.type != old.type:
        changes.append(f"{where}:type_changed:{old.type}->{new.type}")
    if new.required and not old.required:
        changes.append(f"{where}:now_required")
    if new.enum is not None:
        if old.enum is None:
            changes.append(f"{where}:enum_added")  # any value was fine; now only some are
        else:
            removed = set(old.enum) - set(new.enum)
            if removed:
                changes.append(f"{where}:enum_values_removed:{sorted(map(str, removed))}")
    return changes


# --- Storage (append-only versions) ------------------------------------------------


async def get_latest(session: AsyncSession, project_id: uuid.UUID) -> TrackingPlan | None:
    stmt = (
        select(TrackingPlan)
        .where(TrackingPlan.project_id == project_id)
        .order_by(TrackingPlan.version.desc())
        .limit(1)
    )
    return (await session.scalars(stmt)).first()


async def save(
    session: AsyncSession, project_id: uuid.UUID, plan: TrackingPlanSpec, allow_breaking: bool
) -> tuple[TrackingPlan, list[str]]:
    async with session.begin():
        current = await get_latest(session, project_id)
        changes = (
            breaking_changes(TrackingPlanSpec.model_validate(current.plan), plan) if current else []
        )
        if changes and not allow_breaking:
            raise BreakingChange(changes)
        next_version = (
            await session.scalar(
                select(func.coalesce(func.max(TrackingPlan.version), 0)).where(
                    TrackingPlan.project_id == project_id
                )
            )
            or 0
        ) + 1
        # (project_id, version) is UNIQUE: two concurrent saves can't both
        # become version N; the loser gets an integrity error and retries.
        row = TrackingPlan(
            project_id=project_id, version=next_version, plan=plan.model_dump(mode="json")
        )
        session.add(row)
    _cache.pop(project_id, None)
    return row, changes


# --- Hot-path lookup for ingestion (cached, like write keys) -----------------------

_CACHE_TTL_SECONDS = 30.0
_cache: dict[uuid.UUID, tuple[TrackingPlanSpec | None, float]] = {}


def clear_cache() -> None:
    _cache.clear()


async def active_plan(project_id: uuid.UUID) -> TrackingPlanSpec | None:
    """The plan ingestion enforces. Cached for 30s per API instance, so a plan
    change takes effect within 30s everywhere (immediately on the instance
    that saved it)."""
    cached = _cache.get(project_id)
    now = time.monotonic()
    if cached is not None and cached[1] > now:
        return cached[0]
    async with SessionLocal() as session:
        row = await get_latest(session, project_id)
    plan = TrackingPlanSpec.model_validate(row.plan) if row else None
    _cache[project_id] = (plan, now + _CACHE_TTL_SECONDS)
    return plan


def plan_payload(row: TrackingPlan) -> dict[str, Any]:
    return {"version": row.version, "plan": row.plan}
