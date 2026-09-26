import uuid
from typing import Any

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from httpx import AsyncClient

from app.core.db import SessionLocal
from app.processor.processor import Processor
from app.schemas.events import TrackEvent
from app.schemas.tracking_plan import TrackingPlanSpec
from app.services import projects as project_service
from app.services import tracking_plans
from app.services.tracking_plans import breaking_changes, violations
from tests.conftest import ProjectFixture, Reader, TopicCollector, drain, event

PLAN: dict[str, Any] = {
    "enforcement": "warn",
    "allow_unplanned_events": True,
    "events": {
        "purchase": {
            "properties": {
                "plan": {"type": "string", "required": True, "enum": ["free", "pro"]},
                "value": {"type": "number"},
            },
            "additional_properties": False,
        }
    },
}


def spec(**overrides: Any) -> TrackingPlanSpec:
    return TrackingPlanSpec.model_validate({**PLAN, **overrides})


def track(name: str = "purchase", **properties: Any) -> TrackEvent:
    return TrackEvent(event_id=uuid.uuid4(), event=name, user_id="u", properties=properties)


# --- Validation rules (pure) ------------------------------------------------------------


@pytest.mark.parametrize(
    ("properties", "expected"),
    [
        ({"plan": "pro", "value": 49}, []),
        ({"value": 49}, ["missing_required:plan"]),
        ({"plan": "pro", "value": "49"}, ["wrong_type:value:expected_number"]),
        (
            {"plan": "pro", "value": True},
            ["wrong_type:value:expected_number"],
        ),  # bool isn't a number
        ({"plan": "enterprise"}, ["value_not_allowed:plan"]),
        ({"plan": "pro", "coupon": "X"}, ["unexpected_property:coupon"]),
    ],
)
def test_violations(properties: dict[str, Any], expected: list[str]) -> None:
    assert violations(track(**properties), spec()) == expected


def test_unplanned_events() -> None:
    assert violations(track("mystery"), spec()) == []
    assert violations(track("mystery"), spec(allow_unplanned_events=False)) == ["unplanned_event"]


# --- Compatibility (pure) ------------------------------------------------------------------


def plan_with(**purchase_props: Any) -> TrackingPlanSpec:
    return spec(events={"purchase": {"properties": purchase_props}})


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        # Non-breaking: loosening, or adding optional things.
        (plan_with(), plan_with(coupon={"type": "string"}), []),
        (
            plan_with(plan={"type": "string", "enum": ["free"]}),
            plan_with(plan={"type": "string", "enum": ["free", "pro"]}),
            [],
        ),
        (
            plan_with(plan={"type": "string", "required": True}),
            plan_with(plan={"type": "string"}),
            [],
        ),
        # Breaking: anything that makes a previously valid event fail.
        (
            plan_with(),
            plan_with(coupon={"type": "string", "required": True}),
            ["purchase.coupon:new_required_property"],
        ),
        (
            plan_with(v={"type": "number"}),
            plan_with(v={"type": "string"}),
            ["purchase.v:type_changed:number->string"],
        ),
        (
            plan_with(v={"type": "number"}),
            plan_with(v={"type": "number", "required": True}),
            ["purchase.v:now_required"],
        ),
        (
            plan_with(plan={"type": "string", "enum": ["free", "pro"]}),
            plan_with(plan={"type": "string", "enum": ["pro"]}),
            ["purchase.plan:enum_values_removed:['free']"],
        ),
        (
            plan_with(plan={"type": "string"}),
            plan_with(plan={"type": "string", "enum": ["pro"]}),
            ["purchase.plan:enum_added"],
        ),
    ],
)  # fmt: skip
def test_breaking_changes(
    old: TrackingPlanSpec, new: TrackingPlanSpec, expected: list[str]
) -> None:
    assert breaking_changes(old, new) == expected


def test_tightening_enforcement_and_unplanned_events_is_breaking() -> None:
    changes = breaking_changes(spec(), spec(enforcement="block", allow_unplanned_events=False))
    assert changes == [
        "enforcement:warn->block (violating events will now be dropped)",
        "unplanned_events:now_disallowed",
    ]


# --- API ---------------------------------------------------------------------------------


@pytest.fixture
async def manager(reader: Reader) -> Reader:
    async with SessionLocal() as session:
        created = await project_service.create_read_key(
            session, uuid.UUID(reader.project.id), can_manage=True
        )
    return Reader(reader.project, created.read_key, created.read_key_id)


async def put_plan(client: AsyncClient, key: Reader, plan: dict[str, Any], **extra: Any) -> Any:
    return await client.put("/v1/tracking-plan", json={"plan": plan, **extra}, headers=key.headers)


async def test_plan_versions_and_breaking_change_protection(
    client: AsyncClient, manager: Reader
) -> None:
    first = await put_plan(client, manager, PLAN)
    assert (first.status_code, first.json()["version"]) == (200, 1)

    widened = {**PLAN, "events": {**PLAN["events"], "signup": {}}}
    second = await put_plan(client, manager, widened)
    assert (second.json()["version"], second.json()["breaking_changes"]) == (2, [])

    stricter = {**widened, "enforcement": "block"}
    refused = await put_plan(client, manager, stricter)
    assert refused.status_code == 409
    assert refused.json()["error"]["code"] == "breaking_change"

    forced = await put_plan(client, manager, stricter, allow_breaking_changes=True)
    assert forced.json()["version"] == 3
    assert forced.json()["breaking_changes"] == [
        "enforcement:warn->block (violating events will now be dropped)"
    ]
    current = await client.get("/v1/tracking-plan", headers=manager.headers)
    assert (current.json()["version"], current.json()["plan"]["enforcement"]) == (3, "block")


async def test_ordinary_read_keys_cannot_change_the_plan(
    client: AsyncClient, reader: Reader
) -> None:
    response = await put_plan(client, reader, PLAN)

    assert response.status_code == 403


async def test_warn_mode_records_violations_end_to_end(
    client: AsyncClient,
    manager: Reader,
    kafka: TopicCollector,
    processor: Processor,
    ch: ClickHouseClient,
) -> None:
    await put_plan(client, manager, PLAN)
    project: ProjectFixture = manager.project
    batch = [
        event(event="purchase", properties={"plan": "pro", "value": 10}),
        event(event="purchase", properties={"plan": "gold"}),
        event(event="purchase", properties={"value": "cheap"}),
    ]

    response = await client.post("/v1/batch", json={"batch": batch}, headers=project.headers)

    assert response.json() == {"accepted": 3, "rejected": []}  # warn: nothing dropped
    messages = await kafka.wait_for(project.id, 3)
    assert sorted(len(m.value["violations"]) for m in messages) == [0, 1, 2]

    await drain(processor)
    counts = await client.get("/v1/tracking-plan/violations", headers=manager.headers)
    # The seeded events are "now", so they fall in the default 7-day window.
    assert {(c["violation"], c["count"]) for c in counts.json()} == {
        ("value_not_allowed:plan", 1),
        ("missing_required:plan", 1),
        ("wrong_type:value:expected_number", 1),
    }


async def test_block_mode_rejects_violating_events_at_ingestion(
    client: AsyncClient, manager: Reader, kafka: TopicCollector
) -> None:
    await put_plan(client, manager, {**PLAN, "enforcement": "block"})
    project = manager.project

    response = await client.post(
        "/v1/batch",
        json={
            "batch": [
                event(event="purchase", properties={"plan": "pro"}),
                event(event="purchase", properties={}),
            ]
        },
        headers=project.headers,
    )

    body = response.json()
    assert body["accepted"] == 1
    assert body["rejected"][0]["index"] == 1
    assert body["rejected"][0]["errors"] == ["tracking_plan: missing_required:plan"]
    assert len(await kafka.wait_for(project.id, 1)) == 1


async def test_plan_changes_reach_ingestion_after_the_cache(
    client: AsyncClient, manager: Reader
) -> None:
    project = manager.project
    # Warm the ingestion cache with "no plan".
    await client.post(
        "/v1/batch", json={"batch": [event(event="purchase")]}, headers=project.headers
    )

    await put_plan(client, manager, {**PLAN, "enforcement": "block"})
    # save() evicts the local cache entry, so this instance applies it at once
    # (other instances within 30s).
    response = await client.post(
        "/v1/batch", json={"batch": [event(event="purchase")]}, headers=project.headers
    )

    assert response.status_code == 400  # the only event was blocked
    assert tracking_plans._cache  # repopulated
