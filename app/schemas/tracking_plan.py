from typing import Annotated, Literal

from pydantic import BaseModel, Field

from app.schemas.events import EventName

PropertyName = Annotated[str, Field(min_length=1, max_length=128)]


class PropertySpec(BaseModel):
    type: Literal["string", "number", "boolean", "object", "array"]
    required: bool = False
    # Allowed values (strings or numbers). None = any value of the right type.
    enum: list[str | float] | None = None


class EventSpec(BaseModel):
    properties: dict[PropertyName, PropertySpec] = Field(default_factory=dict)
    # False = properties not listed above are violations.
    additional_properties: bool = True


class TrackingPlanSpec(BaseModel):
    """What a project's events should look like.

    enforcement:
      warn  = accept violating events, but record the violations (queryable)
      block = reject violating events at ingestion (listed in `rejected`)
    """

    enforcement: Literal["warn", "block"] = "warn"
    allow_unplanned_events: bool = True
    events: dict[EventName, EventSpec] = Field(default_factory=dict, max_length=1_000)


class TrackingPlanUpdate(BaseModel):
    plan: TrackingPlanSpec
    # Changes that would make previously valid events fail are refused unless
    # the caller explicitly acknowledges them.
    allow_breaking_changes: bool = False


class TrackingPlanRead(BaseModel):
    version: int
    plan: TrackingPlanSpec


class TrackingPlanSaved(TrackingPlanRead):
    breaking_changes: list[str]


class ViolationCount(BaseModel):
    event: str
    violation: str
    count: int
