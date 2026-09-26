import uuid
from typing import Annotated, Any

import orjson
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

from app.core.config import get_settings

EventName = Annotated[str, Field(min_length=1, max_length=128, examples=["checkout_started"])]
DistinctId = Annotated[str, Field(min_length=1, max_length=255)]


class TrackEvent(BaseModel):
    """One analytics event as sent by a client SDK."""

    model_config = ConfigDict(extra="forbid")

    # Client-generated and stable across retries: the SDK resends a failed
    # batch with the SAME ids, and the pipeline de-duplicates on them. This is
    # what turns at-least-once delivery into effectively-once counting.
    event_id: uuid.UUID
    event: EventName
    user_id: DistinctId | None = None
    anonymous_id: DistinctId | None = None
    # When it happened, by the CLIENT's clock (which may be wrong).
    timestamp: AwareDatetime | None = None
    properties: dict[str, JsonValue] = Field(default_factory=dict)
    context: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> "TrackEvent":
        if self.user_id is None and self.anonymous_id is None:
            raise ValueError("one of user_id or anonymous_id is required")
        limit = get_settings().max_properties_bytes
        if len(orjson.dumps(self.properties)) > limit:
            raise ValueError(f"properties exceed {limit} bytes")
        return self

    @property
    def distinct_id(self) -> str:
        """The identity events are grouped by: the known user if identified."""
        return self.user_id or self.anonymous_id or ""


class BatchRequest(BaseModel):
    """Events are validated individually (see the ingest service), so one bad
    event doesn't cost the client the whole batch."""

    batch: list[dict[str, Any]] = Field(min_length=1)
    # When the client SENT the batch, by its own clock: used to correct skew.
    sent_at: AwareDatetime | None = None


class RejectedEvent(BaseModel):
    index: int
    event_id: str | None
    errors: list[str]


class BatchResponse(BaseModel):
    accepted: int
    rejected: list[RejectedEvent]
