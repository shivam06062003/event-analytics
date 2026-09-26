from datetime import datetime, timedelta
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from app.core.config import get_settings
from app.schemas.events import EventName

PropertyName = Annotated[str, Field(min_length=1, max_length=128)]
Interval = Literal["hour", "day", "week"]
INTERVAL_SECONDS = {"hour": 3_600, "day": 86_400, "week": 604_800}


class PropertyFilter(BaseModel):
    property: PropertyName
    operator: Literal["eq", "neq", "contains", "gt", "gte", "lt", "lte", "is_set", "is_not_set"]
    value: str | float | bool | None = None

    @model_validator(mode="after")
    def _check(self) -> "PropertyFilter":
        if self.operator in ("is_set", "is_not_set"):
            return self
        if self.value is None:
            raise ValueError(f"operator '{self.operator}' needs a value")
        numeric = self.operator in ("gt", "gte", "lt", "lte")
        if numeric and (isinstance(self.value, bool) or not isinstance(self.value, int | float)):
            raise ValueError(f"operator '{self.operator}' needs a number")
        if self.operator == "contains" and not isinstance(self.value, str):
            raise ValueError("operator 'contains' needs a string")
        return self


Filters = Annotated[list[PropertyFilter], Field(max_length=10)]


class TimeRange(BaseModel):
    from_: AwareDatetime = Field(alias="from")
    to: AwareDatetime

    @model_validator(mode="after")
    def _check_range(self) -> "TimeRange":
        if self.to <= self.from_:
            raise ValueError("'to' must be after 'from'")
        max_days = get_settings().query_max_range_days
        if self.to - self.from_ > timedelta(days=max_days):
            raise ValueError(f"time range exceeds {max_days} days")
        return self


class SegmentationQuery(TimeRange):
    event: EventName
    interval: Interval = "day"
    measure: Literal["total", "unique_users"] = "total"
    breakdown: PropertyName | None = None
    filters: Filters = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_buckets(self) -> "SegmentationQuery":
        _check_bucket_count(self.from_, self.to, self.interval)
        return self


def _check_bucket_count(start: datetime, end: datetime, interval: str) -> None:
    buckets = (end - start).total_seconds() / INTERVAL_SECONDS[interval]
    limit = get_settings().query_max_buckets
    if buckets > limit:
        raise ValueError(f"{int(buckets)} {interval} buckets exceed the limit of {limit}")


class SessionsQuery(TimeRange):
    interval: Interval = "day"
    inactivity_minutes: Annotated[int, Field(ge=1, le=240)] = 30

    @model_validator(mode="after")
    def _check_buckets(self) -> "SessionsQuery":
        _check_bucket_count(self.from_, self.to, self.interval)
        return self


class FunnelStep(BaseModel):
    event: EventName
    filters: Filters = Field(default_factory=list)


class FunnelQuery(TimeRange):
    steps: Annotated[list[FunnelStep], Field(min_length=2, max_length=10)]
    # How long a user has, from the first step, to complete the rest.
    window_seconds: Annotated[int, Field(gt=0, le=90 * 86_400)] = 7 * 86_400


class RetentionQuery(TimeRange):
    start_event: EventName
    # None = any event counts as "came back".
    return_event: EventName | None = None
    period: Literal["day", "week"] = "week"
    periods: Annotated[int, Field(ge=1, le=52)] = 8


# --- Responses ---------------------------------------------------------------


class QueryMeta(BaseModel):
    cached: bool
    computed_at: datetime


class SeriesPoint(BaseModel):
    bucket: datetime
    value: int


class Series(BaseModel):
    segment: str | None
    total: int
    values: list[SeriesPoint]


class SegmentationResult(BaseModel):
    series: list[Series]
    meta: QueryMeta


class FunnelStepResult(BaseModel):
    event: str
    users: int
    conversion_from_start: float
    conversion_from_previous: float


class FunnelResult(BaseModel):
    steps: list[FunnelStepResult]
    window_seconds: int
    meta: QueryMeta


class Cohort(BaseModel):
    cohort: datetime
    size: int
    retained: list[int]
    rates: list[float]


class RetentionResult(BaseModel):
    period: str
    cohorts: list[Cohort]
    meta: QueryMeta


class EventNameCount(BaseModel):
    event: str
    count: int


class SessionsPoint(BaseModel):
    bucket: datetime
    sessions: int
    users: int
    avg_duration_seconds: float
    bounce_rate: float
    events_per_session: float


class SessionsTotals(BaseModel):
    sessions: int
    avg_duration_seconds: float
    bounce_rate: float
    events_per_session: float


class SessionsResult(BaseModel):
    values: list[SessionsPoint]
    totals: SessionsTotals
    meta: QueryMeta
