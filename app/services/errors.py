"""Business-rule errors, HTTP-agnostic. The API layer maps them to status codes."""

from typing import Any


class DomainError(Exception):
    code = "domain_error"

    def __init__(self, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class Unauthenticated(DomainError):
    code = "unauthenticated"


class BatchTooLarge(DomainError):
    code = "batch_too_large"

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"Batch has {size} events; the limit is {limit}")


class NoValidEvents(DomainError):
    code = "no_valid_events"

    def __init__(self, rejected: list[dict[str, Any]]) -> None:
        super().__init__("Every event in the batch failed validation", details=rejected)


class IngestUnavailable(DomainError):
    """The event log can't durably accept writes right now. Clients should
    retry the same batch (same event_ids) after Retry-After."""

    code = "ingest_unavailable"


class QueryTimeout(DomainError):
    code = "query_timeout"


class QueryTooExpensive(DomainError):
    code = "query_too_expensive"


class Forbidden(DomainError):
    code = "forbidden"


class NotFound(DomainError):
    code = "not_found"


class BreakingChange(DomainError):
    code = "breaking_change"

    def __init__(self, changes: list[str]) -> None:
        super().__init__(
            "The new plan would reject events the current plan accepts. "
            "Resend with allow_breaking_changes=true to apply it anyway.",
            details=changes,
        )
