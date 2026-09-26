import re
import time
import uuid

import structlog
from fastapi import Request, Response
from starlette.middleware.base import RequestResponseEndpoint

from app.api.errors import error_response
from app.core.config import get_settings
from app.core.metrics import HTTP_DURATION, HTTP_REQUESTS

REQUEST_ID_HEADER = "X-Request-ID"
_VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9\-_.]{1,128}$")
_UNLOGGED_PATHS = frozenset({"/health/live", "/health/ready", "/metrics"})

logger = structlog.get_logger()


async def request_context_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    """Request ID on every log line and response, plus one summary log per request."""
    incoming = request.headers.get(REQUEST_ID_HEADER, "")
    request_id = incoming if _VALID_REQUEST_ID.match(incoming) else uuid.uuid4().hex
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)

    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        _observe(request, 500, start)
        logger.exception("request_failed", method=request.method, path=request.url.path)
        raise

    _observe(request, response.status_code, start)
    response.headers[REQUEST_ID_HEADER] = request_id
    if request.url.path not in _UNLOGGED_PATHS:
        logger.info(
            "request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - start) * 1000, 2),
        )
    return response


def _observe(request: Request, status_code: int, start: float) -> None:
    # Route TEMPLATE (/v1/query/{kind}), never the raw path: bounded cardinality.
    template = getattr(request.scope.get("route"), "path", "unmatched")
    HTTP_REQUESTS.labels(request.method, template, str(status_code)).inc()
    HTTP_DURATION.labels(request.method, template).observe(time.perf_counter() - start)


async def body_size_limit_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    """Reject oversized request bodies BEFORE reading them.

    An ingestion endpoint is exposed to every client device; without a cap, one
    malicious or buggy client can make the server buffer arbitrarily large bodies
    in memory. Chunked uploads (no Content-Length) are refused, since we could
    only discover their size by reading them.
    """
    if request.method in ("POST", "PUT", "PATCH"):
        max_bytes = get_settings().max_request_bytes
        length = request.headers.get("content-length")
        if length is None:
            return error_response(411, "length_required", "Content-Length header is required")
        if not length.isdigit() or int(length) > max_bytes:
            return error_response(
                413, "payload_too_large", f"Request body exceeds {max_bytes} bytes"
            )
    return await call_next(request)
