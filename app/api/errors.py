"""One error shape everywhere:  {"error": {"code", "message", "details"?}}"""

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.services.errors import (
    BatchTooLarge,
    DomainError,
    IngestUnavailable,
    NoValidEvents,
    Unauthenticated,
)

_STATUS_BY_ERROR: dict[type[DomainError], int] = {
    Unauthenticated: status.HTTP_401_UNAUTHORIZED,
    BatchTooLarge: status.HTTP_422_UNPROCESSABLE_CONTENT,
    NoValidEvents: status.HTTP_400_BAD_REQUEST,
    IngestUnavailable: status.HTTP_503_SERVICE_UNAVAILABLE,
}
INGEST_RETRY_AFTER_SECONDS = 5


def error_response(
    status_code: int,
    code: str,
    message: str,
    details: Any = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        body["details"] = details
    return JSONResponse({"error": body}, status_code=status_code, headers=headers)


async def _domain_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, DomainError)
    headers: dict[str, str] | None = None
    if isinstance(exc, Unauthenticated):
        headers = {"WWW-Authenticate": "Bearer"}
    elif isinstance(exc, IngestUnavailable):
        headers = {"Retry-After": str(INGEST_RETRY_AFTER_SECONDS)}
    return error_response(
        _STATUS_BY_ERROR.get(type(exc), status.HTTP_400_BAD_REQUEST),
        exc.code,
        exc.message,
        details=exc.details,
        headers=headers,
    )


async def _validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "validation_error",
        "Request validation failed",
        details=jsonable_encoder(exc.errors()),
    )


async def _http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    code = HTTPStatus(exc.status_code).phrase.lower().replace(" ", "_")
    return error_response(exc.status_code, code, str(exc.detail), headers=exc.headers)


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, _domain_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
