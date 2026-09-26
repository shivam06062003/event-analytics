from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.errors import register_error_handlers
from app.api.middleware import body_size_limit_middleware, request_context_middleware
from app.api.routes import health, ingest
from app.core.config import get_settings
from app.core.db import engine
from app.core.kafka import start_producer, stop_producer
from app.core.logging import configure_logging

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await start_producer()
    logger.info("startup", environment=get_settings().environment)
    yield
    # stop() flushes buffered messages first, so requests still in flight
    # during shutdown get their acks instead of being cut off.
    await stop_producer()
    await engine.dispose()
    logger.info("shutdown")


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
    # Order matters: the LAST added middleware runs FIRST. Request context wraps
    # everything; the size limit rejects before any body is read.
    app.middleware("http")(body_size_limit_middleware)
    app.middleware("http")(request_context_middleware)
    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(ingest.router)
    return app


app = create_app()
