from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from redis.asyncio import Redis

from app.api.errors import register_error_handlers
from app.api.middleware import body_size_limit_middleware, request_context_middleware
from app.api.routes import health, ingest, query
from app.core import clickhouse
from app.core.config import get_settings
from app.core.db import engine
from app.core.kafka import start_producer, stop_producer
from app.core.logging import configure_logging
from app.query import cache, executor

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    await start_producer()
    ch_client = await clickhouse.create_client(settings)
    executor.set_client(ch_client)
    redis = Redis.from_url(settings.redis_url, socket_timeout=0.5, socket_connect_timeout=0.5)
    cache.set_redis(redis)
    logger.info("startup", environment=settings.environment)
    yield
    await ch_client.close()
    await redis.aclose()
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
    app.include_router(query.router)
    return app


app = create_app()
