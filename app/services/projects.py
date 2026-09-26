import hashlib
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.db import SessionLocal
from app.models import Project, ReadKey, WriteKey
from app.repositories import write_keys as write_keys_repo
from app.services.errors import Unauthenticated

WRITE_KEY_PREFIX = "wk_"
READ_KEY_PREFIX = "rk_"


@dataclass(frozen=True)
class CreatedProject:
    project_id: uuid.UUID
    write_key_id: uuid.UUID
    write_key: str  # plaintext, shown once


def hash_key(plaintext: str) -> str:
    # SHA-256, not bcrypt: keys are 192 random bits (not guessable passwords)
    # and are checked on every ingestion request, so a fast hash is correct.
    return hashlib.sha256(plaintext.encode()).hexdigest()


async def create_project(session: AsyncSession, name: str) -> CreatedProject:
    plaintext = WRITE_KEY_PREFIX + secrets.token_urlsafe(24)
    project = Project(name=name)
    async with session.begin():
        session.add(project)
        await session.flush()
        key = WriteKey(project_id=project.id, prefix=plaintext[:11], key_hash=hash_key(plaintext))
        session.add(key)
    return CreatedProject(project.id, key.id, plaintext)


@dataclass(frozen=True)
class CreatedReadKey:
    read_key_id: uuid.UUID
    read_key: str  # plaintext, shown once


async def create_read_key(session: AsyncSession, project_id: uuid.UUID) -> CreatedReadKey:
    plaintext = READ_KEY_PREFIX + secrets.token_urlsafe(24)
    key = ReadKey(project_id=project_id, prefix=plaintext[:11], key_hash=hash_key(plaintext))
    async with session.begin():
        session.add(key)
    return CreatedReadKey(key.id, plaintext)


async def revoke_read_key(session: AsyncSession, read_key_id: uuid.UUID) -> None:
    async with session.begin():
        await session.execute(
            update(ReadKey).where(ReadKey.id == read_key_id).values(revoked_at=datetime.now(UTC))
        )


async def authenticate_read_key(plaintext: str) -> uuid.UUID:
    """No cache here, unlike write keys: a leaked read key exposes data, so
    revocation must be instant. One indexed lookup is negligible next to the
    analytical query that follows it."""
    async with SessionLocal() as session:
        key = await write_keys_repo.get_active_read_key(session, hash_key(plaintext))
    if key is None:
        raise Unauthenticated("Invalid or revoked read key")
    return key.project_id


async def revoke_write_key(session: AsyncSession, write_key_id: uuid.UUID) -> None:
    async with session.begin():
        await session.execute(
            update(WriteKey).where(WriteKey.id == write_key_id).values(revoked_at=datetime.now(UTC))
        )


# --- Write-key authentication with an in-process cache ------------------------
# The ingestion endpoint is the hottest path in the system: every batch from
# every device hits it. Caching the key -> project lookup removes a database
# round trip per request. The cost: a revoked key keeps working for up to the
# TTL on each API instance. Acceptable for write keys (they can only append
# events, which a project can later delete); it would NOT be for read keys.

_MAX_CACHE_ENTRIES = 10_000
_cache: dict[str, tuple[uuid.UUID, float]] = {}


def clear_write_key_cache() -> None:
    _cache.clear()


async def authenticate_write_key(plaintext: str) -> uuid.UUID:
    """Return the project a write key belongs to, or raise Unauthenticated."""
    key_hash = hash_key(plaintext)
    cached = _cache.get(key_hash)
    now = time.monotonic()
    if cached is not None and cached[1] > now:
        return cached[0]

    async with SessionLocal() as session:
        key = await write_keys_repo.get_active_by_hash(session, key_hash)
    if key is None:
        raise Unauthenticated("Invalid or revoked write key")

    if len(_cache) >= _MAX_CACHE_ENTRIES:
        _cache.clear()  # crude but bounded; a real LRU arrives if profiling says so
    _cache[key_hash] = (key.project_id, now + get_settings().write_key_cache_ttl_seconds)
    return key.project_id
