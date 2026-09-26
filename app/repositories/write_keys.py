from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import WriteKey


async def get_active_by_hash(session: AsyncSession, key_hash: str) -> WriteKey | None:
    stmt = select(WriteKey).where(WriteKey.key_hash == key_hash, WriteKey.revoked_at.is_(None))
    return (await session.scalars(stmt)).one_or_none()
