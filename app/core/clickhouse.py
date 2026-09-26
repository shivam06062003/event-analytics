"""ClickHouse client and a minimal migration runner.

Alembic only speaks SQLAlchemy databases, so ClickHouse gets its own tiny
runner: numbered .sql files, applied in order, tracked in schema_migrations.
"""

import asyncio
from pathlib import Path

import structlog
from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient

from app.core.config import Settings

logger = structlog.get_logger()


async def create_client(settings: Settings, *, database: str | None = None) -> AsyncClient:
    return await get_async_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database if database is None else database,
    )


async def migrate(settings: Settings) -> list[str]:
    """Apply pending migrations. Returns the versions applied (empty if none)."""
    admin = await create_client(settings, database="default")
    await admin.command(f"CREATE DATABASE IF NOT EXISTS `{settings.clickhouse_database}`")
    await admin.close()

    client = await create_client(settings)
    try:
        await client.command(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version String, applied_at DateTime DEFAULT now()) "
            "ENGINE = MergeTree ORDER BY version"
        )
        applied = {
            row[0]
            for row in (await client.query("SELECT version FROM schema_migrations")).result_rows
        }
        newly_applied: list[str] = []
        migrations = await asyncio.to_thread(_load, settings.clickhouse_migrations_dir)
        for version, sql in migrations:
            if version in applied:
                continue
            for statement in _statements(sql):
                await client.command(statement)
            await client.insert("schema_migrations", [[version]], column_names=["version"])
            newly_applied.append(version)
            logger.info("clickhouse_migration_applied", version=version)
        return newly_applied
    finally:
        await client.close()


async def seed_demo(settings: Settings, project_id: str, users: int, start: str) -> int:
    """Generate demo events for a project (see clickhouse/seed/demo.sql).
    Returns the number of rows the project now has."""
    sql = await asyncio.to_thread(Path("clickhouse/seed/demo.sql").read_text)
    params = {"project": project_id, "users": users, "start": start}
    client = await create_client(settings)
    try:
        for statement in _statements(sql):
            await client.command(statement, parameters=params)
        result = await client.query(
            "SELECT count() FROM events WHERE project_id = {project:UUID}", parameters=params
        )
        return int(result.result_rows[0][0])
    finally:
        await client.close()


def _load(directory: str) -> list[tuple[str, str]]:
    return [(path.stem, path.read_text()) for path in sorted(Path(directory).glob("*.sql"))]


def _statements(sql: str) -> list[str]:
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
