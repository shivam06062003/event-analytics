"""Operator commands: `python -m app.cli <command>`."""

import argparse
import asyncio
import sys
import uuid

from app.core import clickhouse
from app.core.config import get_settings
from app.core.db import SessionLocal, engine
from app.core.kafka import ensure_topics
from app.services import projects as project_service


async def _create_project(name: str) -> None:
    async with SessionLocal() as session:
        created = await project_service.create_project(session, name)
    # Only the key goes to stdout so it can be captured: KEY=$(python -m app.cli ...)
    print(f"Created project {created.project_id} ({name})", file=sys.stderr)
    print(f"  read key: make read-key project={created.project_id}", file=sys.stderr)
    print(created.write_key)


async def _create_read_key(project_id: str, manage: bool) -> None:
    async with SessionLocal() as session:
        created = await project_service.create_read_key(
            session, uuid.UUID(project_id), can_manage=manage
        )
    print(f"Created read key for project {project_id}", file=sys.stderr)
    print(created.read_key)


async def _run(args: argparse.Namespace) -> None:
    try:
        if args.command == "create-project":
            await _create_project(args.name)
        elif args.command == "create-read-key":
            await _create_read_key(args.project, args.manage)
        elif args.command == "seed-demo":
            rows = await clickhouse.seed_demo(get_settings(), args.project, args.users, args.start)
            print(f"Project {args.project} now has {rows:,} events", file=sys.stderr)
        elif args.command == "ensure-topics":
            await ensure_topics(get_settings())
        elif args.command == "clickhouse-migrate":
            applied = await clickhouse.migrate(get_settings())
            print(f"Applied {len(applied)} ClickHouse migration(s): {applied}", file=sys.stderr)
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-project", help="Create a project and print its write key")
    create.add_argument("--name", required=True)
    read = commands.add_parser("create-read-key", help="Create a read (query) key for a project")
    read.add_argument("--project", required=True, help="Project ID")
    read.add_argument("--manage", action="store_true", help="Also allow changing the tracking plan")
    seed = commands.add_parser("seed-demo", help="Generate realistic demo events in ClickHouse")
    seed.add_argument("--project", required=True)
    seed.add_argument("--users", type=int, default=200_000)
    seed.add_argument("--start", default="2026-06-01 00:00:00.000", help="UTC, first signup day")
    commands.add_parser("ensure-topics", help="Create Kafka topics if missing (idempotent)")
    commands.add_parser("clickhouse-migrate", help="Apply pending ClickHouse migrations")
    asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    main()
