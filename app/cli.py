"""Operator commands: `python -m app.cli <command>`."""

import argparse
import asyncio
import sys

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
    print(created.write_key)


async def _run(args: argparse.Namespace) -> None:
    try:
        if args.command == "create-project":
            await _create_project(args.name)
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
    commands.add_parser("ensure-topics", help="Create Kafka topics if missing (idempotent)")
    commands.add_parser("clickhouse-migrate", help="Apply pending ClickHouse migrations")
    asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    main()
