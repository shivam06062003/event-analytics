from typing import Protocol

from clickhouse_connect.driver.asyncclient import AsyncClient

from app.processor.transform import COLUMNS, EventRow


class Sink(Protocol):
    async def insert(self, rows: list[EventRow]) -> None: ...


class ClickHouseSink:
    """One INSERT per batch. ClickHouse is built for few large inserts: every
    insert creates a new data part on disk that merges later, so thousands of
    tiny inserts per second would overwhelm it ("too many parts")."""

    def __init__(self, client: AsyncClient, table: str = "events") -> None:
        self.client = client
        self.table = table

    async def insert(self, rows: list[EventRow]) -> None:
        await self.client.insert(
            self.table, [row.values() for row in rows], column_names=list(COLUMNS)
        )
