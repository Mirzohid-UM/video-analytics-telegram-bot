from __future__ import annotations

from typing import Optional, Any, Sequence

from psycopg_pool import AsyncConnectionPool


class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.pool: Optional[AsyncConnectionPool] = None

    async def connect(self) -> None:
        # open=False -> explicit open
        self.pool = AsyncConnectionPool(conninfo=self._dsn, open=False, max_size=10)
        await self.pool.open()

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def fetchval(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params or ())
                row = await cur.fetchone()
                return row[0] if row else None

    async def executemany(self, sql: str, rows: list[tuple]) -> None:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.executemany(sql, rows)
