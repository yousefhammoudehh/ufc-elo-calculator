from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from elo_calculator.infrastructure.database.engine import engine


class UnitOfWork:
    def __init__(self, engine: AsyncEngine):
        self.engine: AsyncEngine = engine
        self.connection: AsyncConnection

    async def __aenter__(self) -> Self:
        self.connection = await self.engine.connect()
        await self.connection.begin()
        return self

    async def __aexit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None) -> None:
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
        await self.connection.close()

    # Add repository properties here, e.g. fighter_repo, bout_repo, etc. when implemented.

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()


@asynccontextmanager
async def get_uow() -> AsyncGenerator[UnitOfWork]:
    async with UnitOfWork(engine) as uow:
        yield uow
