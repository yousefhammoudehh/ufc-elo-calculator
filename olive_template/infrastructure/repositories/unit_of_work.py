from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Self

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from olive_template.infrastructure.database.engine import engine
from olive_template.infrastructure.repositories.client_repository import ClientRepository


class UnitOfWork:
    def __init__(self, engine: AsyncEngine):
        self.engine: AsyncEngine = engine
        self.connection: AsyncConnection

    async def __aenter__(self) -> Self:
        self.connection = await self.engine.connect()
        await self.connection.begin()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[object]) -> None:

        if exc_type:
            await self.rollback()
        else:
            await self.commit()
        await self.connection.close()

    @property
    def client_repo(self) -> ClientRepository:
        if not hasattr(self, '_client_repo'):
            self._client_repo = ClientRepository(self.connection)
        return self._client_repo

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()


@asynccontextmanager
async def get_uow() -> AsyncGenerator[UnitOfWork, None]:
    async with UnitOfWork(engine) as uow:
        yield uow
