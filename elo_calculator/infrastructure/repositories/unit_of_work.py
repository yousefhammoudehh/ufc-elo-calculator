from collections.abc import Awaitable, Callable
from typing import Concatenate, ParamSpec, Self, TypeVar

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from elo_calculator.application.base_service import BaseService
from elo_calculator.infrastructure.database.engine import engine


class UnitOfWork:
    def __init__(self, engine: AsyncEngine):
        self.engine: AsyncEngine = engine
        self.connection: AsyncConnection
        self.rollback_only: bool = False

    async def __aenter__(self) -> Self:
        self.connection = await self.engine.connect()
        await self.connection.begin()
        return self

    async def __aexit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None) -> None:
        if exc_type or self.rollback_only:
            await self.rollback()
        else:
            await self.commit()
        await self.connection.close()

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()


T = TypeVar('T', bound=BaseService)
P = ParamSpec('P')
R = TypeVar('R')


def with_uow(  # noqa: UP047 - keep ParamSpec/Concatenate for mypy/py311 compatibility instead of PEP 695 type params
    func: Callable[Concatenate[T, UnitOfWork, P], Awaitable[R]],
) -> Callable[Concatenate[T, P], Awaitable[R]]:
    """Provide a UnitOfWork instance to service methods transparently.

    Methods should be declared as: async def method(self, uow: UnitOfWork, *args) -> R
    Callers invoke: await service.method(*args)  # uow injected by decorator
    """

    async def wrapper(self: T, /, *args: P.args, **kwargs: P.kwargs) -> R:
        async with UnitOfWork(engine) as uow:
            return await func(self, uow, *args, **kwargs)

    return wrapper
