from typing import Any
from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import Bout
from elo_calculator.errors.app_exceptions import DataNotFoundException
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class BoutService(BaseService):
    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: UUID) -> Bout:
        bout = await uow.bouts.get_by_id(bout_id)
        if not bout:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return bout

    @with_uow
    async def get_by_bout_id(self, uow: UnitOfWork, bout_id: str) -> Bout | None:
        return await uow.bouts.get_by_bout_id(bout_id)

    @with_uow
    async def list(self, uow: UnitOfWork) -> list[Bout]:
        return await uow.bouts.get_all()

    @with_uow
    async def create(self, uow: UnitOfWork, bout: Bout) -> Bout:
        return await uow.bouts.add(bout)

    @with_uow
    async def update(self, uow: UnitOfWork, bout_id: UUID, data: dict[str, Any]) -> Bout:
        existing = await uow.bouts.get_by_id(bout_id)
        if not existing:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return await uow.bouts.update(bout_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, bout_id: UUID) -> Bout:
        existing = await uow.bouts.get_by_id(bout_id)
        if not existing:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return await uow.bouts.delete(bout_id)
