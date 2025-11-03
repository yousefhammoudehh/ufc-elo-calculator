from typing import Any
from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import Fighter
from elo_calculator.errors.app_exceptions import DataNotFoundException
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class FighterService(BaseService):
    @with_uow
    async def get_all(self, uow: UnitOfWork) -> list[Fighter]:
        return await uow.fighters.get_all()

    @with_uow
    async def get(self, uow: UnitOfWork, fighter_id: UUID) -> Fighter:
        fighter = await uow.fighters.get_by_id(fighter_id)
        if not fighter:
            raise DataNotFoundException(f'Fighter id:{fighter_id} not found')
        return fighter

    @with_uow
    async def get_by_fighter_id(self, uow: UnitOfWork, fighter_id: str) -> Fighter | None:
        return await uow.fighters.get_by_fighter_id(fighter_id)

    @with_uow
    async def create(self, uow: UnitOfWork, fighter: Fighter) -> Fighter:
        return await uow.fighters.add(fighter)

    @with_uow
    async def update(self, uow: UnitOfWork, fighter_id: UUID, data: dict[str, Any]) -> Fighter:
        existing = await uow.fighters.get_by_id(fighter_id)
        if not existing:
            raise DataNotFoundException(f'Fighter id:{fighter_id} not found')
        return await uow.fighters.update(fighter_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, fighter_id: UUID) -> Fighter:
        existing = await uow.fighters.get_by_id(fighter_id)
        if not existing:
            raise DataNotFoundException(f'Fighter id:{fighter_id} not found')
        return await uow.fighters.delete(fighter_id)

    @with_uow
    async def get_by_stats_link(self, uow: UnitOfWork, stats_link: str) -> Fighter | None:
        return await uow.fighters.get_by_stats_link(stats_link)

    @with_uow
    async def get_by_tapology_link(self, uow: UnitOfWork, tapology_link: str) -> Fighter | None:
        return await uow.fighters.get_by_tapology_link(tapology_link)

    @with_uow
    async def search_by_name(self, uow: UnitOfWork, q: str, limit: int = 20) -> list[Fighter]:
        return await uow.fighters.search_by_name(q, limit)

    @with_uow
    async def search_by_name_paginated(
        self, uow: UnitOfWork, q: str, page: int, limit: int, sort_by: str, order: str
    ) -> tuple[list[Fighter], int]:
        return await uow.fighters.search_by_name_paginated(q, page, limit, sort_by, order)
