from typing import Any
from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import Promotion
from elo_calculator.errors.app_exceptions import DataNotFoundException
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class PromotionService(BaseService):
    @with_uow
    async def get_all(self, uow: UnitOfWork, page: int = 1, limit: int = 100) -> list[Promotion]:
        rows, _ = await uow.promotions.get_paginated(page=page, limit=limit)
        return rows

    @with_uow
    async def get(self, uow: UnitOfWork, promotion_id: UUID) -> Promotion:
        promotion = await uow.promotions.get_by_id(promotion_id)
        if not promotion:
            raise DataNotFoundException(f'Promotion id:{promotion_id} not found')
        return promotion

    @with_uow
    async def create(self, uow: UnitOfWork, promotion: Promotion) -> Promotion:
        return await uow.promotions.add(promotion)

    @with_uow
    async def update(self, uow: UnitOfWork, promotion_id: UUID, data: dict[str, Any]) -> Promotion:
        existing = await uow.promotions.get_by_id(promotion_id)
        if not existing:
            raise DataNotFoundException(f'Promotion id:{promotion_id} not found')
        return await uow.promotions.update(promotion_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, promotion_id: UUID) -> Promotion:
        existing = await uow.promotions.get_by_id(promotion_id)
        if not existing:
            raise DataNotFoundException(f'Promotion id:{promotion_id} not found')
        return await uow.promotions.delete(promotion_id)

    @with_uow
    async def get_by_link(self, uow: UnitOfWork, link: str) -> Promotion | None:
        return await uow.promotions.get_by_link(link)
