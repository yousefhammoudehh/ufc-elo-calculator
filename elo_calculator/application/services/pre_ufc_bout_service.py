from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import PreUfcBout
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class PreUfcBoutService(BaseService):
    @with_uow
    async def list_by_fighter(self, uow: UnitOfWork, fighter_id: str) -> list[PreUfcBout]:
        return await uow.pre_ufc_bouts.get_by_fighter_id(fighter_id)

    @with_uow
    async def list_by_promotion(self, uow: UnitOfWork, promotion_id: UUID) -> list[PreUfcBout]:
        return await uow.pre_ufc_bouts.get_by_promotion(promotion_id)

    @with_uow
    async def list_by_fighter_and_promotion(
        self, uow: UnitOfWork, fighter_id: str, promotion_id: UUID
    ) -> list[PreUfcBout]:
        return await uow.pre_ufc_bouts.get_by_fighter_and_promotion(fighter_id, promotion_id)

    @with_uow
    async def record(self, uow: UnitOfWork, fighter_id: str) -> dict[str, int]:
        return await uow.pre_ufc_bouts.get_fighter_pre_ufc_record(fighter_id)
