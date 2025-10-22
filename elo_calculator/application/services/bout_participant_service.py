from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import BoutParticipant
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class BoutParticipantService(BaseService):
    @with_uow
    async def list_by_bout(self, uow: UnitOfWork, bout_id: str) -> list[BoutParticipant]:
        return await uow.bout_participants.get_by_bout_id(bout_id)

    @with_uow
    async def list_by_fighter(self, uow: UnitOfWork, fighter_id: str) -> list[BoutParticipant]:
        return await uow.bout_participants.get_by_fighter_id(fighter_id)

    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: str, fighter_id: str) -> BoutParticipant | None:
        return await uow.bout_participants.get_by_bout_and_fighter(bout_id, fighter_id)

    @with_uow
    async def record(self, uow: UnitOfWork, fighter_id: str) -> dict[str, int]:
        return await uow.bout_participants.get_fighter_record(fighter_id)
