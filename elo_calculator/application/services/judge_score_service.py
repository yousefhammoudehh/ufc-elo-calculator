from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import JudgeScore
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class JudgeScoreService(BaseService):
    @with_uow
    async def list_by_bout(self, uow: UnitOfWork, bout_id: str) -> list[JudgeScore]:
        return await uow.judge_scores.get_by_bout_id(bout_id)

    @with_uow
    async def list_by_fighter(self, uow: UnitOfWork, fighter_id: str) -> list[JudgeScore]:
        return await uow.judge_scores.get_by_fighter_id(fighter_id)

    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: str, fighter_id: str) -> JudgeScore | None:
        return await uow.judge_scores.get_by_bout_and_fighter(bout_id, fighter_id)

    @with_uow
    async def total(self, uow: UnitOfWork, bout_id: str, fighter_id: str) -> int | None:
        return await uow.judge_scores.calculate_total_score(bout_id, fighter_id)
