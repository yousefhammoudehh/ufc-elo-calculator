"""Leaderboard service — serves current rankings from the serving table."""

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities.rating import CurrentRanking
from elo_calculator.infrastructure.repositories.ranking_repository import RankingRepository
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class LeaderboardService(BaseService):
    """Read-only service for current rankings by system and division."""

    @with_uow
    async def get_rankings(
        self, uow: UnitOfWork, *, system_key: str, division_key: str, sex: str = 'M', limit: int = 25, offset: int = 0
    ) -> tuple[list[CurrentRanking], int]:
        """Return a paginated slice of rankings plus total count."""
        repo = RankingRepository(uow.connection)
        rankings = await repo.list_rankings(system_key, division_key, sex, limit=limit, offset=offset)
        total = await repo.count_rankings(system_key, division_key, sex)
        return rankings, total
