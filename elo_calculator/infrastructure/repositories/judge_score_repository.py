from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import JudgeScore
from elo_calculator.infrastructure.database.schema import judge_scores
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class JudgeScoreRepository(BaseRepository[JudgeScore]):
    """Repository for JudgeScore entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(connection=connection, model_cls=JudgeScore, table=judge_scores, cache_prefix='judge_scores')

    async def get_by_bout_id(self, bout_id: str) -> list[JudgeScore]:
        """Get all judge scores for a specific bout (one record per fighter)."""
        cmd = select(self.table).where(self.table.c.bout_id == bout_id)
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_by_fighter_id(self, fighter_id: str) -> list[JudgeScore]:
        """Get all judge scores for a specific fighter across all bouts."""
        cmd = select(self.table).where(self.table.c.fighter_id == fighter_id)
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_by_bout_and_fighter(self, bout_id: str, fighter_id: str) -> JudgeScore | None:
        """Get judge scores for a specific fighter in a specific bout."""
        cmd = select(self.table).where(and_(self.table.c.bout_id == bout_id, self.table.c.fighter_id == fighter_id))
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def calculate_total_score(self, bout_id: str, fighter_id: str) -> int | None:
        """Calculate the total score for a fighter from all three judges."""
        scores = await self.get_by_bout_and_fighter(bout_id, fighter_id)
        if not scores:
            return None

        total = 0
        if scores.judge1_score:
            total += scores.judge1_score
        if scores.judge2_score:
            total += scores.judge2_score
        if scores.judge3_score:
            total += scores.judge3_score

        return total
