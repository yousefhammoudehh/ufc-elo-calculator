from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import PreUfcBout
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.database.schema import pre_ufc_bouts
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class PreUfcBoutRepository(BaseRepository[PreUfcBout]):
    """Repository for PreUfcBout entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(connection=connection, model_cls=PreUfcBout, table=pre_ufc_bouts, cache_prefix='pre_ufc_bouts')

    async def get_by_bout_id(self, bout_id: UUID) -> PreUfcBout | None:
        """Get a pre-UFC bout by bout_id (UUID)."""
        cmd = select(self.table).where(self.table.c.bout_id == bout_id)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_by_fighter_id(self, fighter_id: str) -> list[PreUfcBout]:
        """Get all pre-UFC bouts for a specific fighter."""
        cmd = select(self.table).where(self.table.c.fighter_id == fighter_id)
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_by_promotion(self, promotion_id: UUID) -> list[PreUfcBout]:
        """Get all pre-UFC bouts from a specific promotion."""
        cmd = select(self.table).where(self.table.c.promotion_id == promotion_id)
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_by_fighter_and_promotion(self, fighter_id: str, promotion_id: UUID) -> list[PreUfcBout]:
        """Get all pre-UFC bouts for a fighter in a specific promotion."""
        cmd = select(self.table).where(
            and_(self.table.c.fighter_id == fighter_id, self.table.c.promotion_id == promotion_id)
        )
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_fighter_pre_ufc_record(self, fighter_id: str) -> dict[str, int]:
        """Get a fighter's pre-UFC record (wins, losses, draws, no contests)."""
        all_bouts = await self.get_by_fighter_id(fighter_id)

        return {
            'wins': sum(1 for b in all_bouts if b.result == FightOutcome.WIN),
            'losses': sum(1 for b in all_bouts if b.result == FightOutcome.LOSS),
            'draws': sum(1 for b in all_bouts if b.result == FightOutcome.DRAW),
            'no_contests': sum(1 for b in all_bouts if b.result == FightOutcome.NO_CONTEST),
        }

    async def get_by_result(self, result: FightOutcome) -> list[PreUfcBout]:
        """Get all pre-UFC bouts with a specific result."""
        return await self.get_all(filters={'result': result.value})
