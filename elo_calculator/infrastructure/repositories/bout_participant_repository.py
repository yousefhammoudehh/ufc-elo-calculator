from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import BoutParticipant
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.database.schema import bout_participants
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class BoutParticipantRepository(BaseRepository[BoutParticipant]):
    """Repository for BoutParticipant entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(
            connection=connection, model_cls=BoutParticipant, table=bout_participants, cache_prefix='bout_participants'
        )

    async def get_by_bout_id(self, bout_id: str) -> list[BoutParticipant]:
        """Get all participants for a specific bout (usually 2 fighters)."""
        cmd = select(self.table).where(self.table.c.bout_id == bout_id)
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_by_fighter_id(self, fighter_id: str) -> list[BoutParticipant]:
        """Get all bout participations for a specific fighter."""
        cmd = select(self.table).where(self.table.c.fighter_id == fighter_id)
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_by_bout_and_fighter(self, bout_id: str, fighter_id: str) -> BoutParticipant | None:
        """Get a specific fighter's participation in a specific bout."""
        cmd = select(self.table).where(and_(self.table.c.bout_id == bout_id, self.table.c.fighter_id == fighter_id))
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_fighter_wins(self, fighter_id: str) -> list[BoutParticipant]:
        """Get all wins for a specific fighter."""
        cmd = select(self.table).where(
            and_(self.table.c.fighter_id == fighter_id, self.table.c.outcome == FightOutcome.WIN.value)
        )
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_fighter_losses(self, fighter_id: str) -> list[BoutParticipant]:
        """Get all losses for a specific fighter."""
        cmd = select(self.table).where(
            and_(self.table.c.fighter_id == fighter_id, self.table.c.outcome == FightOutcome.LOSS.value)
        )
        result = await self.connection.execute(cmd)
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def get_fighter_record(self, fighter_id: str) -> dict[str, int]:
        """Get a fighter's complete record (wins, losses, draws, no contests)."""
        all_bouts = await self.get_by_fighter_id(fighter_id)

        return {
            'wins': sum(1 for b in all_bouts if b.outcome == FightOutcome.WIN),
            'losses': sum(1 for b in all_bouts if b.outcome == FightOutcome.LOSS),
            'draws': sum(1 for b in all_bouts if b.outcome == FightOutcome.DRAW),
            'no_contests': sum(1 for b in all_bouts if b.outcome == FightOutcome.NO_CONTEST),
        }

    async def get_by_outcome(self, outcome: FightOutcome) -> list[BoutParticipant]:
        """Get all bout participants with a specific outcome."""
        return await self.get_all(filters={'outcome': outcome.value})
