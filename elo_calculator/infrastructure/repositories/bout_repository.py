from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import Bout
from elo_calculator.infrastructure.database.schema import bouts
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class BoutRepository(BaseRepository[Bout]):
    """Repository for Bout entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(connection=connection, model_cls=Bout, table=bouts, cache_prefix='bouts')

    async def get_by_bout_id(self, bout_id: str) -> Bout | None:
        """Get a bout by bout_id (string)."""
        cmd = select(self.table).where(self.table.c.bout_id == bout_id)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_by_event(self, event_id: UUID) -> list[Bout]:
        """Get all bouts for a specific event."""
        return await self.get_all(filters={'event_id': str(event_id)})

    async def get_title_fights(self) -> list[Bout]:
        """Get all title fights."""
        return await self.get_all(filters={'is_title_fight': True})

    async def get_by_method(self, method: str) -> list[Bout]:
        """Get all bouts with a specific finish method (KO, TKO, Submission, Decision, etc.)."""
        return await self.get_all(filters={'method:ilike': f'%{method}%'})

    async def get_by_round(self, round_num: int) -> list[Bout]:
        """Get all bouts that ended in a specific round."""
        return await self.get_all(filters={'round_num': round_num})
