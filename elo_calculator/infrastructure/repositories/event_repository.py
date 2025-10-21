from datetime import date
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import Event
from elo_calculator.infrastructure.database.schema import events
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class EventRepository(BaseRepository[Event]):
    """Repository for Event entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(connection=connection, model_cls=Event, table=events, cache_prefix='events')

    async def get_by_event_id(self, event_id: UUID) -> Event | None:
        """Get an event by event_id (UUID)."""
        cmd = select(self.table).where(self.table.c.event_id == event_id)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_by_date(self, event_date: date) -> list[Event]:
        """Get all events on a specific date."""
        return await self.get_all(filters={'event_date': event_date.isoformat()})

    async def get_events_by_date_range(self, start_date: date, end_date: date) -> list[Event]:
        """Get events within a date range."""
        return await self.get_all(
            filters={'event_date:>=': start_date.isoformat(), 'event_date:<=': end_date.isoformat()},
            sort_by='event_date',
            order='asc',
        )
