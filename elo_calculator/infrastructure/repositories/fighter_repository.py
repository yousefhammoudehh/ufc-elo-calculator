from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import Fighter
from elo_calculator.infrastructure.database.schema import fighters
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class FighterRepository(BaseRepository[Fighter]):
    """Repository for Fighter entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(connection=connection, model_cls=Fighter, table=fighters, cache_prefix='fighters')

    async def get_by_fighter_id(self, fighter_id: str) -> Fighter | None:
        """Get a fighter by their fighter_id."""
        cmd = select(self.table).where(self.table.c.fighter_id == fighter_id)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_by_name(self, name: str) -> Fighter | None:
        """Get a fighter by name."""
        fighters_list = await self.get_all(filters={'name': name})
        return fighters_list[0] if fighters_list else None

    async def get_by_stats_link(self, stats_link: str) -> Fighter | None:
        """Get a fighter by their stats_link."""
        cmd = select(self.table).where(self.table.c.stats_link == stats_link)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_by_tapology_link(self, tapology_link: str) -> Fighter | None:
        """Get a fighter by their tapology_link."""
        cmd = select(self.table).where(self.table.c.tapology_link == tapology_link)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self._map_row_to_model(row._asdict()) if row else None

    async def get_top_fighters_by_elo(self, limit: int = 10) -> list[Fighter]:
        """Get the top fighters by current ELO rating."""
        all_fighters = await self.get_all(sort_by='current_elo', order='desc')
        return all_fighters[:limit]

    async def search_by_name(self, q: str, limit: int = 20) -> list[Fighter]:
        """Search fighters by name substring (case-insensitive)."""
        results = await self.get_all(filters={'name:ilike': f'%{q}%'}, sort_by='name', order='asc')
        return results[:limit]

    async def get_top_fighters_by_peak_elo(self, limit: int = 10) -> list[Fighter]:
        """Get the top fighters by peak ELO rating."""
        all_fighters = await self.get_all(sort_by='peak_elo', order='desc')
        return all_fighters[:limit]

    async def search_by_name_paginated(
        self, q: str, page: int, limit: int, sort_by: str = 'name', order: str = 'asc'
    ) -> tuple[list[Fighter], int]:
        filters = {'name:ilike': f'%{q}%'} if q else None
        rows, total = await self.get_paginated_with_filters(page, limit, filters, sort_by, order)
        return rows, total

    async def get_fighters_by_elo_range(self, min_elo: float, max_elo: float) -> list[Fighter]:
        """Get fighters within an ELO range."""
        return await self.get_all(filters={'current_elo:>=': min_elo, 'current_elo:<=': max_elo})

    async def update_fighter_elo(self, fighter_id: str, new_elo: float) -> Fighter:
        """Update a fighter's current ELO and peak ELO if applicable."""
        fighter = await self.get_by_fighter_id(fighter_id)
        if not fighter:
            raise ValueError(f'Fighter with ID {fighter_id} not found')
        update_data = {'current_elo': new_elo}

        # Update peak ELO if new ELO is higher
        if fighter.peak_elo is None or new_elo > fighter.peak_elo:
            update_data['peak_elo'] = new_elo

        # Note: BaseRepository expects UUID for entity_id, but our fighters use string IDs
        # We'll need to update directly using the fighter_id
        cmd = (
            update(self.table)
            .where(self.table.c.fighter_id == fighter_id)
            .values(**update_data)
            .returning(*self.table.columns)
        )
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise ValueError(f'Failed to update fighter {fighter_id}')

        await self.cache.flush_db()
        return self._map_row_to_model(row._asdict())
