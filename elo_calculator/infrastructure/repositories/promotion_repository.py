from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities import Promotion
from elo_calculator.infrastructure.database.schema import promotions
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class PromotionRepository(BaseRepository[Promotion]):
    """Repository for Promotion entities."""

    def __init__(self, connection: AsyncConnection):
        super().__init__(connection=connection, model_cls=Promotion, table=promotions, cache_prefix='promotions')

    async def get_by_name(self, name: str) -> Promotion | None:
        """Get a promotion by name."""
        promotions_list = await self.get_all(filters={'name': name})
        return promotions_list[0] if promotions_list else None

    async def get_by_strength_range(self, min_strength: float, max_strength: float) -> list[Promotion]:
        """Get promotions within a strength range."""
        return await self.get_all(filters={'strength:>=': min_strength, 'strength:<=': max_strength})
