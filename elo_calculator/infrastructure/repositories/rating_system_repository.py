"""Repository for ``dim_rating_system``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.division import RatingSystem
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class RatingSystemRepository(BaseRepository['RatingSystem']):
    """Read-access to rating system definitions."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[RatingSystem]:
        _ = (limit, offset)  # small table, always return all
        result = await self._conn.execute(
            text("""
                SELECT system_id::text, system_key, description
                FROM dim_rating_system
                ORDER BY system_key
            """)
        )
        return [
            RatingSystem(
                id=r['system_id'],
                system_id=r['system_id'],
                system_key=r['system_key'],
                description=r.get('description'),
            )
            for r in result.mappings()
        ]

    async def get_id_by_key(self, system_key: str) -> str | None:
        """Return the UUID (as text) for a given system_key."""
        result = await self._conn.execute(
            text('SELECT system_id::text FROM dim_rating_system WHERE system_key = :sk'), {'sk': system_key}
        )
        return result.scalar_one_or_none()
