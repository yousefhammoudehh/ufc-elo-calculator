"""Repository for ``serving_fighter_timeseries``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.rating import RatingTimeseriesPoint
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class FighterTimeseriesRepository(BaseRepository['RatingTimeseriesPoint']):
    """Read-access to the pre-computed fighter-rating timeseries."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get_timeseries(
        self, fighter_id: str, system_key: str, *, limit: int = 500, offset: int = 0
    ) -> list[RatingTimeseriesPoint]:
        result = await self._conn.execute(
            text("""
                SELECT t.date::text, t.rating_mean, t.rd
                FROM serving_fighter_timeseries t
                JOIN dim_rating_system s ON s.system_id = t.system_id
                WHERE t.fighter_id = :fid
                  AND s.system_key = :sk
                ORDER BY t.date
                LIMIT :lim OFFSET :off
            """),
            {'fid': fighter_id, 'sk': system_key, 'lim': limit, 'off': offset},
        )
        return [
            RatingTimeseriesPoint(
                date=r['date'],
                rating_mean=float(r['rating_mean']),
                rd=float(r['rd']) if r.get('rd') is not None else None,
            )
            for r in result.mappings()
        ]

    async def count(self, fighter_id: str, system_key: str) -> int:
        result = await self._conn.execute(
            text("""
                SELECT count(*)
                FROM serving_fighter_timeseries t
                JOIN dim_rating_system s ON s.system_id = t.system_id
                WHERE t.fighter_id = :fid
                  AND s.system_key = :sk
            """),
            {'fid': fighter_id, 'sk': system_key},
        )
        return int(result.scalar_one())
