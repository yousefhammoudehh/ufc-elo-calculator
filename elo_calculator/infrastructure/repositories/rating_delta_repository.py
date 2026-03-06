"""Repository for ``fact_rating_delta``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.bout import BoutRatingDelta
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class RatingDeltaRepository(BaseRepository['BoutRatingDelta']):
    """Read-access to per-bout rating deltas."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get_by_bout(self, bout_id: str) -> list[BoutRatingDelta]:
        result = await self._conn.execute(
            text("""
                SELECT rd.fighter_id::text, s.system_key,
                       rd.pre_rating, rd.post_rating, rd.delta_rating,
                       rd.expected_win_prob
                FROM fact_rating_delta rd
                JOIN dim_rating_system s ON s.system_id = rd.system_id
                WHERE rd.bout_id = :bid
                ORDER BY s.system_key, rd.fighter_id
            """),
            {'bid': bout_id},
        )
        return [
            BoutRatingDelta(
                fighter_id=r['fighter_id'],
                system_key=r['system_key'],
                pre_rating=float(r['pre_rating']),
                post_rating=float(r['post_rating']),
                delta_rating=float(r['delta_rating']),
                expected_win_prob=float(r['expected_win_prob']) if r.get('expected_win_prob') is not None else None,
            )
            for r in result.mappings()
        ]
