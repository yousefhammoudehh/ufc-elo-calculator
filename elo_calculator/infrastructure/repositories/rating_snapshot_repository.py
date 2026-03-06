"""Repository for ``fact_rating_snapshot``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.rating import FighterRatingProfile
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class RatingSnapshotRepository(BaseRepository['FighterRatingProfile']):
    """Read-access to rating snapshots."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get_latest_by_fighter(self, fighter_id: str) -> list[FighterRatingProfile]:
        """Return the most recent rating for each system for a given fighter."""
        result = await self._conn.execute(
            text("""
                WITH latest AS (
                    SELECT system_id, MAX(as_of_date) AS max_date
                    FROM fact_rating_snapshot
                    WHERE fighter_id = :fid
                    GROUP BY system_id
                )
                SELECT s.system_key, rs.rating_mean, rs.rating_rd
                FROM fact_rating_snapshot rs
                JOIN latest l ON rs.system_id = l.system_id
                                 AND rs.as_of_date = l.max_date
                                 AND rs.fighter_id = :fid
                JOIN dim_rating_system s ON s.system_id = rs.system_id
                ORDER BY s.system_key
            """),
            {'fid': fighter_id},
        )
        return [
            FighterRatingProfile(
                system_key=r['system_key'],
                rating_mean=float(r['rating_mean']),
                rd=float(r['rating_rd']) if r.get('rating_rd') is not None else None,
            )
            for r in result.mappings()
        ]

    async def get_peak_by_fighter(self, fighter_id: str) -> dict[str, float]:
        """Return peak (max) rating per system for a given fighter."""
        result = await self._conn.execute(
            text("""
                SELECT s.system_key, MAX(rs.rating_mean) AS peak
                FROM fact_rating_snapshot rs
                JOIN dim_rating_system s ON s.system_id = rs.system_id
                WHERE rs.fighter_id = :fid
                GROUP BY s.system_key
            """),
            {'fid': fighter_id},
        )
        return {r['system_key']: float(r['peak']) for r in result.mappings()}
