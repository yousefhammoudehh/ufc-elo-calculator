"""Repository for ``serving_current_rankings``."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.rating import CurrentRanking
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class RankingRepository(BaseRepository['CurrentRanking']):
    """Read-access to the pre-computed current-rankings serving table."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def list_rankings(
        self, system_key: str, division_key: str, sex: str = 'M', *, limit: int = 25, offset: int = 0
    ) -> list[CurrentRanking]:
        result = await self._conn.execute(
            text("""
                SELECT r.rank, r.rating_mean, r.rd,
                       r.last_fight_date::text, r.as_of_date::text,
                       r.fighter_id::text, f.display_name,
                       s.system_key,
                       d.division_key, d.display_name AS division_display_name, d.sex
                FROM serving_current_rankings r
                JOIN dim_fighter f ON f.fighter_id = r.fighter_id
                JOIN dim_rating_system s ON s.system_id = r.system_id
                JOIN dim_division d ON d.division_id = r.division_id
                WHERE s.system_key = :sk
                  AND d.division_key = :dk
                  AND d.sex = :sex
                ORDER BY r.rank
                LIMIT :lim OFFSET :off
            """),
            {'sk': system_key, 'dk': division_key, 'sex': sex, 'lim': limit, 'off': offset},
        )
        return [self._to_entity(r) for r in result.mappings()]

    async def count_rankings(self, system_key: str, division_key: str, sex: str = 'M') -> int:
        result = await self._conn.execute(
            text("""
                SELECT count(*)
                FROM serving_current_rankings r
                JOIN dim_rating_system s ON s.system_id = r.system_id
                JOIN dim_division d ON d.division_id = r.division_id
                WHERE s.system_key = :sk
                  AND d.division_key = :dk
                  AND d.sex = :sex
            """),
            {'sk': system_key, 'dk': division_key, 'sex': sex},
        )
        return int(result.scalar_one())

    # ------------------------------------------------------------------

    @staticmethod
    def _to_entity(row: object) -> CurrentRanking:
        r = cast(Mapping[str, Any], row)
        return CurrentRanking(
            system_key=r['system_key'],
            division_key=r['division_key'],
            division_display_name=r.get('division_display_name'),
            sex=r.get('sex', 'U'),
            as_of_date=r.get('as_of_date', ''),
            fighter_id=r['fighter_id'],
            display_name=r['display_name'],
            rank=int(r['rank']),
            rating_mean=float(r['rating_mean']),
            rd=float(r['rd']) if r.get('rd') is not None else None,
            last_fight_date=r.get('last_fight_date'),
        )
