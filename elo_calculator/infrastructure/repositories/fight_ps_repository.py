"""Repository for ``fact_fight_ps``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.bout import BoutFightPS
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class FightPSRepository(BaseRepository['BoutFightPS']):
    """Read-access to fight-level performance scores."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get_by_bout(self, bout_id: str) -> list[BoutFightPS]:
        result = await self._conn.execute(
            text("""
                SELECT fighter_id::text, ps_fight, quality_of_win
                FROM fact_fight_ps
                WHERE bout_id = :bid
                ORDER BY fighter_id
            """),
            {'bid': bout_id},
        )
        return [
            BoutFightPS(
                fighter_id=r['fighter_id'], ps_fight=float(r['ps_fight']), quality_of_win=float(r['quality_of_win'])
            )
            for r in result.mappings()
        ]
