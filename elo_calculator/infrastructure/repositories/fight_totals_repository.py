"""Repository for ``fact_fight_totals``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.bout import FighterBoutStats
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class FightTotalsRepository(BaseRepository['FighterBoutStats']):
    """Read-access to fight-level totals."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get_by_bout(self, bout_id: str) -> list[FighterBoutStats]:
        result = await self._conn.execute(
            text("""
                SELECT fighter_id::text,
                       COALESCE(kd, 0) AS kd,
                       COALESCE(sig_landed, 0) AS sig_landed,
                       COALESCE(sig_attempted, 0) AS sig_attempted,
                       COALESCE(total_landed, 0) AS total_landed,
                       COALESCE(total_attempted, 0) AS total_attempted,
                       COALESCE(td_landed, 0) AS td_landed,
                       COALESCE(td_attempted, 0) AS td_attempted,
                       COALESCE(sub_attempts, 0) AS sub_attempts,
                       COALESCE(ctrl_seconds, 0) AS ctrl_seconds
                FROM fact_fight_totals
                WHERE bout_id = :bid
                ORDER BY fighter_id
            """),
            {'bid': bout_id},
        )
        return [
            FighterBoutStats(
                fighter_id=r['fighter_id'],
                kd=int(r['kd']),
                sig_landed=int(r['sig_landed']),
                sig_attempted=int(r['sig_attempted']),
                total_landed=int(r['total_landed']),
                total_attempted=int(r['total_attempted']),
                td_landed=int(r['td_landed']),
                td_attempted=int(r['td_attempted']),
                sub_attempts=int(r['sub_attempts']),
                ctrl_seconds=int(r['ctrl_seconds']),
            )
            for r in result.mappings()
        ]
