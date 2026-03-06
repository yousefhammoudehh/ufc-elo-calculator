"""Repository for ``fact_bout_participant``."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.bout import BoutParticipant
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class BoutParticipantRepository(BaseRepository['BoutParticipant']):
    """Read-access to bout participants."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get_by_bout(self, bout_id: str) -> list[BoutParticipant]:
        result = await self._conn.execute(
            text("""
                SELECT bp.fighter_id::text, f.display_name,
                       bp.corner, bp.outcome_key
                FROM fact_bout_participant bp
                JOIN dim_fighter f ON f.fighter_id = bp.fighter_id
                WHERE bp.bout_id = :bid
                ORDER BY bp.corner
            """),
            {'bid': bout_id},
        )
        return [
            BoutParticipant(
                fighter_id=r['fighter_id'],
                display_name=r['display_name'],
                corner=r['corner'],
                outcome_key=r['outcome_key'],
            )
            for r in result.mappings()
        ]

    async def get_by_bouts(self, bout_ids: list[str]) -> dict[str, list[BoutParticipant]]:
        """Batch-load participants for multiple bouts."""
        if not bout_ids:
            return {}
        result = await self._conn.execute(
            text("""
                SELECT bp.bout_id::text, bp.fighter_id::text,
                       f.display_name, bp.corner, bp.outcome_key
                FROM fact_bout_participant bp
                JOIN dim_fighter f ON f.fighter_id = bp.fighter_id
                WHERE bp.bout_id::text = ANY(:bids)
                ORDER BY bp.bout_id, bp.corner
            """),
            {'bids': bout_ids},
        )
        out: dict[str, list[BoutParticipant]] = {}
        for r in result.mappings():
            bid = r['bout_id']
            out.setdefault(bid, []).append(
                BoutParticipant(
                    fighter_id=r['fighter_id'],
                    display_name=r['display_name'],
                    corner=r['corner'],
                    outcome_key=r['outcome_key'],
                )
            )
        return out
