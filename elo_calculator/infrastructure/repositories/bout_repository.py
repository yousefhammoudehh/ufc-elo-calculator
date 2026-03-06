"""Repository for ``fact_bout``."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class BoutRepository(BaseRepository['dict[str, Any]']):
    """Read-access to bouts.

    Returns raw dicts rather than full entities because bouts must be
    enriched with participants, stats and rating changes by the service layer.
    """

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get(self, bout_id: str) -> dict[str, Any] | None:
        result = await self._conn.execute(
            text("""
                SELECT b.bout_id::text, b.event_id::text,
                       e.event_date::text, e.event_name,
                       s.sport_key,
                       d.division_key,
                       b.weight_class_raw,
                       b.is_title_fight,
                       b.method_group, b.decision_type,
                       b.finish_round, b.finish_time_seconds,
                       b.scheduled_rounds, b.referee
                FROM fact_bout b
                JOIN fact_event e ON e.event_id = b.event_id
                JOIN dim_sport s ON s.sport_id = b.sport_id
                LEFT JOIN dim_division d ON d.division_id = COALESCE(b.mma_division_id, b.division_id)
                WHERE b.bout_id = :bid
            """),
            {'bid': bout_id},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def list_by_event(self, event_id: str) -> list[dict[str, Any]]:
        result = await self._conn.execute(
            text("""
                SELECT b.bout_id::text, b.event_id::text,
                       e.event_date::text, e.event_name,
                       d.division_key,
                       b.weight_class_raw,
                       b.is_title_fight,
                       b.method_group, b.decision_type,
                       b.finish_round, b.finish_time_seconds
                FROM fact_bout b
                JOIN fact_event e ON e.event_id = b.event_id
                LEFT JOIN dim_division d ON d.division_id = COALESCE(b.mma_division_id, b.division_id)
                WHERE b.event_id = :eid
                ORDER BY b.bout_id
            """),
            {'eid': event_id},
        )
        return [dict(r) for r in result.mappings()]

    async def list_by_fighter(self, fighter_id: str, *, limit: int = 25, offset: int = 0) -> list[dict[str, Any]]:
        result = await self._conn.execute(
            text("""
                SELECT b.bout_id::text, b.event_id::text,
                       e.event_date::text, e.event_name,
                       d.division_key,
                       b.weight_class_raw,
                       b.is_title_fight,
                       b.method_group, b.decision_type,
                       b.finish_round, b.finish_time_seconds
                FROM fact_bout b
                JOIN fact_event e ON e.event_id = b.event_id
                JOIN fact_bout_participant bp ON bp.bout_id = b.bout_id
                LEFT JOIN dim_division d ON d.division_id = COALESCE(b.mma_division_id, b.division_id)
                WHERE bp.fighter_id = :fid
                ORDER BY e.event_date DESC
                LIMIT :lim OFFSET :off
            """),
            {'fid': fighter_id, 'lim': limit, 'off': offset},
        )
        return [dict(r) for r in result.mappings()]

    async def count_by_fighter(self, fighter_id: str) -> int:
        result = await self._conn.execute(
            text("""
                SELECT count(*)
                FROM fact_bout_participant
                WHERE fighter_id = :fid
            """),
            {'fid': fighter_id},
        )
        return int(result.scalar_one())
