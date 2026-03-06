"""Repository for ``fact_event``."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.event import Event
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class EventRepository(BaseRepository['Event']):
    """Read-access to canonical events."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get(self, event_id: str) -> Event | None:
        result = await self._conn.execute(
            text("""
                SELECT e.event_id::text, e.event_date::text, e.event_name,
                       e.promotion_id::text, p.promotion_name,
                       e.location, e.num_fights
                FROM fact_event e
                LEFT JOIN dim_promotion p ON p.promotion_id = e.promotion_id
                WHERE e.event_id = :eid
            """),
            {'eid': event_id},
        )
        row = result.mappings().first()
        if not row:
            return None
        return self._to_entity(row)

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[Event]:
        result = await self._conn.execute(
            text("""
                SELECT e.event_id::text, e.event_date::text, e.event_name,
                       e.promotion_id::text, p.promotion_name,
                       e.location, e.num_fights
                FROM fact_event e
                LEFT JOIN dim_promotion p ON p.promotion_id = e.promotion_id
                ORDER BY e.event_date DESC
                LIMIT :lim OFFSET :off
            """),
            {'lim': limit, 'off': offset},
        )
        return [self._to_entity(r) for r in result.mappings()]

    async def count(self) -> int:
        result = await self._conn.execute(text('SELECT count(*) FROM fact_event'))
        return int(result.scalar_one())

    # ------------------------------------------------------------------

    @staticmethod
    def _to_entity(row: object) -> Event:
        r = cast(Mapping[str, Any], row)
        return Event(
            id=r['event_id'],
            event_id=r['event_id'],
            event_date=r['event_date'],
            event_name=r['event_name'],
            promotion_id=r.get('promotion_id'),
            promotion_name=r.get('promotion_name'),
            location=r.get('location'),
            num_fights=int(r['num_fights']) if r.get('num_fights') is not None else None,
        )
