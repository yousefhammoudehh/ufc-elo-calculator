"""Repository for ``dim_division``."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.division import Division
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class DivisionRepository(BaseRepository['Division']):
    """Read-access to weight-class divisions."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def list_canonical_mma(self) -> list[Division]:
        """Return all canonical MMA divisions (men + women)."""
        result = await self._conn.execute(
            text("""
                SELECT division_id::text, division_key, display_name,
                       sex, limit_lbs, is_canonical_mma
                FROM dim_division
                WHERE is_canonical_mma = true
                ORDER BY sex, limit_lbs NULLS LAST, division_key
            """)
        )
        return [self._to_entity(r) for r in result.mappings()]

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[Division]:
        result = await self._conn.execute(
            text("""
                SELECT division_id::text, division_key, display_name,
                       sex, limit_lbs, is_canonical_mma
                FROM dim_division
                ORDER BY sex, limit_lbs NULLS LAST, division_key
                LIMIT :lim OFFSET :off
            """),
            {'lim': limit, 'off': offset},
        )
        return [self._to_entity(r) for r in result.mappings()]

    # ------------------------------------------------------------------

    @staticmethod
    def _to_entity(row: object) -> Division:
        r = cast(Mapping[str, Any], row)
        return Division(
            id=r['division_id'],
            division_id=r['division_id'],
            division_key=r['division_key'],
            display_name=r.get('display_name'),
            sex=r.get('sex', 'U'),
            limit_lbs=float(r['limit_lbs']) if r.get('limit_lbs') is not None else None,
            is_canonical_mma=bool(r.get('is_canonical_mma', False)),
        )
