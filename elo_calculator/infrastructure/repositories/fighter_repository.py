"""Repository for ``dim_fighter``."""

from __future__ import annotations

import builtins
from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.domain.entities.fighter import Fighter
from elo_calculator.infrastructure.repositories.base_repository import BaseRepository


class FighterRepository(BaseRepository['Fighter']):
    """Read-access to the canonical fighter dimension table."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection

    async def get(self, fighter_id: str) -> Fighter | None:
        result = await self._conn.execute(
            text("""
                SELECT fighter_id::text, display_name, nickname,
                       birth_date::text, birth_place, country_code,
                       fighting_out_of, affiliation_gym, foundation_style,
                       profile_image_url, height_cm, reach_cm, stance, sex
                FROM dim_fighter
                WHERE fighter_id = :fid
            """),
            {'fid': fighter_id},
        )
        row = result.mappings().first()
        if not row:
            return None
        return self._to_entity(row)

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[Fighter]:
        result = await self._conn.execute(
            text("""
                SELECT fighter_id::text, display_name, nickname,
                       birth_date::text, birth_place, country_code,
                       fighting_out_of, affiliation_gym, foundation_style,
                       profile_image_url, height_cm, reach_cm, stance, sex
                FROM dim_fighter
                ORDER BY display_name
                LIMIT :lim OFFSET :off
            """),
            {'lim': limit, 'off': offset},
        )
        return [self._to_entity(r) for r in result.mappings()]

    async def search(self, query: str, *, limit: int = 25, offset: int = 0) -> builtins.list[Fighter]:
        """Case-insensitive ILIKE search on ``display_name``."""
        result = await self._conn.execute(
            text("""
                SELECT fighter_id::text, display_name, nickname,
                       birth_date::text, birth_place, country_code,
                       fighting_out_of, affiliation_gym, foundation_style,
                       profile_image_url, height_cm, reach_cm, stance, sex
                FROM dim_fighter
                WHERE display_name ILIKE :q
                ORDER BY display_name
                LIMIT :lim OFFSET :off
            """),
            {'q': f'%{query}%', 'lim': limit, 'off': offset},
        )
        return [self._to_entity(r) for r in result.mappings()]

    async def count_search(self, query: str) -> int:
        result = await self._conn.execute(
            text('SELECT count(*) FROM dim_fighter WHERE display_name ILIKE :q'), {'q': f'%{query}%'}
        )
        return int(result.scalar_one())

    async def count(self) -> int:
        result = await self._conn.execute(text('SELECT count(*) FROM dim_fighter'))
        return int(result.scalar_one())

    # ------------------------------------------------------------------

    @staticmethod
    def _to_entity(row: object) -> Fighter:
        r = cast(Mapping[str, Any], row)
        return Fighter(
            id=r['fighter_id'],
            fighter_id=r['fighter_id'],
            display_name=r['display_name'],
            nickname=r.get('nickname'),
            birth_date=r.get('birth_date'),
            birth_place=r.get('birth_place'),
            country_code=r.get('country_code', 'UNK'),
            fighting_out_of=r.get('fighting_out_of'),
            affiliation_gym=r.get('affiliation_gym'),
            foundation_style=r.get('foundation_style'),
            profile_image_url=r.get('profile_image_url'),
            height_cm=float(r['height_cm']) if r.get('height_cm') is not None else None,
            reach_cm=float(r['reach_cm']) if r.get('reach_cm') is not None else None,
            stance=r.get('stance'),
            sex=r.get('sex', 'U'),
        )
