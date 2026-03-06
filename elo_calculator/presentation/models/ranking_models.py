"""Pydantic models for ranking endpoints."""

from pydantic import BaseModel

from elo_calculator.presentation.models.common import PaginationMeta


class RankingEntry(BaseModel):
    rank: int
    fighter_id: str
    display_name: str
    rating_mean: float
    rd: float | None = None
    last_fight_date: str | None = None


class RankingListResponse(BaseModel):
    system_key: str
    division_key: str
    sex: str
    as_of_date: str
    data: list[RankingEntry]
    pagination: PaginationMeta
