"""Pydantic models for fighter endpoints."""

from pydantic import BaseModel

from elo_calculator.presentation.models.common import PaginationMeta


class FighterSummary(BaseModel):
    fighter_id: str
    display_name: str
    nickname: str | None = None
    country_code: str
    sex: str


class FighterListResponse(BaseModel):
    data: list[FighterSummary]
    pagination: PaginationMeta


class FighterRating(BaseModel):
    system_key: str
    rating_mean: float
    rd: float | None = None
    peak_rating: float


class FighterProfileResponse(BaseModel):
    fighter_id: str
    display_name: str
    nickname: str | None = None
    birth_date: str | None = None
    birth_place: str | None = None
    country_code: str
    fighting_out_of: str | None = None
    affiliation_gym: str | None = None
    foundation_style: str | None = None
    profile_image_url: str | None = None
    height_cm: float | None = None
    reach_cm: float | None = None
    stance: str | None = None
    sex: str
    ratings: list[FighterRating]


class TimeseriesPoint(BaseModel):
    date: str
    rating_mean: float
    rd: float | None = None


class FighterTimeseriesResponse(BaseModel):
    fighter_id: str
    system_key: str
    data: list[TimeseriesPoint]
    pagination: PaginationMeta


class BoutParticipantSummary(BaseModel):
    fighter_id: str
    display_name: str
    corner: str
    outcome_key: str


class FighterBoutSummary(BaseModel):
    bout_id: str
    event_id: str
    event_date: str
    event_name: str
    division_key: str | None = None
    weight_class_raw: str | None = None
    is_title_fight: bool
    method_group: str | None = None
    decision_type: str | None = None
    finish_round: int | None = None
    finish_time_seconds: int | None = None
    participants: list[BoutParticipantSummary]


class FighterBoutListResponse(BaseModel):
    fighter_id: str
    data: list[FighterBoutSummary]
    pagination: PaginationMeta
