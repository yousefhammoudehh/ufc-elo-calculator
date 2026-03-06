"""Pydantic models for bout-detail endpoint."""

from pydantic import BaseModel


class BoutParticipantResponse(BaseModel):
    fighter_id: str
    display_name: str
    corner: str
    outcome_key: str


class FightStatsResponse(BaseModel):
    fighter_id: str
    kd: int
    sig_landed: int
    sig_attempted: int
    total_landed: int
    total_attempted: int
    td_landed: int
    td_attempted: int
    sub_attempts: int
    ctrl_seconds: int


class RatingChangeResponse(BaseModel):
    fighter_id: str
    system_key: str
    pre_rating: float
    post_rating: float
    delta_rating: float
    expected_win_prob: float | None = None


class PerformanceScoreResponse(BaseModel):
    fighter_id: str
    ps_fight: float
    quality_of_win: float


class BoutDetailResponse(BaseModel):
    bout_id: str
    event_id: str
    event_date: str
    event_name: str
    sport_key: str
    division_key: str | None = None
    weight_class_raw: str | None = None
    is_title_fight: bool
    method_group: str | None = None
    decision_type: str | None = None
    finish_round: int | None = None
    finish_time_seconds: int | None = None
    scheduled_rounds: int | None = None
    referee: str | None = None
    participants: list[BoutParticipantResponse]
    fight_stats: list[FightStatsResponse]
    rating_changes: list[RatingChangeResponse]
    performance_scores: list[PerformanceScoreResponse]
