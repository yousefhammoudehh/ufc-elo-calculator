"""Pydantic models for event endpoints."""

from pydantic import BaseModel

from elo_calculator.presentation.models.common import PaginationMeta


class EventSummary(BaseModel):
    event_id: str
    event_date: str
    event_name: str
    promotion_name: str | None = None
    location: str | None = None
    num_fights: int | None = None


class EventListResponse(BaseModel):
    data: list[EventSummary]
    pagination: PaginationMeta


class ParticipantCard(BaseModel):
    fighter_id: str
    display_name: str
    corner: str
    outcome_key: str


class BoutCard(BaseModel):
    bout_id: str
    division_key: str | None = None
    weight_class_raw: str | None = None
    is_title_fight: bool
    method_group: str | None = None
    decision_type: str | None = None
    finish_round: int | None = None
    finish_time_seconds: int | None = None
    participants: list[ParticipantCard]


class EventDetailResponse(BaseModel):
    event_id: str
    event_date: str
    event_name: str
    promotion_name: str | None = None
    location: str | None = None
    bouts: list[BoutCard]
