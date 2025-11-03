from typing import Any
from uuid import UUID

from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.presentation.models.fighter_models import FighterResponse
from elo_calculator.presentation.models.shared import DataModel


class BoutCreateRequest(DataModel):
    bout_id: str
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None


class BoutUpdateRequest(DataModel):
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None


class BoutResponse(DataModel):
    bout_id: str
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None


class BoutCalcSide(DataModel):
    fighter: FighterResponse
    outcome: FightOutcome
    # Performance score
    ps: float | None = None
    # Core stats
    kd: int | None = None
    sig_strikes: int | None = None
    sig_strikes_thrown: int | None = None
    total_strikes: int | None = None
    total_strikes_thrown: int | None = None
    td: int | None = None
    td_attempts: int | None = None
    sub_attempts: int | None = None
    reversals: int | None = None
    control_time_sec: int | None = None
    head_ss: int | None = None
    body_ss: int | None = None
    leg_ss: int | None = None
    distance_ss: int | None = None
    clinch_ss: int | None = None
    ground_ss: int | None = None
    strike_accuracy: float | None = None
    # Elo state
    elo_before: float | None = None
    elo_after: float | None = None
    elo_delta: float | None = None
    # Calculation components
    E: float | None = None
    Y: float | None = None
    K: float | None = None
    # Context used for K
    ufc_fights_before: int | None = None
    days_since_last_fight: int | None = None
    # K-factor breakdown for transparency
    k_breakdown: dict[str, Any] | None = None
    # Human-readable explanation of K for this side
    k_explanation: str | None = None


class BoutDetailsResponse(DataModel):
    bout_id: str
    event_id: UUID | None = None
    event_date: str | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None
    rounds_scheduled: int | None = None
    # Computation inputs summary (optional, for transparency)
    inputs: dict[str, Any] | None = None
    # Two sides
    side1: BoutCalcSide
    side2: BoutCalcSide
    # Human-readable explanation of PS v2.0 for this bout
    ps_explanation: str | None = None
