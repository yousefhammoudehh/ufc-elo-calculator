from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum


class BoutOutcome(str, Enum):
    WIN = 'win'
    LOSS = 'loss'
    DRAW = 'draw'
    NO_CONTEST = 'no_contest'


class EvidenceTier(str, Enum):
    A = 'A'
    B = 'B'
    C = 'C'


class FinishMethod(str, Enum):
    KO = 'KO'
    TKO = 'TKO'
    SUB = 'SUB'
    DEC = 'DEC'
    DQ = 'DQ'
    OVERTURNED = 'OVERTURNED'
    OTHER = 'OTHER'
    UNKNOWN = 'UNKNOWN'


@dataclass(frozen=True)
class FighterRoundStats:
    kd: float = 0.0
    sig_landed: float = 0.0
    sig_attempted: float = 0.0
    total_landed: float = 0.0
    total_attempted: float = 0.0
    td_landed: float = 0.0
    td_attempted: float = 0.0
    sub_attempts: float = 0.0
    rev: float = 0.0
    ctrl_seconds: float = 0.0
    head_landed: float = 0.0
    body_landed: float = 0.0
    leg_landed: float = 0.0
    distance_landed: float = 0.0
    clinch_landed: float = 0.0
    ground_landed: float = 0.0


@dataclass(frozen=True)
class RoundMeta:
    round_num: int
    round_seconds: int = 300
    is_finish_round: bool = False
    finish_elapsed_seconds: int | None = None
    winner_is_a: bool | None = None


@dataclass(frozen=True)
class RoundPSResult:
    ps_a: float
    ps_b: float
    dmg_a: float
    dmg_b: float
    dom_a: float
    dom_b: float
    dur_a: float
    dur_b: float
    p10_9_a: float
    p10_8_a: float
    p10_7_a: float
    p10_9_b: float
    p10_8_b: float
    p10_7_b: float


@dataclass(frozen=True)
class FightPSResult:
    ps_fight_a: float
    ps_fight_b: float
    quality_of_win_a: float
    quality_of_win_b: float


@dataclass(frozen=True)
class BoutEvidence:
    bout_id: str
    event_date: date
    sport_key: str
    outcome_for_a: BoutOutcome
    tier: EvidenceTier
    method: FinishMethod = FinishMethod.UNKNOWN
    finish_round: int | None = None
    finish_time_seconds: int | None = None
    scheduled_rounds: int = 3
    is_title_fight: bool = False
    promotion_strength: float | None = None
    ps_margin_a: float | None = None
    ps_fight_a: float | None = None


@dataclass(frozen=True)
class RatingDelta:
    bout_id: str
    fighter_id: str
    pre_rating: float
    post_rating: float
    delta_rating: float
    expected_win_prob: float
    target_score: float
    k_effective: float
    tier_used: EvidenceTier


@dataclass(frozen=True)
class ProbabilitySnapshot:
    fighter_a_id: str
    fighter_b_id: str
    p_a_wins: float


@dataclass(frozen=True)
class StackingSample:
    p_a: float
    p_b: float
    p_c: float
    p_n: float | None
    outcome_a_win: float
    tier: EvidenceTier
    scheduled_rounds: int
    is_title_fight: bool
    days_since_last_mma: float
    missing_weight_class: bool
    missing_promotion: bool
    opponent_connectivity: float
