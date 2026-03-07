from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum

# ---------------------------------------------------------------------------
# Core enums (existing)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# System H — UFC 5 Stat Builder types
# ---------------------------------------------------------------------------


class UFC5StatKey(str, Enum):
    """22 automatable UFC 5-style attributes (Switch Stance & Cut Resistance excluded)."""

    ACCURACY = 'accuracy'
    BLOCKING = 'blocking'
    FOOTWORK = 'footwork'
    HEAD_MOVEMENT = 'head_movement'
    KICK_POWER = 'kick_power'
    KICK_SPEED = 'kick_speed'
    PUNCH_POWER = 'punch_power'
    PUNCH_SPEED = 'punch_speed'
    TAKEDOWN_DEFENCE = 'takedown_defence'
    BOTTOM_GAME = 'bottom_game'
    CLINCH_GRAPPLE = 'clinch_grapple'
    CLINCH_STRIKING = 'clinch_striking'
    GROUND_STRIKING = 'ground_striking'
    SUBMISSION_DEFENCE = 'submission_defence'
    SUBMISSION_OFFENCE = 'submission_offence'
    TAKEDOWNS = 'takedowns'
    TOP_GAME = 'top_game'
    BODY_STRENGTH = 'body_strength'
    CARDIO = 'cardio'
    CHIN_STRENGTH = 'chin_strength'
    LEGS_STRENGTH = 'legs_strength'
    RECOVERY = 'recovery'


class UFC5PerkKey(str, Enum):
    """Perks that can be modelled from aggregate fight data."""

    LIKE_GLUE = 'like_glue'
    SLIPPERY = 'slippery'
    TO_YOUR_FEET = 'to_your_feet'
    WRESTLE_CLINIC = 'wrestle_clinic'
    GRINDER = 'grinder'
    WORK_HORSE = 'work_horse'
    MARATHONER = 'marathoner'
    OUT_THE_GATES = 'out_the_gates'
    HIGHER_ALTITUDE = 'higher_altitude'
    WAKE_UP_CALL = 'wake_up_call'
    CRAFTY = 'crafty'
    NO_CIGAR = 'no_cigar'


class UFC5StatBucket(str, Enum):
    """Attribute buckets for weighted overall computation."""

    STANDUP = 'standup'
    GRAPPLING = 'grappling'
    HEALTH = 'health'


@dataclass
class UFC5BoutFeatures:
    """Raw computed features from one bout for one fighter."""

    fight_minutes: float = 0.0

    # Striking accuracy / defence
    sig_acc: float = 0.0
    sig_def: float = 0.0
    head_evasion: float = 0.0
    slpm: float = 0.0
    sapm: float = 0.0
    str_diff: float = 0.0

    # Takedown / grappling
    td15: float = 0.0
    td_acc: float = 0.0
    td_def: float = 0.0
    sub15: float = 0.0
    opp_sub15: float = 0.0
    ctrl_share: float = 0.0
    opp_ctrl_share: float = 0.0
    rev15: float = 0.0
    opp_rev15: float = 0.0

    # Striking distribution
    head_share: float = 0.0
    body_share: float = 0.0
    leg_share: float = 0.0
    dist_share: float = 0.0
    clinch_share: float = 0.0
    ground_share: float = 0.0

    # Per-minute rates
    head_sig_per_min: float = 0.0
    body_sig_per_min: float = 0.0
    leg_sig_per_min: float = 0.0
    dist_sig_per_min: float = 0.0
    clinch_sig_per_min: float = 0.0
    ground_sig_per_min: float = 0.0

    # Knockdowns / damage
    kd15: float = 0.0
    kd_abs15: float = 0.0
    head_abs15: float = 0.0
    body_abs15: float = 0.0
    leg_abs15: float = 0.0

    # Cardio / late-game
    late_output_ratio: float | None = None
    late_abs_ratio: float | None = None

    # Round-1-2 output ratio (for OUT_THE_GATES perk)
    early_output_ratio: float | None = None

    # Career-level aggregates (updated progressively)
    ko_win_rate: float = 0.0
    sub_win_rate: float = 0.0
    total_fight_minutes: float = 0.0
    kd_survival_rate: float = 1.0


@dataclass
class UFC5FighterState:
    """Mutable state for one fighter in System H, carried across bouts."""

    fighter_id: str
    stats: dict[UFC5StatKey, float] = field(default_factory=dict)
    mma_fight_count: int = 0
    tier_a_fight_count: int = 0
    division_id: str | None = None

    # Perk state
    perk_scores: dict[UFC5PerkKey, float] = field(default_factory=dict)
    perk_active: dict[UFC5PerkKey, bool] = field(default_factory=dict)
    perk_consecutive_above: dict[UFC5PerkKey, int] = field(default_factory=dict)
    perk_consecutive_below: dict[UFC5PerkKey, int] = field(default_factory=dict)

    # Career accumulators
    total_fights: int = 0
    total_wins: int = 0
    total_ko_wins: int = 0
    total_sub_wins: int = 0
    total_knockdowns_scored: float = 0.0
    total_knockdowns_absorbed: float = 0.0
    total_kd_events_survived: float = 0.0
    total_fight_minutes: float = 0.0
    total_rounds_fought: int = 0

    def __post_init__(self) -> None:
        if not self.stats:
            self.stats = dict.fromkeys(UFC5StatKey, 85.0)
        if not self.perk_scores:
            self.perk_scores = dict.fromkeys(UFC5PerkKey, 80.0)
        if not self.perk_active:
            self.perk_active = dict.fromkeys(UFC5PerkKey, False)
        if not self.perk_consecutive_above:
            self.perk_consecutive_above = dict.fromkeys(UFC5PerkKey, 0)
        if not self.perk_consecutive_below:
            self.perk_consecutive_below = dict.fromkeys(UFC5PerkKey, 0)


@dataclass(frozen=True)
class UFC5StatDelta:
    """Per-bout overall change for System H."""

    bout_id: str
    fighter_id: str
    pre_overall: float
    post_overall: float
    delta_overall: float
    tier_used: EvidenceTier
    stat_deltas: dict[str, dict[str, float]]
    perk_deltas: dict[str, dict[str, float]]
