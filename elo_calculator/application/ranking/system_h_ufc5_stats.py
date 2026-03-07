"""System H — UFC 5 Progressive Stat Builder.

Builds 22 UFC 5-style attribute ratings (80-100 scale) for every fighter,
updated fight-by-fight.  Each bout computes raw features from round/fight
stats, converts them to division-relative shrunk-percentile targets, and
moves the fighter's ratings partway toward those targets with tier-based,
opponent-quality, and sample-size controls.

A 5-fight tier-A minimum gates inclusion in division percentile pools.
Non-MMA sports update only relevant stat families.

The overall score is a 3-layer composite:
  BaseOverall   (weighted geometric mean across standup/grappling/health)
  + SynergyBonus (pairwise stat interactions, capped)
  + PerkBonus    (active perk value contributions)

Perk scores use a hidden latent score with activation/deactivation thresholds.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from elo_calculator.application.ranking.math_utils import clip, logistic
from elo_calculator.application.ranking.types import (
    BoutEvidence,
    BoutOutcome,
    EvidenceTier,
    FinishMethod,
    UFC5BoutFeatures,
    UFC5FighterState,
    UFC5PerkKey,
    UFC5StatBucket,
    UFC5StatDelta,
    UFC5StatKey,
)
from elo_calculator.application.ranking.ufc5_percentiles import DivisionPercentilePool, shrink

_EPS = 1e-9
_PROMO_STRENGTH_UFC = 0.9
_PROMO_STRENGTH_MAJOR = 0.6

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Stat range bounds
_L_DEFAULT = 80.0
_U_DEFAULT = 100.0
_L_HEALTH = 84.0
_U_HEALTH = 96.0

_HEALTH_STATS: frozenset[UFC5StatKey] = frozenset(
    {
        UFC5StatKey.BODY_STRENGTH,
        UFC5StatKey.CARDIO,
        UFC5StatKey.CHIN_STRENGTH,
        UFC5StatKey.LEGS_STRENGTH,
        UFC5StatKey.RECOVERY,
    }
)


@dataclass(frozen=True)
class _BoutUpdateParams:
    """Bundle of per-bout parameters passed to _update_single_fighter."""

    division_id: str | None
    opp_overall: float
    tier: EvidenceTier
    org_weight: float
    allowed_stats: frozenset[UFC5StatKey]
    is_winner: bool
    is_mma: bool


@dataclass
class _CareerAccumulators:
    """Mutable bucket for career-level counters updated each bout."""

    mma_fight_count: int = 0
    tier_a_fight_count: int = 0
    total_fights: int = 0
    total_wins: int = 0
    total_ko_wins: int = 0
    total_sub_wins: int = 0
    total_knockdowns_scored: float = 0.0
    total_knockdowns_absorbed: float = 0.0
    total_kd_events_survived: float = 0.0
    total_fight_minutes: float = 0.0
    total_rounds_fought: int = 0


@dataclass
class _PerkResult:
    """Output of _update_perk_states."""

    scores: dict[UFC5PerkKey, float]
    active: dict[UFC5PerkKey, bool]
    consecutive_above: dict[UFC5PerkKey, int]
    consecutive_below: dict[UFC5PerkKey, int]
    deltas: dict[str, dict[str, float]]


@dataclass(frozen=True)
class UFC5StatsConfig:
    """All tuneable knobs for System H."""

    # --- Stat score bounds ---
    L_default: float = _L_DEFAULT
    U_default: float = _U_DEFAULT
    L_health: float = _L_HEALTH
    U_health: float = _U_HEALTH

    # --- Shrinkage taus by stat family ---
    tau_strike: float = 30.0
    tau_td: float = 15.0
    tau_sub: float = 10.0
    tau_health: float = 20.0
    tau_default: float = 20.0

    # --- K-factor bases by stat signal quality ---
    k_base_direct: float = 0.35
    k_base_proxy: float = 0.25
    k_base_health: float = 0.18

    # --- Tier multipliers for stat updates ---
    tier_a_mult: float = 1.00
    tier_b_mult: float = 0.60
    tier_c_mult: float = 0.40

    # --- Org / sport weights ---
    org_weight_ufc_mma: float = 1.00
    org_weight_major_mma: float = 0.80
    org_weight_regional_mma: float = 0.55
    org_weight_boxing: float = 0.35
    org_weight_kickboxing: float = 0.40
    org_weight_wrestling: float = 0.40
    org_weight_bjj: float = 0.35

    # --- Quality weight ---
    quality_offset: float = 0.85
    quality_scale: float = 0.30

    # --- Single-fight caps ---
    cap_direct: float = 3.0
    cap_proxy: float = 2.0
    cap_health: float = 2.0
    cap_extreme_down: float = 4.0

    # --- Method bonus magnitudes ---
    ko_power_bonus: float = 0.8
    sub_offence_bonus: float = 0.8
    sub_ground_bonus: float = 0.5

    # --- Bucket overall weights ---
    w_standup: float = 0.40
    w_grappling: float = 0.33
    w_health: float = 0.27

    # --- Perk thresholds ---
    perk_activation: float = 88.0
    perk_deactivation: float = 80.0
    perk_consecutive_required: int = 3
    perk_ema_alpha: float = 0.10
    perk_ema_retain: float = 0.90

    # --- Perk strength tiers (overall-point equivalent) ---
    perk_strength_minor: float = 0.25
    perk_strength_medium: float = 0.50
    perk_strength_major: float = 0.90

    # --- Synergy cap ---
    synergy_cap: float = 1.5

    # --- Non-MMA prior decay ---
    prior_decay_lambda: float = 5.0  # exp(-mma_fights / lambda)

    # --- Geometric-mean epsilon ---
    geo_eps: float = 0.02

    # --- K-factor career decay ---
    k_career_divisor: float = 6.0

    # --- Default division avg overall for quality weight ---
    default_division_avg_overall: float = 87.0


DEFAULT_UFC5_CONFIG = UFC5StatsConfig()


# ---------------------------------------------------------------------------
# Stat classification helpers
# ---------------------------------------------------------------------------

# Which bucket each stat belongs to
STAT_BUCKET: dict[UFC5StatKey, UFC5StatBucket] = {
    # Standup
    UFC5StatKey.ACCURACY: UFC5StatBucket.STANDUP,
    UFC5StatKey.BLOCKING: UFC5StatBucket.STANDUP,
    UFC5StatKey.FOOTWORK: UFC5StatBucket.STANDUP,
    UFC5StatKey.HEAD_MOVEMENT: UFC5StatBucket.STANDUP,
    UFC5StatKey.KICK_POWER: UFC5StatBucket.STANDUP,
    UFC5StatKey.KICK_SPEED: UFC5StatBucket.STANDUP,
    UFC5StatKey.PUNCH_POWER: UFC5StatBucket.STANDUP,
    UFC5StatKey.PUNCH_SPEED: UFC5StatBucket.STANDUP,
    UFC5StatKey.TAKEDOWN_DEFENCE: UFC5StatBucket.STANDUP,
    # Grappling
    UFC5StatKey.BOTTOM_GAME: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.CLINCH_GRAPPLE: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.CLINCH_STRIKING: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.GROUND_STRIKING: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.SUBMISSION_DEFENCE: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.SUBMISSION_OFFENCE: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.TAKEDOWNS: UFC5StatBucket.GRAPPLING,
    UFC5StatKey.TOP_GAME: UFC5StatBucket.GRAPPLING,
    # Health
    UFC5StatKey.BODY_STRENGTH: UFC5StatBucket.HEALTH,
    UFC5StatKey.CARDIO: UFC5StatBucket.HEALTH,
    UFC5StatKey.CHIN_STRENGTH: UFC5StatBucket.HEALTH,
    UFC5StatKey.LEGS_STRENGTH: UFC5StatBucket.HEALTH,
    UFC5StatKey.RECOVERY: UFC5StatBucket.HEALTH,
}

# Signal quality classification (direct / proxy / health)
_DIRECT_STATS: frozenset[UFC5StatKey] = frozenset(
    {
        UFC5StatKey.ACCURACY,
        UFC5StatKey.BLOCKING,
        UFC5StatKey.HEAD_MOVEMENT,
        UFC5StatKey.TAKEDOWNS,
        UFC5StatKey.TAKEDOWN_DEFENCE,
        UFC5StatKey.SUBMISSION_OFFENCE,
        UFC5StatKey.TOP_GAME,
        UFC5StatKey.GROUND_STRIKING,
        UFC5StatKey.CARDIO,
    }
)

_PROXY_STATS: frozenset[UFC5StatKey] = frozenset(
    {
        UFC5StatKey.FOOTWORK,
        UFC5StatKey.PUNCH_SPEED,
        UFC5StatKey.PUNCH_POWER,
        UFC5StatKey.KICK_SPEED,
        UFC5StatKey.KICK_POWER,
        UFC5StatKey.BOTTOM_GAME,
        UFC5StatKey.CLINCH_GRAPPLE,
        UFC5StatKey.CLINCH_STRIKING,
        UFC5StatKey.SUBMISSION_DEFENCE,
        UFC5StatKey.CHIN_STRENGTH,
        UFC5StatKey.BODY_STRENGTH,
        UFC5StatKey.LEGS_STRENGTH,
        UFC5StatKey.RECOVERY,
    }
)

# Intra-bucket weights for overall computation (from plan)
STANDUP_WEIGHTS: dict[UFC5StatKey, float] = {
    UFC5StatKey.ACCURACY: 0.15,
    UFC5StatKey.BLOCKING: 0.14,
    UFC5StatKey.FOOTWORK: 0.11,
    UFC5StatKey.HEAD_MOVEMENT: 0.12,
    UFC5StatKey.KICK_POWER: 0.07,
    UFC5StatKey.KICK_SPEED: 0.06,
    UFC5StatKey.PUNCH_POWER: 0.15,
    UFC5StatKey.PUNCH_SPEED: 0.11,
    UFC5StatKey.TAKEDOWN_DEFENCE: 0.09,
}

GRAPPLING_WEIGHTS: dict[UFC5StatKey, float] = {
    UFC5StatKey.BOTTOM_GAME: 0.08,
    UFC5StatKey.CLINCH_GRAPPLE: 0.07,
    UFC5StatKey.CLINCH_STRIKING: 0.05,
    UFC5StatKey.GROUND_STRIKING: 0.09,
    UFC5StatKey.SUBMISSION_DEFENCE: 0.15,
    UFC5StatKey.SUBMISSION_OFFENCE: 0.11,
    UFC5StatKey.TAKEDOWNS: 0.16,
    UFC5StatKey.TOP_GAME: 0.17,
    UFC5StatKey.TAKEDOWN_DEFENCE: 0.12,
}

HEALTH_WEIGHTS: dict[UFC5StatKey, float] = {
    UFC5StatKey.BODY_STRENGTH: 0.13,
    UFC5StatKey.CARDIO: 0.23,
    UFC5StatKey.CHIN_STRENGTH: 0.24,
    UFC5StatKey.LEGS_STRENGTH: 0.11,
    UFC5StatKey.RECOVERY: 0.23,
    # CUT_RESISTANCE excluded → weight redistributed into chin/cardio/recovery
    # Original: 0.06 for cut resistance
}

# ---------------------------------------------------------------------------
# Non-MMA sport → stat families
# ---------------------------------------------------------------------------

_SPORT_STAT_FAMILIES: dict[str, frozenset[UFC5StatKey]] = {
    'boxing': frozenset(
        {
            UFC5StatKey.ACCURACY,
            UFC5StatKey.BLOCKING,
            UFC5StatKey.HEAD_MOVEMENT,
            UFC5StatKey.FOOTWORK,
            UFC5StatKey.PUNCH_SPEED,
            UFC5StatKey.PUNCH_POWER,
            UFC5StatKey.CARDIO,
        }
    ),
    'kickboxing': frozenset(
        {
            UFC5StatKey.ACCURACY,
            UFC5StatKey.BLOCKING,
            UFC5StatKey.HEAD_MOVEMENT,
            UFC5StatKey.FOOTWORK,
            UFC5StatKey.PUNCH_SPEED,
            UFC5StatKey.PUNCH_POWER,
            UFC5StatKey.KICK_SPEED,
            UFC5StatKey.KICK_POWER,
            UFC5StatKey.CARDIO,
        }
    ),
    'muay': frozenset(
        {
            UFC5StatKey.ACCURACY,
            UFC5StatKey.BLOCKING,
            UFC5StatKey.HEAD_MOVEMENT,
            UFC5StatKey.FOOTWORK,
            UFC5StatKey.PUNCH_SPEED,
            UFC5StatKey.PUNCH_POWER,
            UFC5StatKey.KICK_SPEED,
            UFC5StatKey.KICK_POWER,
            UFC5StatKey.CLINCH_GRAPPLE,
            UFC5StatKey.CLINCH_STRIKING,
            UFC5StatKey.CARDIO,
        }
    ),
    'wrestling': frozenset(
        {
            UFC5StatKey.TAKEDOWNS,
            UFC5StatKey.TAKEDOWN_DEFENCE,
            UFC5StatKey.TOP_GAME,
            UFC5StatKey.CLINCH_GRAPPLE,
            UFC5StatKey.BOTTOM_GAME,
            UFC5StatKey.CARDIO,
        }
    ),
    'grappling': frozenset(
        {
            UFC5StatKey.SUBMISSION_OFFENCE,
            UFC5StatKey.SUBMISSION_DEFENCE,
            UFC5StatKey.BOTTOM_GAME,
            UFC5StatKey.TOP_GAME,
            UFC5StatKey.TAKEDOWNS,
            UFC5StatKey.TAKEDOWN_DEFENCE,
        }
    ),
    'combat_jj': frozenset(
        {UFC5StatKey.SUBMISSION_OFFENCE, UFC5StatKey.SUBMISSION_DEFENCE, UFC5StatKey.BOTTOM_GAME, UFC5StatKey.TOP_GAME}
    ),
    'judo': frozenset(
        {UFC5StatKey.TAKEDOWNS, UFC5StatKey.TAKEDOWN_DEFENCE, UFC5StatKey.CLINCH_GRAPPLE, UFC5StatKey.TOP_GAME}
    ),
    'sambo': frozenset(
        {
            UFC5StatKey.TAKEDOWNS,
            UFC5StatKey.TAKEDOWN_DEFENCE,
            UFC5StatKey.SUBMISSION_OFFENCE,
            UFC5StatKey.TOP_GAME,
            UFC5StatKey.CLINCH_GRAPPLE,
        }
    ),
    'lethwei': frozenset(
        {
            UFC5StatKey.ACCURACY,
            UFC5StatKey.BLOCKING,
            UFC5StatKey.HEAD_MOVEMENT,
            UFC5StatKey.PUNCH_POWER,
            UFC5StatKey.KICK_POWER,
            UFC5StatKey.CLINCH_STRIKING,
            UFC5StatKey.CHIN_STRENGTH,
            UFC5StatKey.CARDIO,
        }
    ),
}


# ---------------------------------------------------------------------------
# Synergy pairs (from plan)
# ---------------------------------------------------------------------------

_SYNERGY_PAIRS: list[tuple[UFC5StatKey, UFC5StatKey, float]] = [
    (UFC5StatKey.PUNCH_POWER, UFC5StatKey.ACCURACY, 1.2),
    (UFC5StatKey.PUNCH_SPEED, UFC5StatKey.ACCURACY, 0.8),
    (UFC5StatKey.FOOTWORK, UFC5StatKey.HEAD_MOVEMENT, 0.8),
    (UFC5StatKey.TAKEDOWNS, UFC5StatKey.TOP_GAME, 1.0),
    (UFC5StatKey.TOP_GAME, UFC5StatKey.SUBMISSION_OFFENCE, 0.8),
    (UFC5StatKey.BLOCKING, UFC5StatKey.CHIN_STRENGTH, 0.7),
    (UFC5StatKey.CARDIO, UFC5StatKey.RECOVERY, 0.7),
]


# ---------------------------------------------------------------------------
# Perk definitions — evidence formulas (feature weights)
# ---------------------------------------------------------------------------

# Each perk maps to a list of (feature_source, weight) where feature_source
# is a UFC5StatKey whose *normalised value* feeds into the perk evidence.
# The system normalises the stat to [0,1] then computes
# perk_evidence = sum(w * normalised_stat_value).

_PERK_FORMULAS: dict[UFC5PerkKey, list[tuple[UFC5StatKey, float]]] = {
    UFC5PerkKey.LIKE_GLUE: [
        (UFC5StatKey.CLINCH_GRAPPLE, 0.45),
        (UFC5StatKey.TAKEDOWNS, 0.30),
        (UFC5StatKey.TOP_GAME, 0.25),
    ],
    UFC5PerkKey.SLIPPERY: [
        (UFC5StatKey.TAKEDOWN_DEFENCE, 0.40),
        (UFC5StatKey.BOTTOM_GAME, 0.30),
        (UFC5StatKey.SUBMISSION_DEFENCE, 0.30),
    ],
    UFC5PerkKey.TO_YOUR_FEET: [
        (UFC5StatKey.BOTTOM_GAME, 0.45),
        (UFC5StatKey.TAKEDOWN_DEFENCE, 0.35),
        (UFC5StatKey.CARDIO, 0.20),
    ],
    UFC5PerkKey.WRESTLE_CLINIC: [
        (UFC5StatKey.TOP_GAME, 0.45),
        (UFC5StatKey.TAKEDOWNS, 0.30),
        (UFC5StatKey.GROUND_STRIKING, 0.25),
    ],
    UFC5PerkKey.GRINDER: [(UFC5StatKey.CARDIO, 0.40), (UFC5StatKey.CLINCH_GRAPPLE, 0.30), (UFC5StatKey.TOP_GAME, 0.30)],
    UFC5PerkKey.WORK_HORSE: [
        (UFC5StatKey.CARDIO, 0.40),
        (UFC5StatKey.TOP_GAME, 0.35),
        (UFC5StatKey.GROUND_STRIKING, 0.25),
    ],
    UFC5PerkKey.MARATHONER: [(UFC5StatKey.CARDIO, 0.55), (UFC5StatKey.RECOVERY, 0.25), (UFC5StatKey.FOOTWORK, 0.20)],
    UFC5PerkKey.OUT_THE_GATES: [
        (UFC5StatKey.PUNCH_POWER, 0.35),
        (UFC5StatKey.PUNCH_SPEED, 0.30),
        (UFC5StatKey.ACCURACY, 0.20),
        (UFC5StatKey.KICK_POWER, 0.15),
    ],
    UFC5PerkKey.HIGHER_ALTITUDE: [
        (UFC5StatKey.CARDIO, 0.60),
        (UFC5StatKey.RECOVERY, 0.25),
        (UFC5StatKey.BLOCKING, 0.15),
    ],
    UFC5PerkKey.WAKE_UP_CALL: [
        (UFC5StatKey.RECOVERY, 0.50),
        (UFC5StatKey.CHIN_STRENGTH, 0.30),
        (UFC5StatKey.CARDIO, 0.20),
    ],
    UFC5PerkKey.CRAFTY: [
        (UFC5StatKey.SUBMISSION_DEFENCE, 0.45),
        (UFC5StatKey.BOTTOM_GAME, 0.30),
        (UFC5StatKey.TAKEDOWN_DEFENCE, 0.25),
    ],
    UFC5PerkKey.NO_CIGAR: [
        (UFC5StatKey.SUBMISSION_DEFENCE, 0.50),
        (UFC5StatKey.CHIN_STRENGTH, 0.25),
        (UFC5StatKey.RECOVERY, 0.25),
    ],
}

# Perk strength tier assignments
_PERK_STRENGTH_TIER: dict[UFC5PerkKey, str] = {
    UFC5PerkKey.LIKE_GLUE: 'medium',
    UFC5PerkKey.SLIPPERY: 'medium',
    UFC5PerkKey.TO_YOUR_FEET: 'medium',
    UFC5PerkKey.WRESTLE_CLINIC: 'major',
    UFC5PerkKey.GRINDER: 'medium',
    UFC5PerkKey.WORK_HORSE: 'medium',
    UFC5PerkKey.MARATHONER: 'medium',
    UFC5PerkKey.OUT_THE_GATES: 'minor',
    UFC5PerkKey.HIGHER_ALTITUDE: 'medium',
    UFC5PerkKey.WAKE_UP_CALL: 'minor',
    UFC5PerkKey.CRAFTY: 'medium',
    UFC5PerkKey.NO_CIGAR: 'medium',
}


# ---------------------------------------------------------------------------
# Stat-to-feature mapping (weighted percentile formulas from plan)
# ---------------------------------------------------------------------------

# Each stat maps to a list of (feature_key, weight, inverted, shrinkage_tau)
# 'inverted' means lower values are better (use inv_percentile)

StatFeatureSpec = tuple[str, float, bool, float]

_STAT_FEATURES: dict[UFC5StatKey, list[StatFeatureSpec]] = {
    # ---- Direct / near-direct ----
    UFC5StatKey.ACCURACY: [('sig_acc', 1.0, False, 30.0)],
    UFC5StatKey.BLOCKING: [('sig_def', 0.70, False, 30.0), ('head_evasion', 0.30, False, 30.0)],
    UFC5StatKey.HEAD_MOVEMENT: [
        ('head_evasion', 0.60, False, 30.0),
        ('sig_def', 0.25, False, 30.0),
        ('kd_abs15', 0.15, True, 20.0),
    ],
    UFC5StatKey.TAKEDOWNS: [
        ('td15', 0.45, False, 15.0),
        ('td_acc', 0.35, False, 15.0),
        ('ctrl_share', 0.20, False, 20.0),
    ],
    UFC5StatKey.TAKEDOWN_DEFENCE: [
        ('td_def', 0.45, False, 15.0),
        ('opp_ctrl_share', 0.25, True, 20.0),
        ('rev15', 0.20, False, 15.0),
        ('opp_sub15', 0.10, True, 10.0),
    ],
    UFC5StatKey.SUBMISSION_OFFENCE: [
        ('sub15', 0.35, False, 10.0),
        ('sub_win_rate', 0.25, False, 10.0),
        ('ctrl_share', 0.20, False, 20.0),
        ('ground_sig_per_min', 0.20, False, 20.0),
    ],
    UFC5StatKey.TOP_GAME: [
        ('ctrl_share', 0.45, False, 20.0),
        ('ground_sig_per_min', 0.25, False, 20.0),
        ('sub15', 0.20, False, 10.0),
        ('opp_rev15', 0.10, True, 15.0),
    ],
    UFC5StatKey.GROUND_STRIKING: [
        ('ground_sig_per_min', 0.50, False, 20.0),
        ('ctrl_share', 0.30, False, 20.0),
        ('ko_win_rate', 0.20, False, 20.0),
    ],
    UFC5StatKey.CARDIO: [
        ('late_output_ratio', 0.40, False, 20.0),
        ('late_abs_ratio', 0.25, True, 20.0),
        ('total_fight_minutes', 0.20, False, 20.0),
        ('slpm', 0.15, False, 30.0),
    ],
    # ---- Strong proxies ----
    UFC5StatKey.FOOTWORK: [
        ('dist_share', 0.35, False, 30.0),
        ('str_diff', 0.25, False, 30.0),
        ('sig_def', 0.20, False, 30.0),
        ('late_output_ratio', 0.20, False, 20.0),
    ],
    UFC5StatKey.PUNCH_SPEED: [
        ('head_sig_per_min', 0.40, False, 30.0),
        ('sig_acc', 0.25, False, 30.0),
        ('dist_sig_per_min', 0.20, False, 30.0),
        ('sapm', 0.15, True, 30.0),
    ],
    UFC5StatKey.PUNCH_POWER: [
        ('kd15', 0.35, False, 20.0),
        ('ko_win_rate', 0.30, False, 20.0),
        ('head_share', 0.20, False, 30.0),
        ('head_sig_per_min', 0.15, False, 30.0),
    ],
    UFC5StatKey.KICK_SPEED: [
        ('leg_sig_per_min', 0.40, False, 30.0),
        ('leg_share', 0.25, False, 30.0),
        ('dist_sig_per_min', 0.20, False, 30.0),
        ('late_output_ratio', 0.15, False, 20.0),
    ],
    UFC5StatKey.KICK_POWER: [
        ('kd15', 0.30, False, 20.0),
        ('leg_share', 0.25, False, 30.0),
        ('body_share', 0.20, False, 30.0),
        ('body_sig_per_min', 0.15, False, 30.0),
        ('leg_sig_per_min', 0.10, False, 30.0),
    ],
    UFC5StatKey.BOTTOM_GAME: [
        ('rev15', 0.35, False, 15.0),
        ('opp_ctrl_share', 0.25, True, 20.0),
        ('sub15', 0.20, False, 10.0),
        ('td_def', 0.20, False, 15.0),
    ],
    UFC5StatKey.CLINCH_GRAPPLE: [
        ('clinch_share', 0.35, False, 30.0),
        ('td15', 0.25, False, 15.0),
        ('td_acc', 0.20, False, 15.0),
        ('ctrl_share', 0.20, False, 20.0),
    ],
    UFC5StatKey.CLINCH_STRIKING: [
        ('clinch_sig_per_min', 0.55, False, 30.0),
        ('clinch_share', 0.25, False, 30.0),
        ('sig_acc', 0.20, False, 30.0),
    ],
    UFC5StatKey.SUBMISSION_DEFENCE: [
        ('opp_sub15', 0.45, True, 10.0),
        ('td_def', 0.25, False, 15.0),
        ('opp_ctrl_share', 0.20, True, 20.0),
        ('rev15', 0.10, False, 15.0),
    ],
    UFC5StatKey.CHIN_STRENGTH: [
        ('kd_abs15', 0.45, True, 20.0),
        ('ko_win_rate', 0.15, False, 20.0),  # proxy: good chin → stays to win
        ('head_abs15', 0.15, True, 20.0),
        ('kd_survival_rate', 0.25, False, 20.0),
    ],
    UFC5StatKey.BODY_STRENGTH: [
        ('body_abs15', 0.50, True, 20.0),
        ('late_output_ratio', 0.25, False, 20.0),
        ('total_fight_minutes', 0.15, False, 20.0),
        ('kd_survival_rate', 0.10, False, 20.0),
    ],
    UFC5StatKey.LEGS_STRENGTH: [
        ('leg_abs15', 0.50, True, 20.0),
        ('dist_share', 0.20, False, 30.0),
        ('late_output_ratio', 0.15, False, 20.0),
        ('total_fight_minutes', 0.15, False, 20.0),
    ],
    UFC5StatKey.RECOVERY: [
        ('kd_survival_rate', 0.40, False, 20.0),
        ('late_output_ratio', 0.25, False, 20.0),
        ('total_fight_minutes', 0.20, False, 20.0),
        ('kd_abs15', 0.15, True, 20.0),
    ],
}


# ---------------------------------------------------------------------------
# Public system class
# ---------------------------------------------------------------------------


class UFC5StatsSystem:
    """System H: progressive UFC 5-style stat builder."""

    def __init__(self, config: UFC5StatsConfig | None = None) -> None:
        self._cfg = config or DEFAULT_UFC5_CONFIG

    # ------------------------------------------------------------------
    # Main update interface
    # ------------------------------------------------------------------

    def update_bout(
        self,
        state_a: UFC5FighterState,
        state_b: UFC5FighterState,
        evidence: BoutEvidence,
        features_a: UFC5BoutFeatures | None,
        features_b: UFC5BoutFeatures | None,
        pool: DivisionPercentilePool,
        *,
        division_id_a: str | None = None,
        division_id_b: str | None = None,
    ) -> tuple[UFC5FighterState, UFC5FighterState, UFC5StatDelta, UFC5StatDelta]:
        """Process one bout for both fighters.

        Returns (post_a, post_b, delta_a, delta_b).
        """
        # Skip no-contests entirely
        if evidence.outcome_for_a == BoutOutcome.NO_CONTEST:
            delta_a = self._no_change_delta(evidence, state_a)
            delta_b = self._no_change_delta(evidence, state_b)
            return state_a, state_b, delta_a, delta_b

        sport = evidence.sport_key.lower()
        is_mma = sport == 'mma'
        allowed_stats = self._allowed_stats(sport)
        tier = evidence.tier
        org_w = self._org_weight(evidence.promotion_strength, sport)

        params_a = _BoutUpdateParams(
            division_id=division_id_a,
            opp_overall=self.compute_effective_overall(state_b),
            tier=tier,
            org_weight=org_w,
            allowed_stats=allowed_stats,
            is_winner=evidence.outcome_for_a == BoutOutcome.WIN,
            is_mma=is_mma,
        )

        opp_outcome = _flip_outcome(evidence.outcome_for_a)
        params_b = _BoutUpdateParams(
            division_id=division_id_b,
            opp_overall=self.compute_effective_overall(state_a),
            tier=tier,
            org_weight=org_w,
            allowed_stats=allowed_stats,
            is_winner=opp_outcome == BoutOutcome.WIN,
            is_mma=is_mma,
        )

        # Update fighter A
        pre_overall_a = self.compute_effective_overall(state_a)
        post_a, stat_deltas_a, perk_deltas_a = self._update_single_fighter(
            state_a, features_a, evidence, pool, params_a
        )
        post_overall_a = self.compute_effective_overall(post_a)

        # Update fighter B
        pre_overall_b = self.compute_effective_overall(state_b)
        post_b, stat_deltas_b, perk_deltas_b = self._update_single_fighter(
            state_b, features_b, evidence, pool, params_b
        )
        post_overall_b = self.compute_effective_overall(post_b)

        delta_a = UFC5StatDelta(
            bout_id=evidence.bout_id,
            fighter_id=state_a.fighter_id,
            pre_overall=pre_overall_a,
            post_overall=post_overall_a,
            delta_overall=post_overall_a - pre_overall_a,
            tier_used=tier,
            stat_deltas=stat_deltas_a,
            perk_deltas=perk_deltas_a,
        )
        delta_b = UFC5StatDelta(
            bout_id=evidence.bout_id,
            fighter_id=state_b.fighter_id,
            pre_overall=pre_overall_b,
            post_overall=post_overall_b,
            delta_overall=post_overall_b - pre_overall_b,
            tier_used=tier,
            stat_deltas=stat_deltas_b,
            perk_deltas=perk_deltas_b,
        )
        return post_a, post_b, delta_a, delta_b

    # ------------------------------------------------------------------
    # Overall computation
    # ------------------------------------------------------------------

    def compute_base_overall(self, state: UFC5FighterState) -> float:
        """Weighted geometric mean across standup/grappling/health buckets."""
        standup = self._bucket_geo_mean(state, STANDUP_WEIGHTS)
        grappling = self._bucket_geo_mean(state, GRAPPLING_WEIGHTS)
        health = self._bucket_geo_mean(state, HEALTH_WEIGHTS)

        raw = self._cfg.w_standup * standup + self._cfg.w_grappling * grappling + self._cfg.w_health * health
        return _L_DEFAULT + (_U_DEFAULT - _L_DEFAULT) * raw

    def compute_synergy_bonus(self, state: UFC5FighterState) -> float:
        """Pairwise stat synergy bonus, capped."""
        total = 0.0
        for key_a, key_b, coeff in _SYNERGY_PAIRS:
            n_a = self._normalise_stat(state.stats.get(key_a, 85.0), key_a)
            n_b = self._normalise_stat(state.stats.get(key_b, 85.0), key_b)
            total += coeff * n_a * n_b
        return min(total, self._cfg.synergy_cap)

    def compute_perk_bonus(self, state: UFC5FighterState) -> float:
        """Sum of active perk values."""
        total = 0.0
        for perk_key in UFC5PerkKey:
            if not state.perk_active.get(perk_key, False):
                continue
            tier = _PERK_STRENGTH_TIER.get(perk_key, 'minor')
            if tier == 'major':
                total += self._cfg.perk_strength_major
            elif tier == 'medium':
                total += self._cfg.perk_strength_medium
            else:
                total += self._cfg.perk_strength_minor
        return total

    def compute_effective_overall(self, state: UFC5FighterState) -> float:
        """Full 3-layer overall: Base + Synergy + Perks."""
        base = self.compute_base_overall(state)
        synergy = self.compute_synergy_bonus(state)
        perks = self.compute_perk_bonus(state)
        return clip(base + synergy + perks, _L_DEFAULT, _U_DEFAULT)

    def compute_bucket_scores(self, state: UFC5FighterState) -> dict[UFC5StatBucket, float]:
        """Return sub-scores for each bucket on the 80-100 scale."""
        standup_raw = self._bucket_geo_mean(state, STANDUP_WEIGHTS)
        grappling_raw = self._bucket_geo_mean(state, GRAPPLING_WEIGHTS)
        health_raw = self._bucket_geo_mean(state, HEALTH_WEIGHTS)
        return {
            UFC5StatBucket.STANDUP: _L_DEFAULT + (_U_DEFAULT - _L_DEFAULT) * standup_raw,
            UFC5StatBucket.GRAPPLING: _L_DEFAULT + (_U_DEFAULT - _L_DEFAULT) * grappling_raw,
            UFC5StatBucket.HEALTH: _L_DEFAULT + (_U_DEFAULT - _L_DEFAULT) * health_raw,
        }

    # ------------------------------------------------------------------
    # Internals — single-fighter update
    # ------------------------------------------------------------------

    def _update_single_fighter(
        self,
        state: UFC5FighterState,
        features: UFC5BoutFeatures | None,
        evidence: BoutEvidence,
        pool: DivisionPercentilePool,
        params: _BoutUpdateParams,
    ) -> tuple[UFC5FighterState, dict[str, dict[str, float]], dict[str, dict[str, float]]]:
        """Update one fighter's state from one bout."""
        cfg = self._cfg

        tier_mult = self._tier_mult(params.tier)
        k_career = 1.0 / math.sqrt(1.0 + state.mma_fight_count / cfg.k_career_divisor)
        div_avg = cfg.default_division_avg_overall
        quality_w = cfg.quality_offset + cfg.quality_scale * logistic(params.opp_overall - div_avg)

        org_weight = params.org_weight
        if not params.is_mma:
            org_weight *= math.exp(-state.mma_fight_count / cfg.prior_decay_lambda)

        targets = self._compute_stat_targets(features, state, pool, params.division_id) if features is not None else {}

        k_mult = k_career * tier_mult * org_weight * quality_w
        new_stats, stat_deltas = self._apply_stat_updates(
            state, targets, params.allowed_stats, k_mult=k_mult, evidence=evidence, is_winner=params.is_winner
        )

        acc = self._compute_career_accumulators(state, features, evidence, params)

        perk_result = self._update_perk_states(state, new_stats)

        post = UFC5FighterState(
            fighter_id=state.fighter_id,
            stats=new_stats,
            mma_fight_count=acc.mma_fight_count,
            tier_a_fight_count=acc.tier_a_fight_count,
            division_id=params.division_id or state.division_id,
            perk_scores=perk_result.scores,
            perk_active=perk_result.active,
            perk_consecutive_above=perk_result.consecutive_above,
            perk_consecutive_below=perk_result.consecutive_below,
            total_fights=acc.total_fights,
            total_wins=acc.total_wins,
            total_ko_wins=acc.total_ko_wins,
            total_sub_wins=acc.total_sub_wins,
            total_knockdowns_scored=acc.total_knockdowns_scored,
            total_knockdowns_absorbed=acc.total_knockdowns_absorbed,
            total_kd_events_survived=acc.total_kd_events_survived,
            total_fight_minutes=acc.total_fight_minutes,
            total_rounds_fought=acc.total_rounds_fought,
        )
        return post, stat_deltas, perk_result.deltas

    def _apply_stat_updates(
        self,
        state: UFC5FighterState,
        targets: dict[UFC5StatKey, float],
        allowed_stats: frozenset[UFC5StatKey],
        *,
        k_mult: float,
        evidence: BoutEvidence,
        is_winner: bool,
    ) -> tuple[dict[UFC5StatKey, float], dict[str, dict[str, float]]]:
        """Apply stat updates for each allowed stat key.

        Returns (updated_stats, stat_deltas).
        """
        new_stats = dict(state.stats)
        stat_deltas: dict[str, dict[str, float]] = {}
        for stat_key in UFC5StatKey:
            if stat_key not in allowed_stats:
                continue
            old_val = state.stats.get(stat_key, 85.0)
            target = targets.get(stat_key)
            if target is None:
                stat_deltas[stat_key.value] = {'pre': old_val, 'target': old_val, 'post': old_val, 'delta': 0.0}
                continue
            k_eff = self._k_base(stat_key) * k_mult
            method_bonus = self._method_bonus(stat_key, evidence.method, is_winner)
            raw_delta = k_eff * (target - old_val) + method_bonus
            cap = self._stat_cap(stat_key, raw_delta)
            capped_delta = clip(raw_delta, -cap, cap)
            lo, hi = self._stat_bounds(stat_key)
            new_val = clip(old_val + capped_delta, lo, hi)
            new_stats[stat_key] = new_val
            stat_deltas[stat_key.value] = {
                'pre': old_val,
                'target': target,
                'post': new_val,
                'delta': new_val - old_val,
            }
        return new_stats, stat_deltas

    @staticmethod
    def _compute_career_accumulators(
        state: UFC5FighterState, features: UFC5BoutFeatures | None, evidence: BoutEvidence, params: _BoutUpdateParams
    ) -> _CareerAccumulators:
        """Compute updated career accumulators from the bout."""
        fight_mins = features.fight_minutes if features else 0.0
        ko_wins = state.total_ko_wins
        sub_wins = state.total_sub_wins
        if params.is_winner:
            if evidence.method in {FinishMethod.KO, FinishMethod.TKO}:
                ko_wins += 1
            elif evidence.method == FinishMethod.SUB:
                sub_wins += 1

        kd_survived = state.total_kd_events_survived
        if features and features.kd_abs15 > 0 and params.is_winner:
            kd_survived += features.kd_abs15 * fight_mins / 15.0

        return _CareerAccumulators(
            mma_fight_count=state.mma_fight_count + (1 if params.is_mma else 0),
            tier_a_fight_count=state.tier_a_fight_count + (1 if params.tier == EvidenceTier.A else 0),
            total_fights=state.total_fights + 1,
            total_wins=state.total_wins + (1 if params.is_winner else 0),
            total_ko_wins=ko_wins,
            total_sub_wins=sub_wins,
            total_knockdowns_scored=state.total_knockdowns_scored
            + (features.kd15 * fight_mins / 15.0 if features else 0.0),
            total_knockdowns_absorbed=state.total_knockdowns_absorbed
            + (features.kd_abs15 * fight_mins / 15.0 if features else 0.0),
            total_kd_events_survived=kd_survived,
            total_fight_minutes=state.total_fight_minutes + fight_mins,
            total_rounds_fought=state.total_rounds_fought
            + (int(fight_mins / 5.0) + 1 if features and fight_mins > 0 else 0),
        )

    def _update_perk_states(self, state: UFC5FighterState, new_stats: dict[UFC5StatKey, float]) -> _PerkResult:
        """Compute updated perk scores, activation states, and deltas."""
        cfg = self._cfg
        scores = dict(state.perk_scores)
        active = dict(state.perk_active)
        above = dict(state.perk_consecutive_above)
        below = dict(state.perk_consecutive_below)
        deltas: dict[str, dict[str, float]] = {}

        for perk_key in UFC5PerkKey:
            old_score = state.perk_scores.get(perk_key, 80.0)
            evidence_val = self._compute_perk_evidence(perk_key, new_stats)
            new_score = cfg.perk_ema_retain * old_score + cfg.perk_ema_alpha * evidence_val
            scores[perk_key] = new_score

            was_active = state.perk_active.get(perk_key, False)
            if new_score >= cfg.perk_activation:
                above[perk_key] = state.perk_consecutive_above.get(perk_key, 0) + 1
                below[perk_key] = 0
            elif new_score < cfg.perk_deactivation:
                below[perk_key] = state.perk_consecutive_below.get(perk_key, 0) + 1
                above[perk_key] = 0
            elif was_active:
                above[perk_key] = state.perk_consecutive_above.get(perk_key, 0) + 1
                below[perk_key] = 0
            else:
                above[perk_key] = 0
                below[perk_key] = 0

            if above[perk_key] >= cfg.perk_consecutive_required:
                active[perk_key] = True
            elif below[perk_key] >= cfg.perk_consecutive_required:
                active[perk_key] = False
            else:
                active[perk_key] = was_active

            deltas[perk_key.value] = {'pre': old_score, 'evidence': evidence_val, 'post': new_score}

        return _PerkResult(
            scores=scores, active=active, consecutive_above=above, consecutive_below=below, deltas=deltas
        )

    # ------------------------------------------------------------------
    # Target computation
    # ------------------------------------------------------------------

    def _compute_stat_targets(
        self, features: UFC5BoutFeatures, state: UFC5FighterState, pool: DivisionPercentilePool, division_id: str | None
    ) -> dict[UFC5StatKey, float]:
        """For each stat, compute a target on the [L, U] scale from features + percentiles."""
        # Inject career-level features into the bout features object
        features = self._enrich_features(features, state)

        targets: dict[UFC5StatKey, float] = {}
        for stat_key, feature_specs in _STAT_FEATURES.items():
            weighted_pct = 0.0
            total_weight = 0.0
            has_data = False

            for fk, weight, inverted, tau in feature_specs:
                raw_val = getattr(features, fk, None)
                if raw_val is None:
                    continue

                has_data = True
                # Shrink toward division mean
                div_mean = pool.division_mean(division_id, fk)
                sample_size = float(state.tier_a_fight_count)
                shrunk = shrink(raw_val, sample_size, tau, div_mean)

                # Get percentile
                if inverted:
                    pct = pool.inv_percentile(division_id, fk, shrunk)
                else:
                    pct = pool.percentile(division_id, fk, shrunk)

                weighted_pct += weight * pct
                total_weight += weight

            if not has_data or total_weight < _EPS:
                continue

            normalised_pct = weighted_pct / total_weight
            lo, hi = self._stat_bounds(stat_key)
            targets[stat_key] = round(lo + (hi - lo) * normalised_pct, 2)

        return targets

    def _enrich_features(self, features: UFC5BoutFeatures, state: UFC5FighterState) -> UFC5BoutFeatures:
        """Inject career-level aggregates into bout features."""
        total_f = max(state.total_fights, 1)
        total_mins = max(state.total_fight_minutes, _EPS)

        ko_wr = state.total_ko_wins / total_f
        sub_wr = state.total_sub_wins / total_f
        kd_survival = 1.0
        if state.total_knockdowns_absorbed > 0:
            kd_survival = state.total_kd_events_survived / state.total_knockdowns_absorbed

        return UFC5BoutFeatures(
            fight_minutes=features.fight_minutes,
            sig_acc=features.sig_acc,
            sig_def=features.sig_def,
            head_evasion=features.head_evasion,
            slpm=features.slpm,
            sapm=features.sapm,
            str_diff=features.str_diff,
            td15=features.td15,
            td_acc=features.td_acc,
            td_def=features.td_def,
            sub15=features.sub15,
            opp_sub15=features.opp_sub15,
            ctrl_share=features.ctrl_share,
            opp_ctrl_share=features.opp_ctrl_share,
            rev15=features.rev15,
            opp_rev15=features.opp_rev15,
            head_share=features.head_share,
            body_share=features.body_share,
            leg_share=features.leg_share,
            dist_share=features.dist_share,
            clinch_share=features.clinch_share,
            ground_share=features.ground_share,
            head_sig_per_min=features.head_sig_per_min,
            body_sig_per_min=features.body_sig_per_min,
            leg_sig_per_min=features.leg_sig_per_min,
            dist_sig_per_min=features.dist_sig_per_min,
            clinch_sig_per_min=features.clinch_sig_per_min,
            ground_sig_per_min=features.ground_sig_per_min,
            kd15=features.kd15,
            kd_abs15=features.kd_abs15,
            head_abs15=features.head_abs15,
            body_abs15=features.body_abs15,
            leg_abs15=features.leg_abs15,
            late_output_ratio=features.late_output_ratio,
            late_abs_ratio=features.late_abs_ratio,
            early_output_ratio=features.early_output_ratio,
            ko_win_rate=ko_wr,
            sub_win_rate=sub_wr,
            total_fight_minutes=total_mins,
            kd_survival_rate=kd_survival,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalise_stat(self, value: float, stat_key: UFC5StatKey) -> float:
        """Normalise a stat to [0, 1]."""
        lo, hi = self._stat_bounds(stat_key)
        return clip((value - lo) / max(hi - lo, _EPS), 0.0, 1.0)

    def _stat_bounds(self, stat_key: UFC5StatKey) -> tuple[float, float]:
        if stat_key in _HEALTH_STATS:
            return self._cfg.L_health, self._cfg.U_health
        return self._cfg.L_default, self._cfg.U_default

    def _k_base(self, stat_key: UFC5StatKey) -> float:
        if stat_key in _DIRECT_STATS:
            return self._cfg.k_base_direct
        if stat_key in _HEALTH_STATS:
            return self._cfg.k_base_health
        return self._cfg.k_base_proxy

    def _stat_cap(self, stat_key: UFC5StatKey, delta: float) -> float:
        """Per-fight movement cap. Extreme damage events allow larger drops."""
        if stat_key in _HEALTH_STATS and delta < 0:
            return self._cfg.cap_extreme_down
        if stat_key in _DIRECT_STATS:
            return self._cfg.cap_direct
        if stat_key in _HEALTH_STATS:
            return self._cfg.cap_health
        return self._cfg.cap_proxy

    def _tier_mult(self, tier: EvidenceTier) -> float:
        if tier == EvidenceTier.A:
            return self._cfg.tier_a_mult
        if tier == EvidenceTier.B:
            return self._cfg.tier_b_mult
        return self._cfg.tier_c_mult

    def _org_weight(self, promotion_strength: float | None, sport: str) -> float:
        """Determine org weight from promotion strength and sport type."""
        cfg = self._cfg
        if sport == 'mma':
            if promotion_strength is None:
                return cfg.org_weight_regional_mma
            if promotion_strength >= _PROMO_STRENGTH_UFC:
                return cfg.org_weight_ufc_mma
            if promotion_strength >= _PROMO_STRENGTH_MAJOR:
                return cfg.org_weight_major_mma
            return cfg.org_weight_regional_mma
        # Non-MMA sports
        sport_weights = {
            'boxing': cfg.org_weight_boxing,
            'boxing_cage': cfg.org_weight_boxing,
            'kickboxing': cfg.org_weight_kickboxing,
            'muay': cfg.org_weight_kickboxing,
            'lethwei': cfg.org_weight_kickboxing,
            'wrestling': cfg.org_weight_wrestling,
            'judo': cfg.org_weight_wrestling,
            'sambo': cfg.org_weight_wrestling,
            'grappling': cfg.org_weight_bjj,
            'combat_jj': cfg.org_weight_bjj,
        }
        return sport_weights.get(sport, cfg.org_weight_regional_mma * 0.5)

    def _allowed_stats(self, sport: str) -> frozenset[UFC5StatKey]:
        """Which stats may be updated for this sport type."""
        if sport == 'mma':
            return frozenset(UFC5StatKey)
        return _SPORT_STAT_FAMILIES.get(sport, frozenset())

    def _method_bonus(self, stat_key: UFC5StatKey, method: FinishMethod, is_winner: bool) -> float:
        """Small bonus for stats strongly tied to the finish method."""
        if not is_winner:
            return 0.0
        cfg = self._cfg
        if method in {FinishMethod.KO, FinishMethod.TKO}:
            if stat_key == UFC5StatKey.PUNCH_POWER:
                return cfg.ko_power_bonus
            if stat_key == UFC5StatKey.KICK_POWER:
                return cfg.ko_power_bonus * 0.5
        elif method == FinishMethod.SUB:
            if stat_key == UFC5StatKey.SUBMISSION_OFFENCE:
                return cfg.sub_offence_bonus
            if stat_key in {UFC5StatKey.TOP_GAME, UFC5StatKey.BOTTOM_GAME}:
                return cfg.sub_ground_bonus
        return 0.0

    def _bucket_geo_mean(self, state: UFC5FighterState, weights: dict[UFC5StatKey, float]) -> float:
        """Weighted geometric mean over stats in a bucket, on [0, 1]."""
        eps = self._cfg.geo_eps
        log_sum = 0.0
        total_w = 0.0
        for stat_key, w in weights.items():
            normed = self._normalise_stat(state.stats.get(stat_key, 85.0), stat_key)
            log_sum += w * math.log(eps + normed)
            total_w += w
        if total_w < _EPS:
            return 0.5
        return math.exp(log_sum / total_w)

    def _compute_perk_evidence(self, perk_key: UFC5PerkKey, stats: dict[UFC5StatKey, float]) -> float:
        """Compute perk evidence from current stats (0-100 scale)."""
        formula = _PERK_FORMULAS.get(perk_key)
        if formula is None:
            return 80.0
        total = 0.0
        for stat_key, weight in formula:
            val = stats.get(stat_key, 85.0)
            total += weight * val
        return total

    def _no_change_delta(self, evidence: BoutEvidence, state: UFC5FighterState) -> UFC5StatDelta:
        overall = self.compute_effective_overall(state)
        return UFC5StatDelta(
            bout_id=evidence.bout_id,
            fighter_id=state.fighter_id,
            pre_overall=overall,
            post_overall=overall,
            delta_overall=0.0,
            tier_used=evidence.tier,
            stat_deltas={},
            perk_deltas={},
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def new_ufc5_state(fighter_id: str) -> UFC5FighterState:
    """Create a fresh System H fighter state with defaults."""
    return UFC5FighterState(fighter_id=fighter_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _flip_outcome(outcome: BoutOutcome) -> BoutOutcome:
    if outcome == BoutOutcome.WIN:
        return BoutOutcome.LOSS
    if outcome == BoutOutcome.LOSS:
        return BoutOutcome.WIN
    return outcome
