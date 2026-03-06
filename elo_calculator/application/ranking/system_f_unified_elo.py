"""System F — Unified Composite Elo.

A single rating system that synthesises every signal from Systems A-E:

* **Elo backbone** (System A) — a single interpretable rating centred at 1500.
* **Uncertainty + volatility tracking** (System B / Glicko-2) — RD grows with
  inactivity and shrinks after each fight; volatility adapts to inconsistent
  fighters.
* **Multi-dimensional latent skills** (System C / EKF) — MMA, striking,
  grappling, durability, *and* a new **cardio** dimension.  Cross-sport bouts
  update the appropriate skill channel.
* **Adaptive K-factor** (System A enhanced) — tier, rounds, method, PS margin,
  experience, recency, promotion, **plus** upset bonus, opponent quality, and
  consistency.
* **Performance-adjusted targets** (PS + target system) — full PS target
  scoring with method and finish-time bonuses.
* **Momentum tracking** — win/loss streak feeds back into probability
  estimation and K-factor.
* **Consistency signal** — rolling PS history penalises wild swings.
* **Peak-rating memory** — fighters who once reached elite level retain a floor.

The final "composite rating" blends the Elo backbone and the skill-implied MMA
rating, weighted by certainty.  This gives a single number suitable for
leaderboard ranking that is driven by *everything* the data contains.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date

import numpy as np
from numpy.typing import NDArray

from elo_calculator.application.ranking.math_utils import clip, logistic
from elo_calculator.application.ranking.targets import DEFAULT_TARGET_CONFIG, TargetScoreConfig, compute_target_score
from elo_calculator.application.ranking.types import BoutEvidence, BoutOutcome, EvidenceTier, FinishMethod, RatingDelta

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SKILL_DIM = 5  # mma, striking, grappling, durability, cardio

_GLICKO_SCALE = 173.7178
_PI_SQ = math.pi**2

# Default rolling PS window size for consistency measurement
_PS_HISTORY_MAX = 10
_DRAW_TARGET = 0.5
_MIN_PS_HISTORY_STD = 2
_MIN_PS_HISTORY_CONSISTENCY = 3


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class UnifiedEloConfig:
    """All tuneable knobs for System F live here."""

    # --- Elo backbone ---
    base_rating: float = 1500.0
    rating_scale: float = 400.0

    # --- Glicko-2 uncertainty layer ---
    base_rd: float = 300.0
    base_volatility: float = 0.06
    tau: float = 0.5
    convergence_eps: float = 1e-6
    rating_period_days: float = 30.0
    inactivity_rd_growth: float = 0.05
    rd_floor: float = 30.0
    rd_ceiling: float = 350.0

    # --- Adaptive K-factor (System A enhanced) ---
    k0: float = 28.0
    tier_a_mult: float = 1.00
    tier_b_mult: float = 0.85
    tier_c_mult: float = 0.70
    title_rounds_mult: float = 1.10
    regular_rounds_mult: float = 1.00
    finish_method_mult: float = 1.08
    decision_method_mult: float = 1.00
    ps_margin_k_sens: float = 0.45
    ps_margin_k_cap: float = 0.40
    early_career_mult: float = 1.20
    established_mult: float = 1.00
    veteran_mult: float = 0.90
    active_recency_mult: float = 1.05
    layoff_recency_mult: float = 0.88
    promo_min_mult: float = 0.75
    promo_max_mult: float = 1.12
    title_round_threshold: int = 5
    early_career_fights: int = 5
    established_fights: int = 12
    layoff_days: int = 540

    # --- New K-factor extras ---
    upset_bonus_scale: float = 0.30  # extra K when underdog wins
    upset_bonus_cap: float = 0.40
    opponent_quality_scale: float = 0.15  # scale boost when beating high-rated
    opponent_quality_centre: float = 1500.0
    consistency_bonus_scale: float = 0.10  # bonus for consistent performers

    # --- Skill latent EKF (extended System C) ---
    process_noise_mma: float = 0.0007
    process_noise_striking: float = 0.0010
    process_noise_grappling: float = 0.0010
    process_noise_durability: float = 0.0008
    process_noise_cardio: float = 0.0009
    initial_var_mma: float = 1.0
    initial_var_striking: float = 1.2
    initial_var_grappling: float = 1.2
    initial_var_durability: float = 1.0
    initial_var_cardio: float = 1.1
    beta_ps_margin: float = 0.65
    beta_promotion: float = 0.20
    gamma_damage: float = 0.35
    eta_latent: float = 0.30
    finish_intensity_weight: float = 0.05
    obs_var_floor: float = 0.02

    # --- Probability blending ---
    elo_weight: float = 0.45  # weight for Elo probability
    skill_weight: float = 0.40  # weight for skill-implied probability
    momentum_weight: float = 0.15  # weight for momentum signal
    momentum_per_streak: float = 0.04  # win-prob shift per streak fight
    momentum_cap: float = 0.12  # max momentum adjustment

    # --- Composite rating blend ---
    composite_elo_share: float = 0.55
    composite_skill_share: float = 0.45

    # --- Peak rating memory ---
    peak_decay_per_loss: float = 0.02  # how fast peak floor decays
    peak_floor_fraction: float = 0.35  # floor = base + fraction*(peak-base)

    # --- Draw / NC handling ---
    no_update_on_draw: bool = True


# ---------------------------------------------------------------------------
# Fighter state
# ---------------------------------------------------------------------------
@dataclass
class UnifiedFighterState:
    """Complete state for one fighter in System F."""

    fighter_id: str
    # Elo backbone
    rating: float = 1500.0
    rd: float = 300.0
    volatility: float = 0.06
    # Skill latent vector [mma, striking, grappling, durability, cardio]
    skill_mean: NDArray[np.float64] = field(default_factory=lambda: np.zeros(SKILL_DIM, dtype=np.float64))
    skill_cov: NDArray[np.float64] = field(
        default_factory=lambda: np.diag(np.array([1.0, 1.2, 1.2, 1.0, 1.1], dtype=np.float64))
    )
    # Metadata
    mma_fights_count: int = 0
    last_mma_date: date | None = None
    # Momentum
    win_streak: int = 0  # positive = winning streak, negative = losing streak
    # Finishing power
    total_finishes: int = 0
    total_fights_tracked: int = 0
    # Consistency (rolling PS history)
    ps_history: list[float] = field(default_factory=list)
    # Peak
    peak_rating: float = 1500.0


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------
class UnifiedCompositeEloSystem:
    """System F: one rating to rule them all."""

    def __init__(
        self, config: UnifiedEloConfig | None = None, target_config: TargetScoreConfig = DEFAULT_TARGET_CONFIG
    ) -> None:
        self._cfg = config or UnifiedEloConfig()
        self._target_cfg = target_config

    # ---- public API ----

    def expected_win_probability(
        self, fighter_a: UnifiedFighterState, fighter_b: UnifiedFighterState, evidence: BoutEvidence | None = None
    ) -> float:
        """Blended win probability using Elo, skill latent, and momentum."""
        p_elo = self._elo_prob(fighter_a.rating, fighter_b.rating)

        sport = evidence.sport_key if evidence else 'mma'
        weights = self._sport_weights(sport)
        latent_gap = float(weights @ (fighter_a.skill_mean - fighter_b.skill_mean))
        covariate = self._covariate_shift(evidence, latent_gap) if evidence else 0.0
        p_skill = logistic(latent_gap + covariate)

        momentum_a = self._momentum_signal(fighter_a.win_streak, fighter_b.win_streak)
        p_momentum = clip(0.5 + momentum_a, 0.0, 1.0)

        w_elo = self._cfg.elo_weight
        w_skill = self._cfg.skill_weight
        w_mom = self._cfg.momentum_weight
        total = w_elo + w_skill + w_mom
        p_blend = (w_elo * p_elo + w_skill * p_skill + w_mom * p_momentum) / total
        return clip(p_blend, 1e-6, 1.0 - 1e-6)

    def composite_rating(self, state: UnifiedFighterState) -> float:
        """Single number for leaderboard: blend of Elo and skill-implied rating."""
        skill_implied = self._cfg.base_rating + self._cfg.rating_scale * float(state.skill_mean[0])
        composite = self._cfg.composite_elo_share * state.rating + self._cfg.composite_skill_share * skill_implied
        # Apply peak floor only for fighters who have risen above the base —
        # otherwise newcomers can never drop below 1500.
        if state.peak_rating > self._cfg.base_rating:
            peak_floor = self._peak_floor(state)
            composite = max(composite, peak_floor)
        return composite

    def conservative_rating(self, state: UnifiedFighterState) -> float:
        """Composite rating minus uncertainty — for rankings that penalise unknowns."""
        return self.composite_rating(state) - state.rd

    def skill_implied_rating(self, state: UnifiedFighterState) -> float:
        return self._cfg.base_rating + self._cfg.rating_scale * float(state.skill_mean[0])

    def mma_uncertainty(self, state: UnifiedFighterState) -> float:
        return self._cfg.rating_scale * float(np.sqrt(max(0.0, state.skill_cov[0, 0])))

    def finish_rate(self, state: UnifiedFighterState) -> float:
        if state.total_fights_tracked == 0:
            return 0.0
        return state.total_finishes / state.total_fights_tracked

    def consistency_score(self, state: UnifiedFighterState) -> float:
        """Lower is more consistent.  Returns std-dev of recent PS scores."""
        if len(state.ps_history) < _MIN_PS_HISTORY_STD:
            return 0.0
        return float(np.std(state.ps_history))

    def update_bout(
        self, fighter_a: UnifiedFighterState, fighter_b: UnifiedFighterState, evidence: BoutEvidence
    ) -> tuple[UnifiedFighterState, UnifiedFighterState, RatingDelta, RatingDelta]:
        """Process one bout.  Returns (post_a, post_b, delta_a, delta_b)."""
        # 1) Advance uncertainty for inactivity
        advanced_a = self._advance_inactivity(fighter_a, evidence.event_date)
        advanced_b = self._advance_inactivity(fighter_b, evidence.event_date)

        # 2) Expected probability (blended)
        expected_a = self.expected_win_probability(advanced_a, advanced_b, evidence)
        expected_b = 1.0 - expected_a

        # 3) Skip conditions
        if evidence.sport_key.lower() != 'mma':
            return self._cross_sport_update(advanced_a, advanced_b, evidence, expected_a, expected_b)

        if evidence.outcome_for_a == BoutOutcome.NO_CONTEST:
            return self._no_update(advanced_a, advanced_b, evidence, expected_a, expected_b)

        if evidence.outcome_for_a == BoutOutcome.DRAW and self._cfg.no_update_on_draw:
            return self._no_update(advanced_a, advanced_b, evidence, expected_a, expected_b)

        # 4) Target score (performance-adjusted)
        target_a = compute_target_score(evidence, expected_a, self._target_cfg)
        target_b = 1.0 - target_a

        # 5) Adaptive K-factor
        k_a = self._effective_k(advanced_a, advanced_b, evidence, expected_a)
        k_b = self._effective_k(advanced_b, advanced_a, evidence, expected_b)

        # 6) Update Elo backbone with Glicko-2-style RD shrinkage
        new_rating_a, new_rd_a, new_vol_a = self._glicko_elo_update(advanced_a, advanced_b, target_a, k_a)
        new_rating_b, new_rd_b, new_vol_b = self._glicko_elo_update(advanced_b, advanced_a, target_b, k_b)

        # 7) Update skill vector via EKF
        new_skill_a, new_cov_a, new_skill_b, new_cov_b = self._ekf_skill_update(
            advanced_a, advanced_b, evidence, expected_a, target_a
        )

        # 8) Build post-states with all metadata updates
        post_a = self._build_post_state(
            advanced_a, evidence, new_rating_a, new_rd_a, new_vol_a, new_skill_a, new_cov_a, target_a
        )
        post_b = self._build_post_state(
            advanced_b, evidence, new_rating_b, new_rd_b, new_vol_b, new_skill_b, new_cov_b, target_b
        )

        delta_a = self._make_delta(evidence, advanced_a, post_a, expected_a, target_a, k_a)
        delta_b = self._make_delta(evidence, advanced_b, post_b, expected_b, target_b, k_b)
        return post_a, post_b, delta_a, delta_b

    # ---- Elo probability ----

    def _elo_prob(self, rating_a: float, rating_b: float) -> float:
        delta = (rating_a - rating_b) / self._cfg.rating_scale
        return logistic(delta)

    # ---- Momentum signal ----

    def _momentum_signal(self, streak_a: int, streak_b: int) -> float:
        raw = self._cfg.momentum_per_streak * (streak_a - streak_b)
        return clip(raw, -self._cfg.momentum_cap, self._cfg.momentum_cap)

    # ---- Peak floor ----

    def _peak_floor(self, state: UnifiedFighterState) -> float:
        base = self._cfg.base_rating
        return base + self._cfg.peak_floor_fraction * (state.peak_rating - base)

    # ---- Inactivity advancement ----

    def _advance_inactivity(self, state: UnifiedFighterState, event_date: date) -> UnifiedFighterState:
        if state.last_mma_date is None:
            return state

        days = max(0, (event_date - state.last_mma_date).days)
        if days == 0:
            return state

        # RD growth (Glicko-2 style)
        periods = days / self._cfg.rating_period_days
        mu, phi = _to_mu_phi(state.rating, state.rd)
        phi_new = math.sqrt(phi**2 + periods * self._cfg.inactivity_rd_growth**2)
        _, rd_new = _from_mu_phi(mu, phi_new)
        rd_new = clip(rd_new, self._cfg.rd_floor, self._cfg.rd_ceiling)

        # Skill covariance growth (EKF process noise)
        noise = np.diag(self._process_noise_vector(days))
        new_cov = state.skill_cov + noise

        return replace(state, rd=rd_new, skill_cov=new_cov)

    # ---- Adaptive K-factor ----

    def _effective_k(
        self, fighter: UnifiedFighterState, opponent: UnifiedFighterState, evidence: BoutEvidence, expected: float
    ) -> float:
        k = self._cfg.k0
        k *= self._tier_mult(evidence.tier)
        k *= self._rounds_mult(evidence)
        k *= self._method_mult(evidence)
        k *= self._ps_margin_mult(evidence)
        k *= self._experience_mult(fighter)
        k *= self._recency_mult(fighter, evidence.event_date)
        k *= self._promotion_mult(evidence.promotion_strength)
        k *= self._upset_mult(evidence, expected)
        k *= self._opponent_quality_mult(opponent)
        k *= self._consistency_mult(fighter)
        # Scale by RD — bigger updates when uncertain (like Glicko-2)
        rd_factor = clip(fighter.rd / self._cfg.base_rd, 0.5, 2.0)
        k *= rd_factor
        return k

    def _tier_mult(self, tier: EvidenceTier) -> float:
        if tier == EvidenceTier.A:
            return self._cfg.tier_a_mult
        if tier == EvidenceTier.B:
            return self._cfg.tier_b_mult
        return self._cfg.tier_c_mult

    def _rounds_mult(self, evidence: BoutEvidence) -> float:
        if evidence.scheduled_rounds >= self._cfg.title_round_threshold or evidence.is_title_fight:
            return self._cfg.title_rounds_mult
        return self._cfg.regular_rounds_mult

    def _method_mult(self, evidence: BoutEvidence) -> float:
        if evidence.method in {FinishMethod.KO, FinishMethod.TKO, FinishMethod.SUB}:
            return self._cfg.finish_method_mult
        return self._cfg.decision_method_mult

    def _ps_margin_mult(self, evidence: BoutEvidence) -> float:
        margin = abs(evidence.ps_margin_a or 0.0)
        boost = clip(margin * self._cfg.ps_margin_k_sens, 0.0, self._cfg.ps_margin_k_cap)
        return 1.0 + boost

    def _experience_mult(self, fighter: UnifiedFighterState) -> float:
        if fighter.mma_fights_count < self._cfg.early_career_fights:
            return self._cfg.early_career_mult
        if fighter.mma_fights_count < self._cfg.established_fights:
            return self._cfg.established_mult
        return self._cfg.veteran_mult

    def _recency_mult(self, fighter: UnifiedFighterState, event_date: date) -> float:
        if fighter.last_mma_date is None:
            return self._cfg.active_recency_mult
        days = (event_date - fighter.last_mma_date).days
        if days > self._cfg.layoff_days:
            return self._cfg.layoff_recency_mult
        return self._cfg.active_recency_mult

    def _promotion_mult(self, strength: float | None) -> float:
        if strength is None:
            return 1.0
        raw = 0.85 + 0.30 * clip(strength, 0.0, 1.0)
        return clip(raw, self._cfg.promo_min_mult, self._cfg.promo_max_mult)

    def _upset_mult(self, evidence: BoutEvidence, expected: float) -> float:
        """Bigger updates when the underdog wins."""
        if evidence.outcome_for_a == BoutOutcome.WIN and expected < _DRAW_TARGET:
            surprise = _DRAW_TARGET - expected
            bonus = clip(surprise * self._cfg.upset_bonus_scale * 2.0, 0.0, self._cfg.upset_bonus_cap)
            return 1.0 + bonus
        if evidence.outcome_for_a == BoutOutcome.LOSS and expected > _DRAW_TARGET:
            surprise = expected - _DRAW_TARGET
            bonus = clip(surprise * self._cfg.upset_bonus_scale * 2.0, 0.0, self._cfg.upset_bonus_cap)
            return 1.0 + bonus
        return 1.0

    def _opponent_quality_mult(self, opponent: UnifiedFighterState) -> float:
        """Beating a strong opponent counts more."""
        gap = (opponent.rating - self._cfg.opponent_quality_centre) / self._cfg.rating_scale
        return 1.0 + clip(gap * self._cfg.opponent_quality_scale, -0.15, 0.25)

    def _consistency_mult(self, fighter: UnifiedFighterState) -> float:
        """Consistent performers (low PS variance) get a small K bonus."""
        if len(fighter.ps_history) < _MIN_PS_HISTORY_CONSISTENCY:
            return 1.0
        std = float(np.std(fighter.ps_history))
        # Low std → bonus (up to +consistency_bonus_scale), high std → no penalty
        bonus = self._cfg.consistency_bonus_scale * max(0.0, 1.0 - std * 5.0)
        return 1.0 + bonus

    # ---- Glicko-2-style Elo update ----

    def _glicko_elo_update(
        self, fighter: UnifiedFighterState, opponent: UnifiedFighterState, target: float, k_effective: float
    ) -> tuple[float, float, float]:
        """Update rating, RD, and volatility in one step."""
        mu, phi = _to_mu_phi(fighter.rating, fighter.rd)
        mu_op, phi_op = _to_mu_phi(opponent.rating, opponent.rd)

        g_phi = _g(phi_op)
        expected = _expected_glicko(mu, mu_op, phi_op)
        variance = 1.0 / max(1e-9, g_phi**2 * expected * (1.0 - expected))

        # Innovation in Elo units, scaled by K
        delta_rating = k_effective * (target - self._elo_prob(fighter.rating, opponent.rating))
        new_rating = fighter.rating + delta_rating

        # Volatility update
        delta_glicko = variance * g_phi * (target - expected)
        new_vol = _update_volatility(
            phi=phi,
            delta=delta_glicko,
            variance=variance,
            sigma=fighter.volatility,
            tau=self._cfg.tau,
            eps=self._cfg.convergence_eps,
        )

        # RD shrinkage
        phi_star = math.sqrt(phi**2 + new_vol**2)
        phi_prime = 1.0 / math.sqrt((1.0 / phi_star**2) + (1.0 / variance))
        _, new_rd = _from_mu_phi(0.0, phi_prime)  # we only need the RD part
        new_rd = clip(new_rd, self._cfg.rd_floor, self._cfg.rd_ceiling)

        return new_rating, new_rd, new_vol

    # ---- EKF skill update (extended System C) ----

    def _ekf_skill_update(
        self,
        state_a: UnifiedFighterState,
        state_b: UnifiedFighterState,
        evidence: BoutEvidence,
        expected_a: float,
        target_a: float,
    ) -> tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64], NDArray[np.float64]]:
        """EKF update on the 5-dimensional skill vectors."""
        weights = self._sport_weights(evidence.sport_key)
        gradient_scale = expected_a * (1.0 - expected_a)
        gradient = np.concatenate((gradient_scale * weights, -gradient_scale * weights))

        joint_cov = _block_diag(state_a.skill_cov, state_b.skill_cov)
        innovation = target_a - expected_a
        obs_variance = max(self._cfg.obs_var_floor, expected_a * (1.0 - expected_a))

        denom = float(gradient @ joint_cov @ gradient.T) + obs_variance
        kalman = (joint_cov @ gradient) / denom

        joint_mean = np.concatenate((state_a.skill_mean, state_b.skill_mean))
        updated_mean = joint_mean + kalman * innovation

        identity = np.eye(2 * SKILL_DIM)
        updated_cov = (identity - np.outer(kalman, gradient)) @ joint_cov
        updated_cov = 0.5 * (updated_cov + updated_cov.T)

        return (
            updated_mean[:SKILL_DIM],
            updated_cov[:SKILL_DIM, :SKILL_DIM],
            updated_mean[SKILL_DIM:],
            updated_cov[SKILL_DIM:, SKILL_DIM:],
        )

    # ---- Cross-sport update (skills only, no Elo change) ----

    def _cross_sport_update(
        self,
        fighter_a: UnifiedFighterState,
        fighter_b: UnifiedFighterState,
        evidence: BoutEvidence,
        expected_a: float,
        expected_b: float,
    ) -> tuple[UnifiedFighterState, UnifiedFighterState, RatingDelta, RatingDelta]:
        """For non-MMA bouts: update skill vectors only, leave Elo/RD untouched."""
        if evidence.outcome_for_a == BoutOutcome.NO_CONTEST:
            return self._no_update(fighter_a, fighter_b, evidence, expected_a, expected_b)

        target_a = (
            1.0
            if evidence.outcome_for_a == BoutOutcome.WIN
            else (0.0 if evidence.outcome_for_a == BoutOutcome.LOSS else 0.5)
        )

        new_skill_a, new_cov_a, new_skill_b, new_cov_b = self._ekf_skill_update(
            fighter_a, fighter_b, evidence, expected_a, target_a
        )

        post_a = replace(fighter_a, skill_mean=new_skill_a, skill_cov=new_cov_a)
        post_b = replace(fighter_b, skill_mean=new_skill_b, skill_cov=new_cov_b)

        delta_a = self._make_delta(evidence, fighter_a, post_a, expected_a, target_a, 0.0)
        delta_b = self._make_delta(evidence, fighter_b, post_b, expected_b, 1.0 - target_a, 0.0)
        return post_a, post_b, delta_a, delta_b

    # ---- No-update pass-through ----

    def _no_update(
        self,
        fighter_a: UnifiedFighterState,
        fighter_b: UnifiedFighterState,
        evidence: BoutEvidence,
        expected_a: float,
        expected_b: float,
    ) -> tuple[UnifiedFighterState, UnifiedFighterState, RatingDelta, RatingDelta]:
        target_a = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_a
        target_b = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_b
        delta_a = self._make_delta(evidence, fighter_a, fighter_a, expected_a, target_a, 0.0)
        delta_b = self._make_delta(evidence, fighter_b, fighter_b, expected_b, target_b, 0.0)
        return fighter_a, fighter_b, delta_a, delta_b

    # ---- Post-state builder ----

    def _build_post_state(
        self,
        pre: UnifiedFighterState,
        evidence: BoutEvidence,
        new_rating: float,
        new_rd: float,
        new_vol: float,
        new_skill: NDArray[np.float64],
        new_cov: NDArray[np.float64],
        target: float,
    ) -> UnifiedFighterState:
        is_win = (
            evidence.outcome_for_a == BoutOutcome.WIN
            if target > _DRAW_TARGET
            else (evidence.outcome_for_a == BoutOutcome.LOSS if target < _DRAW_TARGET else False)
        )
        is_loss = not is_win and target != _DRAW_TARGET

        # Win streak: positive for wins, negative for losses, reset on opposite
        if is_win:
            new_streak = max(1, pre.win_streak + 1)
        elif is_loss:
            new_streak = min(-1, pre.win_streak - 1) if pre.win_streak <= 0 else -1
        else:
            new_streak = 0

        # Finish tracking
        is_finish = evidence.method in {FinishMethod.KO, FinishMethod.TKO, FinishMethod.SUB}
        new_finishes = pre.total_finishes + (1 if is_finish and is_win else 0)

        # PS history
        new_ps = list(pre.ps_history)
        if evidence.ps_fight_a is not None:
            ps_val = evidence.ps_fight_a if target > _DRAW_TARGET else (1.0 - evidence.ps_fight_a)
            new_ps.append(ps_val)
            if len(new_ps) > _PS_HISTORY_MAX:
                new_ps = new_ps[-_PS_HISTORY_MAX:]

        # Peak
        composite = self._cfg.composite_elo_share * new_rating + self._cfg.composite_skill_share * (
            self._cfg.base_rating + self._cfg.rating_scale * float(new_skill[0])
        )
        new_peak = max(pre.peak_rating, composite)
        if is_loss:
            # Gradually decay peak memory on losses
            new_peak = pre.peak_rating - self._cfg.peak_decay_per_loss * (pre.peak_rating - self._cfg.base_rating)
            new_peak = max(new_peak, composite)

        return UnifiedFighterState(
            fighter_id=pre.fighter_id,
            rating=new_rating,
            rd=new_rd,
            volatility=new_vol,
            skill_mean=new_skill,
            skill_cov=new_cov,
            mma_fights_count=pre.mma_fights_count + 1,
            last_mma_date=evidence.event_date,
            win_streak=new_streak,
            total_finishes=new_finishes,
            total_fights_tracked=pre.total_fights_tracked + 1,
            ps_history=new_ps,
            peak_rating=new_peak,
        )

    # ---- Sport-specific skill weights ----

    def _sport_weights(self, sport_key: str) -> NDArray[np.float64]:
        normalized = sport_key.lower()
        if normalized == 'mma':
            # mma, striking, grappling, durability, cardio
            return np.array([1.0, 0.30, 0.30, 0.20, 0.20], dtype=np.float64)
        if normalized in {'kickboxing', 'boxing', 'muay', 'lethwei', 'boxing_cage'}:
            return np.array([0.0, 1.0, 0.0, 0.20, 0.15], dtype=np.float64)
        if normalized in {'grappling', 'wrestling', 'judo', 'sambo', 'combat_jj'}:
            return np.array([0.0, 0.0, 1.0, 0.15, 0.20], dtype=np.float64)
        return np.array([0.25, 0.45, 0.20, 0.20, 0.15], dtype=np.float64)

    # ---- Covariate shift (from System C) ----

    def _covariate_shift(self, evidence: BoutEvidence | None, latent_gap: float) -> float:
        if evidence is None:
            return 0.0
        margin = evidence.ps_margin_a or 0.0
        promotion = (evidence.promotion_strength or 0.5) - 0.5
        shift = self._cfg.beta_ps_margin * margin + self._cfg.beta_promotion * promotion

        finish_indicator = 1.0 if evidence.method in {FinishMethod.KO, FinishMethod.TKO, FinishMethod.SUB} else 0.0
        damage_margin = abs(margin)
        finish_intensity = self._cfg.gamma_damage * damage_margin + self._cfg.eta_latent * latent_gap
        shift += self._cfg.finish_intensity_weight * finish_indicator * finish_intensity
        return shift

    # ---- Process noise ----

    def _process_noise_vector(self, days: int) -> NDArray[np.float64]:
        return days * np.array(
            [
                self._cfg.process_noise_mma,
                self._cfg.process_noise_striking,
                self._cfg.process_noise_grappling,
                self._cfg.process_noise_durability,
                self._cfg.process_noise_cardio,
            ],
            dtype=np.float64,
        )

    # ---- Delta construction ----

    def _make_delta(
        self,
        evidence: BoutEvidence,
        pre: UnifiedFighterState,
        post: UnifiedFighterState,
        expected: float,
        target: float,
        k_effective: float,
    ) -> RatingDelta:
        pre_composite = self.composite_rating(pre)
        post_composite = self.composite_rating(post)
        return RatingDelta(
            bout_id=evidence.bout_id,
            fighter_id=pre.fighter_id,
            pre_rating=pre_composite,
            post_rating=post_composite,
            delta_rating=post_composite - pre_composite,
            expected_win_prob=expected,
            target_score=target,
            k_effective=k_effective,
            tier_used=evidence.tier,
        )


# ---------------------------------------------------------------------------
# Glicko-2 helper functions (same math as System B)
# ---------------------------------------------------------------------------


def _to_mu_phi(rating: float, rd: float) -> tuple[float, float]:
    return (rating - 1500.0) / _GLICKO_SCALE, rd / _GLICKO_SCALE


def _from_mu_phi(mu: float, phi: float) -> tuple[float, float]:
    return 1500.0 + mu * _GLICKO_SCALE, phi * _GLICKO_SCALE


def _g(phi: float) -> float:
    return 1.0 / math.sqrt(1.0 + 3.0 * phi**2 / _PI_SQ)


def _expected_glicko(mu: float, mu_op: float, phi_op: float) -> float:
    exponent = -_g(phi_op) * (mu - mu_op)
    return 1.0 / (1.0 + math.exp(exponent))


def _f_vol(x_val: float, phi: float, delta: float, variance: float, a_val: float, tau: float) -> float:
    exp_x = math.exp(x_val)
    numer = exp_x * (delta**2 - phi**2 - variance - exp_x)
    denom = 2.0 * (phi**2 + variance + exp_x) ** 2
    return (numer / denom) - ((x_val - a_val) / (tau**2))


def _initial_b(a_val: float, delta: float, phi: float, variance: float, tau: float) -> float:
    if delta**2 > phi**2 + variance:
        return math.log(delta**2 - phi**2 - variance)
    k = 1
    candidate = a_val - k * tau
    while _f_vol(candidate, phi, delta, variance, a_val, tau) < 0.0:
        k += 1
        candidate = a_val - k * tau
    return candidate


def _update_volatility(phi: float, delta: float, variance: float, sigma: float, tau: float, eps: float) -> float:
    a_val = math.log(sigma**2)
    a_bound = a_val
    b_bound = _initial_b(a_val, delta, phi, variance, tau)

    f_a = _f_vol(a_bound, phi, delta, variance, a_val, tau)
    f_b = _f_vol(b_bound, phi, delta, variance, a_val, tau)

    while abs(b_bound - a_bound) > eps:
        c_val = a_bound + (a_bound - b_bound) * f_a / (f_b - f_a)
        f_c = _f_vol(c_val, phi, delta, variance, a_val, tau)

        if f_c * f_b <= 0.0:
            a_bound = b_bound
            f_a = f_b
        else:
            f_a /= 2.0

        b_bound = c_val
        f_b = f_c

    return math.exp(b_bound / 2.0)


def _block_diag(a_mat: NDArray[np.float64], b_mat: NDArray[np.float64]) -> NDArray[np.float64]:
    rows = a_mat.shape[0] + b_mat.shape[0]
    result = np.zeros((rows, rows), dtype=np.float64)
    n = a_mat.shape[0]
    result[:n, :n] = a_mat
    result[n:, n:] = b_mat
    return result


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def new_unified_state(fighter_id: str, config: UnifiedEloConfig | None = None) -> UnifiedFighterState:
    cfg = config or UnifiedEloConfig()
    return UnifiedFighterState(
        fighter_id=fighter_id,
        rating=cfg.base_rating,
        rd=cfg.base_rd,
        volatility=cfg.base_volatility,
        skill_mean=np.zeros(SKILL_DIM, dtype=np.float64),
        skill_cov=np.diag(
            np.array(
                [
                    cfg.initial_var_mma,
                    cfg.initial_var_striking,
                    cfg.initial_var_grappling,
                    cfg.initial_var_durability,
                    cfg.initial_var_cardio,
                ],
                dtype=np.float64,
            )
        ),
        peak_rating=cfg.base_rating,
    )
