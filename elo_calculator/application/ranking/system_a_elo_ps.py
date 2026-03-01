from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date

from elo_calculator.application.ranking.math_utils import clip, logistic
from elo_calculator.application.ranking.targets import DEFAULT_TARGET_CONFIG, TargetScoreConfig, compute_target_score
from elo_calculator.application.ranking.types import BoutEvidence, BoutOutcome, EvidenceTier, FinishMethod, RatingDelta


@dataclass(frozen=True)
class EloPSConfig:
    base_rating: float = 1500.0
    rating_scale: float = 400.0
    k0: float = 24.0
    tier_a_multiplier: float = 1.00
    tier_b_multiplier: float = 0.85
    tier_c_multiplier: float = 0.70
    title_rounds_multiplier: float = 1.08
    non_title_rounds_multiplier: float = 1.00
    finish_method_multiplier: float = 1.06
    decision_method_multiplier: float = 1.00
    early_career_multiplier: float = 1.15
    established_multiplier: float = 1.00
    veteran_multiplier: float = 0.92
    active_recency_multiplier: float = 1.03
    layoff_recency_multiplier: float = 0.90
    promotion_min_multiplier: float = 0.75
    promotion_max_multiplier: float = 1.10
    ps_margin_k_sensitivity: float = 0.40
    ps_margin_k_cap: float = 0.35
    title_round_threshold: int = 5
    early_career_fights: int = 5
    established_fights: int = 12
    layoff_days_threshold: int = 540
    no_update_on_draw: bool = True


@dataclass(frozen=True)
class EloFighterState:
    fighter_id: str
    rating: float
    mma_fights_count: int = 0
    last_mma_date: date | None = None


class EloPerformanceRatingSystem:
    def __init__(
        self, config: EloPSConfig | None = None, target_config: TargetScoreConfig = DEFAULT_TARGET_CONFIG
    ) -> None:
        self._config = config or EloPSConfig()
        self._target_config = target_config

    def expected_win_probability(self, rating_a: float, rating_b: float) -> float:
        delta = (rating_a - rating_b) / self._config.rating_scale
        return logistic(delta)

    def update_bout(
        self, fighter_a: EloFighterState, fighter_b: EloFighterState, evidence: BoutEvidence
    ) -> tuple[EloFighterState, EloFighterState, RatingDelta, RatingDelta]:
        expected_a = self.expected_win_probability(fighter_a.rating, fighter_b.rating)
        expected_b = 1.0 - expected_a

        if evidence.sport_key.lower() != 'mma':
            return self._no_update(fighter_a, fighter_b, evidence, expected_a, expected_b)

        if evidence.outcome_for_a == BoutOutcome.NO_CONTEST:
            return self._no_update(fighter_a, fighter_b, evidence, expected_a, expected_b)

        if evidence.outcome_for_a == BoutOutcome.DRAW and self._config.no_update_on_draw:
            return self._no_update(fighter_a, fighter_b, evidence, expected_a, expected_b)

        target_a = compute_target_score(evidence, expected_a, self._target_config)
        target_b = 1.0 - target_a

        k_a = self._effective_k(fighter_a, evidence)
        k_b = self._effective_k(fighter_b, evidence)

        delta_a = k_a * (target_a - expected_a)
        delta_b = k_b * (target_b - expected_b)

        updated_a = replace(
            fighter_a,
            rating=fighter_a.rating + delta_a,
            mma_fights_count=fighter_a.mma_fights_count + 1,
            last_mma_date=evidence.event_date,
        )
        updated_b = replace(
            fighter_b,
            rating=fighter_b.rating + delta_b,
            mma_fights_count=fighter_b.mma_fights_count + 1,
            last_mma_date=evidence.event_date,
        )

        return (
            updated_a,
            updated_b,
            self._delta(evidence, fighter_a, updated_a, expected_a, target_a, k_a),
            self._delta(evidence, fighter_b, updated_b, expected_b, target_b, k_b),
        )

    def _delta(
        self,
        evidence: BoutEvidence,
        pre_state: EloFighterState,
        post_state: EloFighterState,
        expected: float,
        target: float,
        k_effective: float,
    ) -> RatingDelta:
        return RatingDelta(
            bout_id=evidence.bout_id,
            fighter_id=pre_state.fighter_id,
            pre_rating=pre_state.rating,
            post_rating=post_state.rating,
            delta_rating=post_state.rating - pre_state.rating,
            expected_win_prob=expected,
            target_score=target,
            k_effective=k_effective,
            tier_used=evidence.tier,
        )

    def _no_update(
        self,
        fighter_a: EloFighterState,
        fighter_b: EloFighterState,
        evidence: BoutEvidence,
        expected_a: float,
        expected_b: float,
    ) -> tuple[EloFighterState, EloFighterState, RatingDelta, RatingDelta]:
        target_a = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_a
        target_b = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_b
        zero_a = self._delta(evidence, fighter_a, fighter_a, expected_a, target_a, 0.0)
        zero_b = self._delta(evidence, fighter_b, fighter_b, expected_b, target_b, 0.0)
        return fighter_a, fighter_b, zero_a, zero_b

    def _effective_k(self, fighter: EloFighterState, evidence: BoutEvidence) -> float:
        k = self._config.k0
        k *= self._tier_multiplier(evidence.tier)
        k *= self._rounds_multiplier(evidence)
        k *= self._method_multiplier(evidence)
        k *= self._ps_margin_multiplier(evidence)
        k *= self._experience_multiplier(fighter)
        k *= self._recency_multiplier(fighter, evidence.event_date)
        k *= self._promotion_multiplier(evidence.promotion_strength)
        return k

    def _tier_multiplier(self, tier: EvidenceTier) -> float:
        if tier == EvidenceTier.A:
            return self._config.tier_a_multiplier
        if tier == EvidenceTier.B:
            return self._config.tier_b_multiplier
        return self._config.tier_c_multiplier

    def _rounds_multiplier(self, evidence: BoutEvidence) -> float:
        if evidence.scheduled_rounds >= self._config.title_round_threshold or evidence.is_title_fight:
            return self._config.title_rounds_multiplier
        return self._config.non_title_rounds_multiplier

    def _method_multiplier(self, evidence: BoutEvidence) -> float:
        if evidence.method in {FinishMethod.KO, FinishMethod.TKO, FinishMethod.SUB}:
            return self._config.finish_method_multiplier
        return self._config.decision_method_multiplier

    def _ps_margin_multiplier(self, evidence: BoutEvidence) -> float:
        margin = abs(evidence.ps_margin_a or 0.0)
        boost = clip(margin * self._config.ps_margin_k_sensitivity, 0.0, self._config.ps_margin_k_cap)
        return 1.0 + boost

    def _experience_multiplier(self, fighter: EloFighterState) -> float:
        if fighter.mma_fights_count < self._config.early_career_fights:
            return self._config.early_career_multiplier
        if fighter.mma_fights_count < self._config.established_fights:
            return self._config.established_multiplier
        return self._config.veteran_multiplier

    def _recency_multiplier(self, fighter: EloFighterState, event_date: date) -> float:
        if fighter.last_mma_date is None:
            return self._config.active_recency_multiplier
        days = (event_date - fighter.last_mma_date).days
        if days > self._config.layoff_days_threshold:
            return self._config.layoff_recency_multiplier
        return self._config.active_recency_multiplier

    def _promotion_multiplier(self, strength: float | None) -> float:
        if strength is None:
            return 1.0
        normalized = clip(strength, 0.0, 1.0)
        raw = 0.85 + 0.30 * normalized
        return clip(raw, self._config.promotion_min_multiplier, self._config.promotion_max_multiplier)


def new_elo_state(fighter_id: str, config: EloPSConfig | None = None) -> EloFighterState:
    default_config = config or EloPSConfig()
    return EloFighterState(fighter_id=fighter_id, rating=default_config.base_rating)
