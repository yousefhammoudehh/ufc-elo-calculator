from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date

import numpy as np
from numpy.typing import NDArray

from elo_calculator.application.ranking.math_utils import logistic
from elo_calculator.application.ranking.targets import (
    DEFAULT_TARGET_CONFIG,
    TargetScoreConfig,
    compute_target_score,
    outcome_score,
)
from elo_calculator.application.ranking.types import BoutEvidence, BoutOutcome, FinishMethod, RatingDelta

LATENT_DIM = 4


@dataclass(frozen=True)
class DynamicFactorConfig:
    base_elo_rating: float = 1500.0
    elo_scale: float = 400.0
    process_noise_mma: float = 0.0007
    process_noise_striking: float = 0.0010
    process_noise_grappling: float = 0.0010
    process_noise_durability: float = 0.0008
    initial_var_mma: float = 1.0
    initial_var_striking: float = 1.2
    initial_var_grappling: float = 1.2
    initial_var_durability: float = 1.0
    beta_ps_margin: float = 0.65
    beta_promotion: float = 0.20
    gamma_damage: float = 0.35
    eta_latent: float = 0.30
    finish_intensity_weight: float = 0.05
    observation_var_floor: float = 0.02
    no_update_on_draw: bool = True


@dataclass(frozen=True)
class DynamicFactorState:
    fighter_id: str
    mean: NDArray[np.float64]
    cov: NDArray[np.float64]
    last_fight_date: date | None = None
    mma_fights_count: int = 0


class DynamicFactorBradleyTerrySystem:
    def __init__(
        self, config: DynamicFactorConfig | None = None, target_config: TargetScoreConfig = DEFAULT_TARGET_CONFIG
    ) -> None:
        self._config = config or DynamicFactorConfig()
        self._target_config = target_config

    def update_bout(
        self, fighter_a: DynamicFactorState, fighter_b: DynamicFactorState, evidence: BoutEvidence
    ) -> tuple[DynamicFactorState, DynamicFactorState, RatingDelta, RatingDelta]:
        prior_a = self._advance_state(fighter_a, evidence.event_date)
        prior_b = self._advance_state(fighter_b, evidence.event_date)

        z_value = self._linear_predictor(prior_a.mean, prior_b.mean, evidence)
        expected_a = logistic(z_value)
        expected_b = 1.0 - expected_a

        if evidence.outcome_for_a == BoutOutcome.NO_CONTEST:
            return self._no_update(prior_a, prior_b, evidence, expected_a, expected_b)

        if evidence.outcome_for_a == BoutOutcome.DRAW and self._config.no_update_on_draw:
            return self._no_update(prior_a, prior_b, evidence, expected_a, expected_b)

        y_a = self._observed_score(evidence, expected_a)
        y_b = 1.0 - y_a

        updated_a, updated_b = self._ekf_update(prior_a, prior_b, evidence, expected_a, y_a)
        post_a = self._with_post_metadata(updated_a, evidence)
        post_b = self._with_post_metadata(updated_b, evidence)

        delta_a = self._delta(evidence, prior_a, post_a, expected_a, y_a)
        delta_b = self._delta(evidence, prior_b, post_b, expected_b, y_b)
        return post_a, post_b, delta_a, delta_b

    def expected_win_probability(
        self, fighter_a: DynamicFactorState, fighter_b: DynamicFactorState, evidence: BoutEvidence
    ) -> float:
        return logistic(self._linear_predictor(fighter_a.mean, fighter_b.mean, evidence))

    def implied_mma_rating(self, state: DynamicFactorState) -> float:
        return self._config.base_elo_rating + self._config.elo_scale * float(state.mean[0])

    def mma_uncertainty(self, state: DynamicFactorState) -> float:
        return self._config.elo_scale * float(np.sqrt(max(0.0, state.cov[0, 0])))

    def _observed_score(self, evidence: BoutEvidence, expected_a: float) -> float:
        if evidence.sport_key.lower() == 'mma':
            return compute_target_score(evidence, expected_a, self._target_config)
        return outcome_score(evidence.outcome_for_a)

    def _advance_state(self, state: DynamicFactorState, event_date: date) -> DynamicFactorState:
        if state.last_fight_date is None:
            return state

        days = max(0, (event_date - state.last_fight_date).days)
        if days == 0:
            return state

        process = np.diag(self._process_noise_vector(days))
        updated_cov = state.cov + process
        return replace(state, cov=updated_cov)

    def _with_post_metadata(self, state: DynamicFactorState, evidence: BoutEvidence) -> DynamicFactorState:
        mma_increment = 1 if evidence.sport_key.lower() == 'mma' else 0
        return replace(
            state, mma_fights_count=state.mma_fights_count + mma_increment, last_fight_date=evidence.event_date
        )

    def _ekf_update(
        self,
        state_a: DynamicFactorState,
        state_b: DynamicFactorState,
        evidence: BoutEvidence,
        expected_a: float,
        observed_a: float,
    ) -> tuple[DynamicFactorState, DynamicFactorState]:
        weights = self._sport_weights(evidence.sport_key)
        gradient_scale = expected_a * (1.0 - expected_a)
        gradient = np.concatenate((gradient_scale * weights, -gradient_scale * weights))

        joint_cov = _block_diag(state_a.cov, state_b.cov)
        innovation = observed_a - expected_a
        obs_variance = max(self._config.observation_var_floor, expected_a * (1.0 - expected_a))

        denom = float(gradient @ joint_cov @ gradient.T) + obs_variance
        kalman = (joint_cov @ gradient) / denom

        joint_mean = np.concatenate((state_a.mean, state_b.mean))
        updated_mean = joint_mean + kalman * innovation

        identity = np.eye(2 * LATENT_DIM)
        updated_cov = (identity - np.outer(kalman, gradient)) @ joint_cov
        updated_cov = 0.5 * (updated_cov + updated_cov.T)

        mean_a = updated_mean[:LATENT_DIM]
        mean_b = updated_mean[LATENT_DIM:]
        cov_a = updated_cov[:LATENT_DIM, :LATENT_DIM]
        cov_b = updated_cov[LATENT_DIM:, LATENT_DIM:]

        return replace(state_a, mean=mean_a, cov=cov_a), replace(state_b, mean=mean_b, cov=cov_b)

    def _no_update(
        self,
        fighter_a: DynamicFactorState,
        fighter_b: DynamicFactorState,
        evidence: BoutEvidence,
        expected_a: float,
        expected_b: float,
    ) -> tuple[DynamicFactorState, DynamicFactorState, RatingDelta, RatingDelta]:
        target_a = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_a
        target_b = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_b
        post_a = self._with_post_metadata(fighter_a, evidence)
        post_b = self._with_post_metadata(fighter_b, evidence)
        delta_a = self._delta(evidence, fighter_a, post_a, expected_a, target_a)
        delta_b = self._delta(evidence, fighter_b, post_b, expected_b, target_b)
        return post_a, post_b, delta_a, delta_b

    def _delta(
        self,
        evidence: BoutEvidence,
        pre_state: DynamicFactorState,
        post_state: DynamicFactorState,
        expected: float,
        target: float,
    ) -> RatingDelta:
        pre_rating = self.implied_mma_rating(pre_state)
        post_rating = self.implied_mma_rating(post_state)
        return RatingDelta(
            bout_id=evidence.bout_id,
            fighter_id=pre_state.fighter_id,
            pre_rating=pre_rating,
            post_rating=post_rating,
            delta_rating=post_rating - pre_rating,
            expected_win_prob=expected,
            target_score=target,
            k_effective=0.0,
            tier_used=evidence.tier,
        )

    def _linear_predictor(
        self, mean_a: NDArray[np.float64], mean_b: NDArray[np.float64], evidence: BoutEvidence
    ) -> float:
        weights = self._sport_weights(evidence.sport_key)
        latent_gap = float(weights @ (mean_a - mean_b))
        shift = self._covariate_shift(evidence, latent_gap)
        return latent_gap + shift

    def _covariate_shift(self, evidence: BoutEvidence, latent_gap: float) -> float:
        margin = evidence.ps_margin_a or 0.0
        promotion = (evidence.promotion_strength or 0.5) - 0.5
        shift = self._config.beta_ps_margin * margin + self._config.beta_promotion * promotion

        finish_indicator = 1.0 if evidence.method in {FinishMethod.KO, FinishMethod.TKO, FinishMethod.SUB} else 0.0
        damage_margin = abs(margin)
        finish_intensity = self._config.gamma_damage * damage_margin + self._config.eta_latent * latent_gap
        shift += self._config.finish_intensity_weight * finish_indicator * finish_intensity
        return shift

    def _sport_weights(self, sport_key: str) -> NDArray[np.float64]:
        normalized = sport_key.lower()
        if normalized == 'mma':
            return np.array([1.0, 0.35, 0.35, 0.20], dtype=np.float64)
        if normalized in {'kickboxing', 'boxing', 'muay', 'lethwei', 'boxing_cage'}:
            return np.array([0.0, 1.0, 0.0, 0.20], dtype=np.float64)
        if normalized in {'grappling', 'wrestling', 'judo', 'sambo', 'combat_jj'}:
            return np.array([0.0, 0.0, 1.0, 0.20], dtype=np.float64)
        return np.array([0.25, 0.50, 0.25, 0.20], dtype=np.float64)

    def _process_noise_vector(self, days: int) -> NDArray[np.float64]:
        return days * np.array(
            [
                self._config.process_noise_mma,
                self._config.process_noise_striking,
                self._config.process_noise_grappling,
                self._config.process_noise_durability,
            ],
            dtype=np.float64,
        )


def _block_diag(a_matrix: NDArray[np.float64], b_matrix: NDArray[np.float64]) -> NDArray[np.float64]:
    rows = a_matrix.shape[0] + b_matrix.shape[0]
    result = np.zeros((rows, rows), dtype=np.float64)
    a_rows = a_matrix.shape[0]
    result[:a_rows, :a_rows] = a_matrix
    result[a_rows:, a_rows:] = b_matrix
    return result


def new_dynamic_factor_state(fighter_id: str, config: DynamicFactorConfig | None = None) -> DynamicFactorState:
    cfg = config or DynamicFactorConfig()
    mean = np.zeros(LATENT_DIM, dtype=np.float64)
    cov = np.diag(
        np.array(
            [cfg.initial_var_mma, cfg.initial_var_striking, cfg.initial_var_grappling, cfg.initial_var_durability],
            dtype=np.float64,
        )
    )
    return DynamicFactorState(fighter_id=fighter_id, mean=mean, cov=cov)
