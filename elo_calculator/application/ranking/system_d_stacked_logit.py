from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from elo_calculator.application.ranking.math_utils import clip, logit
from elo_calculator.application.ranking.types import EvidenceTier, StackingSample


@dataclass(frozen=True)
class StackedLogitConfig:
    learning_rate: float = 0.05
    l2_penalty: float = 1e-3
    max_iter: int = 1500
    five_round_threshold: int = 5
    inactivity_days_norm: float = 365.0
    connectivity_norm_divisor: float = 20.0


class StackedLogitMixtureSystem:
    """System D: logistic stacking of A/B/C/(optional network) probabilities."""

    def __init__(self, config: StackedLogitConfig | None = None) -> None:
        self._config = config or StackedLogitConfig()
        self._weights: NDArray[np.float64] | None = None

    def fit(self, samples: list[StackingSample]) -> None:
        if not samples:
            self._weights = np.zeros(1, dtype=np.float64)
            return

        features = np.vstack([self._design_row(sample) for sample in samples])
        labels = np.array([sample.outcome_a_win for sample in samples], dtype=np.float64)
        self._weights = _fit_logistic(features, labels, self._config)

    def predict_probability(self, sample: StackingSample) -> float:
        if self._weights is None:
            raise ValueError('StackedLogitMixtureSystem must be fit before prediction.')
        row = self._design_row(sample)
        score = float(row @ self._weights)
        return float(_sigmoid(np.array([score], dtype=np.float64))[0])

    def predict_probabilities(self, samples: list[StackingSample]) -> list[float]:
        if self._weights is None:
            raise ValueError('StackedLogitMixtureSystem must be fit before prediction.')
        features = np.vstack([self._design_row(sample) for sample in samples])
        return [float(value) for value in _sigmoid(features @ self._weights)]

    def coefficients(self) -> NDArray[np.float64] | None:
        if self._weights is None:
            return None
        return self._weights.copy()

    def _design_row(self, sample: StackingSample) -> NDArray[np.float64]:
        tier_a = 1.0 if sample.tier == EvidenceTier.A else 0.0
        tier_b = 1.0 if sample.tier == EvidenceTier.B else 0.0
        p_n = sample.p_n if sample.p_n is not None else 0.5
        has_network = 0.0 if sample.p_n is None else 1.0

        scheduled_five = 1.0 if sample.scheduled_rounds >= self._config.five_round_threshold else 0.0
        days_norm = clip(sample.days_since_last_mma / self._config.inactivity_days_norm, 0.0, 4.0)
        connectivity_norm = clip(sample.opponent_connectivity / self._config.connectivity_norm_divisor, 0.0, 3.0)

        return np.array(
            [
                1.0,
                logit(sample.p_a),
                logit(sample.p_b),
                logit(sample.p_c),
                logit(p_n),
                has_network,
                tier_a,
                tier_b,
                scheduled_five,
                1.0 if sample.is_title_fight else 0.0,
                days_norm,
                1.0 if sample.missing_weight_class else 0.0,
                1.0 if sample.missing_promotion else 0.0,
                connectivity_norm,
            ],
            dtype=np.float64,
        )


def _fit_logistic(
    features: NDArray[np.float64], labels: NDArray[np.float64], config: StackedLogitConfig
) -> NDArray[np.float64]:
    weights = np.zeros(features.shape[1], dtype=np.float64)
    sample_count = max(1, features.shape[0])

    for _ in range(config.max_iter):
        logits = features @ weights
        probs = _sigmoid(logits)
        gradient = (features.T @ (probs - labels)) / sample_count
        regularized = np.concatenate((np.array([0.0]), weights[1:]))
        gradient += config.l2_penalty * regularized
        weights -= config.learning_rate * gradient

    return weights


def _sigmoid(values: NDArray[np.float64]) -> NDArray[np.float64]:
    return 1.0 / (1.0 + np.exp(-values))
