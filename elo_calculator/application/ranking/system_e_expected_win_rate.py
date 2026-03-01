from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from elo_calculator.application.ranking.math_utils import clip

ProbabilityFunction = Callable[[str, str], float]


@dataclass(frozen=True)
class ExpectedWinRateConfig:
    uncertainty_penalty_z: float = 1.0


@dataclass(frozen=True)
class ExpectedWinRateResult:
    fighter_id: str
    ewr: float
    adjusted_score: float


class ExpectedWinRatePoolSystem:
    """System E: converts pairwise win probabilities into stable leaderboard scores."""

    def __init__(self, config: ExpectedWinRateConfig | None = None) -> None:
        self._config = config or ExpectedWinRateConfig()

    def compute_ewr(self, fighter_id: str, reference_pool: list[str], probability_fn: ProbabilityFunction) -> float:
        opponents = [opponent for opponent in reference_pool if opponent != fighter_id]
        if not opponents:
            return 0.5

        probabilities = [clip(probability_fn(fighter_id, opponent), 0.0, 1.0) for opponent in opponents]
        return sum(probabilities) / len(probabilities)

    def rank(
        self,
        fighter_ids: list[str],
        reference_pool: list[str],
        probability_fn: ProbabilityFunction,
        uncertainty_map: dict[str, float] | None = None,
    ) -> list[ExpectedWinRateResult]:
        uncertainty_map = uncertainty_map or {}
        results = [self._row(fighter_id, reference_pool, probability_fn, uncertainty_map) for fighter_id in fighter_ids]
        return sorted(results, key=lambda row: row.adjusted_score, reverse=True)

    def _row(
        self,
        fighter_id: str,
        reference_pool: list[str],
        probability_fn: ProbabilityFunction,
        uncertainty_map: dict[str, float],
    ) -> ExpectedWinRateResult:
        ewr = self.compute_ewr(fighter_id, reference_pool, probability_fn)
        uncertainty = max(0.0, uncertainty_map.get(fighter_id, 0.0))
        adjusted = ewr - self._config.uncertainty_penalty_z * uncertainty
        return ExpectedWinRateResult(fighter_id=fighter_id, ewr=ewr, adjusted_score=adjusted)
