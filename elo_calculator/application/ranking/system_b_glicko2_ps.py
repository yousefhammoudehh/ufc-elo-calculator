from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import date

from elo_calculator.application.ranking.targets import DEFAULT_TARGET_CONFIG, TargetScoreConfig, compute_target_score
from elo_calculator.application.ranking.types import BoutEvidence, BoutOutcome, RatingDelta

_SCALE = 173.7178
_PI_SQ = math.pi**2


@dataclass(frozen=True)
class Glicko2PSConfig:
    base_rating: float = 1500.0
    base_rd: float = 350.0
    base_volatility: float = 0.06
    tau: float = 0.5
    convergence_eps: float = 1e-6
    rating_period_days: float = 30.0
    inactivity_rd_growth: float = 0.06
    no_update_on_draw: bool = True


@dataclass(frozen=True)
class Glicko2FighterState:
    fighter_id: str
    rating: float
    rd: float
    volatility: float
    mma_fights_count: int = 0
    last_mma_date: date | None = None


class Glicko2PerformanceRatingSystem:
    def __init__(
        self, config: Glicko2PSConfig | None = None, target_config: TargetScoreConfig = DEFAULT_TARGET_CONFIG
    ) -> None:
        self._config = config or Glicko2PSConfig()
        self._target_config = target_config

    def expected_win_probability(self, fighter_a: Glicko2FighterState, fighter_b: Glicko2FighterState) -> float:
        mu_a, _phi_a = _to_mu_phi(fighter_a.rating, fighter_a.rd)
        mu_b, phi_b = _to_mu_phi(fighter_b.rating, fighter_b.rd)
        return _expected(mu_a, mu_b, phi_b)

    def update_bout(
        self, fighter_a: Glicko2FighterState, fighter_b: Glicko2FighterState, evidence: BoutEvidence
    ) -> tuple[Glicko2FighterState, Glicko2FighterState, RatingDelta, RatingDelta]:
        advanced_a = self._advance_inactivity(fighter_a, evidence.event_date)
        advanced_b = self._advance_inactivity(fighter_b, evidence.event_date)

        expected_a = self.expected_win_probability(advanced_a, advanced_b)
        expected_b = 1.0 - expected_a

        if evidence.sport_key.lower() != 'mma' or evidence.outcome_for_a == BoutOutcome.NO_CONTEST:
            return self._no_update(advanced_a, advanced_b, evidence, expected_a, expected_b)

        if evidence.outcome_for_a == BoutOutcome.DRAW and self._config.no_update_on_draw:
            return self._no_update(advanced_a, advanced_b, evidence, expected_a, expected_b)

        target_a = compute_target_score(evidence, expected_a, self._target_config)
        target_b = 1.0 - target_a

        updated_a = self._update_one(advanced_a, advanced_b, target_a)
        updated_b = self._update_one(advanced_b, advanced_a, target_b)

        post_a = replace(updated_a, mma_fights_count=advanced_a.mma_fights_count + 1, last_mma_date=evidence.event_date)
        post_b = replace(updated_b, mma_fights_count=advanced_b.mma_fights_count + 1, last_mma_date=evidence.event_date)

        delta_a = self._delta(evidence, advanced_a, post_a, expected_a, target_a)
        delta_b = self._delta(evidence, advanced_b, post_b, expected_b, target_b)
        return post_a, post_b, delta_a, delta_b

    def _advance_inactivity(self, fighter: Glicko2FighterState, event_date: date) -> Glicko2FighterState:
        if fighter.last_mma_date is None:
            return fighter
        days = max(0, (event_date - fighter.last_mma_date).days)
        periods = days / self._config.rating_period_days
        if periods <= 0:
            return fighter

        _, phi = _to_mu_phi(fighter.rating, fighter.rd)
        phi_new = math.sqrt(phi**2 + periods * self._config.inactivity_rd_growth**2)
        _, rd_new = _from_mu_phi((fighter.rating - self._config.base_rating) / _SCALE, phi_new)
        return replace(fighter, rd=rd_new)

    def _update_one(
        self, fighter: Glicko2FighterState, opponent: Glicko2FighterState, score: float
    ) -> Glicko2FighterState:
        mu, phi = _to_mu_phi(fighter.rating, fighter.rd)
        mu_op, phi_op = _to_mu_phi(opponent.rating, opponent.rd)

        g_phi = _g(phi_op)
        expected = _expected(mu, mu_op, phi_op)
        variance = 1.0 / (g_phi**2 * expected * (1.0 - expected))
        delta = variance * g_phi * (score - expected)

        sigma_prime = _update_volatility(
            phi=phi,
            delta=delta,
            variance=variance,
            sigma=fighter.volatility,
            tau=self._config.tau,
            eps=self._config.convergence_eps,
        )

        phi_star = math.sqrt(phi**2 + sigma_prime**2)
        phi_prime = 1.0 / math.sqrt((1.0 / phi_star**2) + (1.0 / variance))
        mu_prime = mu + phi_prime**2 * g_phi * (score - expected)

        rating_prime, rd_prime = _from_mu_phi(mu_prime, phi_prime)
        return replace(fighter, rating=rating_prime, rd=rd_prime, volatility=sigma_prime)

    def _no_update(
        self,
        fighter_a: Glicko2FighterState,
        fighter_b: Glicko2FighterState,
        evidence: BoutEvidence,
        expected_a: float,
        expected_b: float,
    ) -> tuple[Glicko2FighterState, Glicko2FighterState, RatingDelta, RatingDelta]:
        target_a = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_a
        target_b = 0.5 if evidence.outcome_for_a == BoutOutcome.DRAW else expected_b

        post_a = replace(fighter_a, mma_fights_count=fighter_a.mma_fights_count + 1, last_mma_date=evidence.event_date)
        post_b = replace(fighter_b, mma_fights_count=fighter_b.mma_fights_count + 1, last_mma_date=evidence.event_date)
        delta_a = self._delta(evidence, fighter_a, post_a, expected_a, target_a)
        delta_b = self._delta(evidence, fighter_b, post_b, expected_b, target_b)
        return post_a, post_b, delta_a, delta_b

    def _delta(
        self,
        evidence: BoutEvidence,
        pre_state: Glicko2FighterState,
        post_state: Glicko2FighterState,
        expected: float,
        target: float,
    ) -> RatingDelta:
        return RatingDelta(
            bout_id=evidence.bout_id,
            fighter_id=pre_state.fighter_id,
            pre_rating=pre_state.rating,
            post_rating=post_state.rating,
            delta_rating=post_state.rating - pre_state.rating,
            expected_win_prob=expected,
            target_score=target,
            k_effective=0.0,
            tier_used=evidence.tier,
        )


def _to_mu_phi(rating: float, rd: float) -> tuple[float, float]:
    return (rating - 1500.0) / _SCALE, rd / _SCALE


def _from_mu_phi(mu: float, phi: float) -> tuple[float, float]:
    return 1500.0 + mu * _SCALE, phi * _SCALE


def _g(phi: float) -> float:
    return 1.0 / math.sqrt(1.0 + 3.0 * phi**2 / _PI_SQ)


def _expected(mu: float, mu_op: float, phi_op: float) -> float:
    exponent = -_g(phi_op) * (mu - mu_op)
    return 1.0 / (1.0 + math.exp(exponent))


def _f(x_value: float, phi: float, delta: float, variance: float, a_value: float, tau: float) -> float:
    exp_x = math.exp(x_value)
    numerator = exp_x * (delta**2 - phi**2 - variance - exp_x)
    denominator = 2.0 * (phi**2 + variance + exp_x) ** 2
    return (numerator / denominator) - ((x_value - a_value) / (tau**2))


def _initial_b(a_value: float, delta: float, phi: float, variance: float, tau: float) -> float:
    if delta**2 > phi**2 + variance:
        return math.log(delta**2 - phi**2 - variance)

    k_value = 1
    candidate = a_value - k_value * tau
    while _f(candidate, phi, delta, variance, a_value, tau) < 0.0:
        k_value += 1
        candidate = a_value - k_value * tau
    return candidate


def _update_volatility(phi: float, delta: float, variance: float, sigma: float, tau: float, eps: float) -> float:
    a_value = math.log(sigma**2)
    a_bound = a_value
    b_bound = _initial_b(a_value, delta, phi, variance, tau)

    f_a = _f(a_bound, phi, delta, variance, a_value, tau)
    f_b = _f(b_bound, phi, delta, variance, a_value, tau)

    while abs(b_bound - a_bound) > eps:
        c_value = a_bound + (a_bound - b_bound) * f_a / (f_b - f_a)
        f_c = _f(c_value, phi, delta, variance, a_value, tau)
        if f_c * f_b < 0.0:
            a_bound = b_bound
            f_a = f_b
        else:
            f_a /= 2.0
        b_bound = c_value
        f_b = f_c

    return math.exp(a_bound / 2.0)


def new_glicko2_state(fighter_id: str, config: Glicko2PSConfig | None = None) -> Glicko2FighterState:
    base = config or Glicko2PSConfig()
    return Glicko2FighterState(
        fighter_id=fighter_id, rating=base.base_rating, rd=base.base_rd, volatility=base.base_volatility
    )
