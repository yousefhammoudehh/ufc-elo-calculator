from __future__ import annotations

from dataclasses import dataclass

from elo_calculator.application.ranking.math_utils import clip
from elo_calculator.application.ranking.types import BoutEvidence, BoutOutcome, FinishMethod


@dataclass(frozen=True)
class TargetScoreConfig:
    win_loss_base_gap: float = 0.24
    alpha_margin: float = 0.22
    method_bonus_finish: float = 0.04
    method_bonus_decision: float = 0.0
    finish_time_bonus_scale: float = 0.03
    method_anchor_epsilon: float = 0.01


DEFAULT_TARGET_CONFIG = TargetScoreConfig()


def outcome_score(outcome: BoutOutcome) -> float:
    if outcome == BoutOutcome.WIN:
        return 1.0
    if outcome == BoutOutcome.LOSS:
        return 0.0
    return 0.5


def should_skip_update(outcome: BoutOutcome) -> bool:
    return outcome == BoutOutcome.NO_CONTEST


def is_finish_method(method: FinishMethod) -> bool:
    return method in {FinishMethod.KO, FinishMethod.TKO, FinishMethod.SUB}


def method_bonus(method: FinishMethod, config: TargetScoreConfig) -> float:
    if is_finish_method(method):
        return config.method_bonus_finish
    if method == FinishMethod.DEC:
        return config.method_bonus_decision
    return 0.0


def finish_time_bonus(evidence: BoutEvidence, config: TargetScoreConfig) -> float:
    if evidence.finish_round is None:
        return 0.0
    if evidence.finish_time_seconds is None:
        return 0.0
    if evidence.finish_round <= 0:
        return 0.0
    scheduled = max(evidence.scheduled_rounds, evidence.finish_round)
    total_elapsed = (evidence.finish_round - 1) * 300 + evidence.finish_time_seconds
    horizon = max(1, scheduled * 300)
    finish_u = clip(total_elapsed / horizon, 0.0, 1.0)
    return config.finish_time_bonus_scale * (1.0 - finish_u)


def _anchor_to_outcome(base: float, expected: float, outcome: BoutOutcome, config: TargetScoreConfig) -> float:
    if outcome == BoutOutcome.WIN:
        lower = max(0.5, expected + config.method_anchor_epsilon)
        return clip(base, lower, 1.0)
    if outcome == BoutOutcome.LOSS:
        upper = min(0.5, expected - config.method_anchor_epsilon)
        return clip(base, 0.0, upper)
    return 0.5


def compute_target_score(
    evidence: BoutEvidence, expected_win_prob: float, config: TargetScoreConfig = DEFAULT_TARGET_CONFIG
) -> float:
    outcome = evidence.outcome_for_a
    base_score = outcome_score(outcome)
    if outcome in {BoutOutcome.DRAW, BoutOutcome.NO_CONTEST}:
        return base_score

    outcome_direction = _outcome_direction(outcome)
    margin = evidence.ps_margin_a or 0.0
    score = _outcome_base_target(outcome, config)
    score += config.alpha_margin * margin
    score += outcome_direction * method_bonus(evidence.method, config)
    score += outcome_direction * finish_time_bonus(evidence, config)
    clipped = clip(score, 0.0, 1.0)
    return _anchor_to_outcome(clipped, expected_win_prob, outcome, config)


def _outcome_base_target(outcome: BoutOutcome, config: TargetScoreConfig) -> float:
    if outcome == BoutOutcome.WIN:
        return 0.5 + config.win_loss_base_gap
    if outcome == BoutOutcome.LOSS:
        return 0.5 - config.win_loss_base_gap
    return 0.5


def _outcome_direction(outcome: BoutOutcome) -> float:
    if outcome == BoutOutcome.WIN:
        return 1.0
    if outcome == BoutOutcome.LOSS:
        return -1.0
    return 0.0
