from __future__ import annotations

from dataclasses import dataclass

from elo_calculator.application.ranking.math_utils import clip, logistic, safe_share
from elo_calculator.application.ranking.types import FighterRoundStats, FightPSResult, RoundMeta, RoundPSResult

_SMALL_SHARE_EPSILON = 1e-9


@dataclass(frozen=True)
class PSRoundConstants:
    kd_prior: float = 0.20
    sig_prior: float = 5.0
    ctrl_prior_sec: float = 45.0
    sub_prior: float = 0.40
    td_prior: float = 1.0
    rev_prior: float = 0.25
    acc_prior_pct: float = 6.0
    pace_prior: float = 20.0
    w_damage: float = 0.70
    w_dominance: float = 0.20
    w_duration: float = 0.10
    head_w: float = 1.00
    body_w: float = 0.75
    leg_w: float = 0.55
    w_kd: float = 0.45
    w_target_sig: float = 0.40
    w_sub_threat: float = 0.10
    w_acc: float = 0.05
    w_ctrl: float = 0.55
    w_td: float = 0.20
    w_rev: float = 0.10
    w_ground_sig: float = 0.15
    w_ctrl_share: float = 0.70
    w_pace_share: float = 0.30
    threat_kd_w: float = 2.0
    threat_target_sig_w: float = 0.10
    threat_sub_w: float = 0.50
    gate_threshold: float = 1.25
    gate_tau: float = 0.70
    finish_boost_kappa: float = 0.35
    margin_10_8: float = 0.16
    margin_10_7: float = 0.28
    margin_scale_10_8: float = 0.05
    margin_scale_10_7: float = 0.05
    threat_sig_threshold: float = 1.20
    threat_some_threshold: float = 0.80
    threat_over_threshold: float = 2.25
    threat_sig_scale: float = 0.35
    threat_some_scale: float = 0.35
    threat_over_scale: float = 0.45
    dom_10_8_threshold: float = 0.66
    dom_10_7_threshold: float = 0.76
    dom_scale: float = 0.06
    low_offense_threshold: float = 2.00
    low_offense_scale: float = 0.60
    fight_finish_weight_kappa: float = 0.15


DEFAULT_PS_CONSTANTS = PSRoundConstants()


@dataclass(frozen=True)
class _RoundFeatures:
    target_sig: float
    sub_threat: float
    threat: float
    offense_level: float


def _target_sig(stats: FighterRoundStats, constants: PSRoundConstants) -> float:
    return (
        constants.head_w * stats.head_landed + constants.body_w * stats.body_landed + constants.leg_w * stats.leg_landed
    )


def _sub_threat(stats: FighterRoundStats) -> float:
    return stats.sub_attempts + 0.5 * stats.ground_landed


def _accuracy_pct(stats: FighterRoundStats) -> float:
    attempts = max(1.0, stats.sig_attempted)
    return 100.0 * stats.sig_landed / attempts


def _pace(stats: FighterRoundStats) -> float:
    return stats.sig_attempted + stats.td_attempted + stats.total_attempted


def _threat_level(kd: float, target_sig: float, sub_threat: float, constants: PSRoundConstants) -> float:
    return constants.threat_kd_w * kd + constants.threat_target_sig_w * target_sig + constants.threat_sub_w * sub_threat


def _extract_features(stats: FighterRoundStats, constants: PSRoundConstants) -> _RoundFeatures:
    target_sig = _target_sig(stats, constants)
    sub_threat = _sub_threat(stats)
    threat = _threat_level(stats.kd, target_sig, sub_threat, constants)
    offense_level = target_sig + stats.kd + sub_threat
    return _RoundFeatures(target_sig=target_sig, sub_threat=sub_threat, threat=threat, offense_level=offense_level)


def _gate(threat: float, constants: PSRoundConstants) -> float:
    return logistic((threat - constants.gate_threshold) / constants.gate_tau)


def _renormalize(a_value: float, b_value: float) -> tuple[float, float]:
    total = a_value + b_value
    if total < _SMALL_SHARE_EPSILON:
        return 0.5, 0.5
    return a_value / total, b_value / total


def _apply_finish_boost(damage_a: float, meta: RoundMeta, constants: PSRoundConstants) -> float:
    if not meta.is_finish_round:
        return damage_a
    if meta.winner_is_a is None:
        return damage_a
    if meta.finish_elapsed_seconds is None:
        return damage_a

    finish_u = clip(meta.finish_elapsed_seconds / max(1, meta.round_seconds), 0.0, 1.0)
    boost = 1.0 + constants.finish_boost_kappa * (1.0 - finish_u) ** 2
    base = damage_a if meta.winner_is_a else 1.0 - damage_a
    boosted = clip(0.5 + boost * (base - 0.5), 0.0, 1.0)
    return boosted if meta.winner_is_a else 1.0 - boosted


def _damage_share(
    a_stats: FighterRoundStats,
    b_stats: FighterRoundStats,
    a_features: _RoundFeatures,
    b_features: _RoundFeatures,
    meta: RoundMeta,
    constants: PSRoundConstants,
) -> tuple[float, float]:
    s_kd = safe_share(a_stats.kd, b_stats.kd, constants.kd_prior)
    s_tsig = safe_share(a_features.target_sig, b_features.target_sig, constants.sig_prior)
    s_sub = safe_share(a_features.sub_threat, b_features.sub_threat, constants.sub_prior)
    s_acc = safe_share(_accuracy_pct(a_stats), _accuracy_pct(b_stats), constants.acc_prior_pct)
    dmg_a = (
        constants.w_kd * s_kd
        + constants.w_target_sig * s_tsig
        + constants.w_sub_threat * s_sub
        + constants.w_acc * s_acc
    )
    boosted = _apply_finish_boost(dmg_a, meta, constants)
    return boosted, 1.0 - boosted


def _dominance_share(
    a_stats: FighterRoundStats, b_stats: FighterRoundStats, gates: tuple[float, float], constants: PSRoundConstants
) -> tuple[float, float]:
    s_ctrl = safe_share(a_stats.ctrl_seconds, b_stats.ctrl_seconds, constants.ctrl_prior_sec)
    s_td = safe_share(a_stats.td_landed, b_stats.td_landed, constants.td_prior)
    s_rev = safe_share(a_stats.rev, b_stats.rev, constants.rev_prior)
    s_ground = safe_share(a_stats.ground_landed, b_stats.ground_landed, constants.sig_prior)
    raw_a = (
        constants.w_ctrl * s_ctrl + constants.w_td * s_td + constants.w_rev * s_rev + constants.w_ground_sig * s_ground
    )
    raw_b = 1.0 - raw_a
    return _renormalize(gates[0] * raw_a, gates[1] * raw_b)


def _duration_share(
    a_stats: FighterRoundStats, b_stats: FighterRoundStats, gates: tuple[float, float], constants: PSRoundConstants
) -> tuple[float, float]:
    s_ctrl = safe_share(a_stats.ctrl_seconds, b_stats.ctrl_seconds, constants.ctrl_prior_sec)
    s_pace = safe_share(_pace(a_stats), _pace(b_stats), constants.pace_prior)
    raw_a = constants.w_ctrl_share * s_ctrl + constants.w_pace_share * s_pace
    raw_b = 1.0 - raw_a
    return _renormalize(gates[0] * raw_a, gates[1] * raw_b)


def _normalize_ten_point(p10_8: float, p10_7: float) -> tuple[float, float, float]:
    p10_8 = clip(p10_8, 0.0, 1.0)
    p10_7 = clip(p10_7, 0.0, 1.0)
    total = p10_8 + p10_7
    if total > 1.0:
        p10_8 /= total
        p10_7 /= total
    p10_9 = clip(1.0 - p10_8 - p10_7, 0.0, 1.0)
    return p10_9, p10_8, p10_7


def _ten_point_probs(
    ps: float, threat: float, dom_share: float, opponent_offense: float, constants: PSRoundConstants
) -> tuple[float, float, float]:
    margin = ps - 0.5
    dmg_path = logistic((margin - constants.margin_10_8) / constants.margin_scale_10_8)
    dmg_path *= logistic((threat - constants.threat_sig_threshold) / constants.threat_sig_scale)

    dom_path = logistic((dom_share - constants.dom_10_8_threshold) / constants.dom_scale)
    dom_path *= logistic((threat - constants.threat_some_threshold) / constants.threat_some_scale)
    dom_path *= logistic((constants.low_offense_threshold - opponent_offense) / constants.low_offense_scale)

    p10_8 = max(dmg_path, dom_path)
    p10_7 = logistic((margin - constants.margin_10_7) / constants.margin_scale_10_7)
    p10_7 *= logistic((threat - constants.threat_over_threshold) / constants.threat_over_scale)
    p10_7 *= logistic((dom_share - constants.dom_10_7_threshold) / constants.dom_scale)
    return _normalize_ten_point(p10_8, p10_7)


def compute_ps_round(
    a_stats: FighterRoundStats,
    b_stats: FighterRoundStats,
    meta: RoundMeta,
    constants: PSRoundConstants = DEFAULT_PS_CONSTANTS,
) -> RoundPSResult:
    features_a = _extract_features(a_stats, constants)
    features_b = _extract_features(b_stats, constants)
    dmg_a, dmg_b = _damage_share(a_stats, b_stats, features_a, features_b, meta, constants)

    gates = (_gate(features_a.threat, constants), _gate(features_b.threat, constants))
    dom_a, dom_b = _dominance_share(a_stats, b_stats, gates, constants)
    dur_a, dur_b = _duration_share(a_stats, b_stats, gates, constants)

    ps_a = clip(constants.w_damage * dmg_a + constants.w_dominance * dom_a + constants.w_duration * dur_a, 0.0, 1.0)
    ps_b = clip(1.0 - ps_a, 0.0, 1.0)

    p10_9_a, p10_8_a, p10_7_a = _ten_point_probs(ps_a, features_a.threat, dom_a, features_b.offense_level, constants)
    p10_9_b, p10_8_b, p10_7_b = _ten_point_probs(ps_b, features_b.threat, dom_b, features_a.offense_level, constants)

    return RoundPSResult(
        ps_a=ps_a,
        ps_b=ps_b,
        dmg_a=dmg_a,
        dmg_b=dmg_b,
        dom_a=dom_a,
        dom_b=dom_b,
        dur_a=dur_a,
        dur_b=dur_b,
        p10_9_a=p10_9_a,
        p10_8_a=p10_8_a,
        p10_7_a=p10_7_a,
        p10_9_b=p10_9_b,
        p10_8_b=p10_8_b,
        p10_7_b=p10_7_b,
    )


def _round_weight(meta: RoundMeta, constants: PSRoundConstants) -> float:
    if not meta.is_finish_round:
        return 1.0
    if meta.finish_elapsed_seconds is None:
        return 1.0
    finish_u = clip(meta.finish_elapsed_seconds / max(1, meta.round_seconds), 0.0, 1.0)
    return 1.0 + constants.fight_finish_weight_kappa * (1.0 - finish_u) ** 2


def aggregate_fight_ps(
    round_results: list[RoundPSResult], round_metas: list[RoundMeta], constants: PSRoundConstants = DEFAULT_PS_CONSTANTS
) -> FightPSResult:
    if not round_results:
        return FightPSResult(ps_fight_a=0.5, ps_fight_b=0.5, quality_of_win_a=0.0, quality_of_win_b=0.0)

    weights = [_round_weight(meta, constants) for meta in round_metas]
    total_weight = sum(weights)
    if total_weight <= 0:
        total_weight = 1.0

    weighted_ps_a = sum(weight * result.ps_a for weight, result in zip(weights, round_results, strict=False))
    weighted_ps_b = sum(weight * result.ps_b for weight, result in zip(weights, round_results, strict=False))

    ps_fight_a = weighted_ps_a / total_weight
    ps_fight_b = weighted_ps_b / total_weight

    qow_a = sum(weight * max(0.0, result.ps_a - 0.5) for weight, result in zip(weights, round_results, strict=False))
    qow_b = sum(weight * max(0.0, result.ps_b - 0.5) for weight, result in zip(weights, round_results, strict=False))

    return FightPSResult(ps_fight_a=ps_fight_a, ps_fight_b=ps_fight_b, quality_of_win_a=qow_a, quality_of_win_b=qow_b)
