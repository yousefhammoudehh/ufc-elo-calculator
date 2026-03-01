from __future__ import annotations

import math


def clip(value: float, lower: float, upper: float) -> float:
    if value < lower:
        return lower
    if value > upper:
        return upper
    return value


def logistic(value: float) -> float:
    if value >= 0:
        exp_neg = math.exp(-value)
        return 1.0 / (1.0 + exp_neg)
    exp_pos = math.exp(value)
    return exp_pos / (1.0 + exp_pos)


def safe_share(a_value: float, b_value: float, prior: float) -> float:
    denominator = a_value + b_value + 2.0 * prior
    if denominator <= 0:
        return 0.5
    return (a_value + prior) / denominator


def logit(probability: float, eps: float = 1e-6) -> float:
    clipped = clip(probability, eps, 1.0 - eps)
    return math.log(clipped / (1.0 - clipped))


def normalize_unit_interval(values: list[float]) -> list[float]:
    total = sum(values)
    if total <= 0:
        if not values:
            return []
        equal = 1.0 / len(values)
        return [equal for _ in values]
    return [value / total for value in values]
