from __future__ import annotations

# ruff: noqa

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PSConstants:
    # Priors
    CTRL_PRIOR: float = 60.0  # seconds
    KD_PRIOR: float = 0.25
    PCT_PRIOR: float = 5.0  # percentage points
    SUB_PRIOR: float = 0.5
    REV_PRIOR: float = 0.25

    # Weights
    W_IMPACT: float = 0.75
    W_DOM: float = 0.15
    W_DUR: float = 0.10

    # Impact components
    W_KD: float = 0.28
    W_WSS: float = 0.32
    W_GDMG: float = 0.07
    W_CDMG: float = 0.03
    W_SUB: float = 0.03
    W_ACC: float = 0.02

    # Dominance components
    W_CTRLQ: float = 0.09
    W_TDP: float = 0.04
    W_REV: float = 0.02

    # Duration component (quality)
    W_DURQ: float = 0.10

    # Damage weights for targets
    HEAD_W: float = 1.00
    BODY_W: float = 0.80
    LEG_W: float = 0.55


def _as_pct(val: float | None) -> float:
    if val is None:
        return 0.0
    try:
        v = float(val)
        # If given as 0..1, convert to percentage points
        return v * 100.0 if v <= 1.0 else v
    except Exception:
        return 0.0


def _share(a: float, b: float, prior: float) -> float:
    a = float(a or 0.0)
    b = float(b or 0.0)
    p = float(prior or 0.0)
    denom = a + b + 2.0 * p
    if denom == 0.0:
        return 0.5
    return (a + p) / denom


def compute_ps_from_row(row: dict[str, Any], *, constants: PSConstants = PSConstants()) -> dict[str, Any]:
    """Compute Performance Scores (PS1, PS2) from a row with fighter1_* and fighter2_* stats.

    Expects keys like fighter1_kd, fighter1_sig_strikes, fighter1_sig_strike_percent, etc.
    Returns dict with PS1, PS2 and intermediate shares for debugging.
    """

    # Extract key stats
    kd1 = float(row.get('fighter1_kd', 0) or 0)
    kd2 = float(row.get('fighter2_kd', 0) or 0)

    # Significant strikes landed by target
    head1 = float(row.get('fighter1_head_ss', 0) or 0)
    body1 = float(row.get('fighter1_body_ss', 0) or 0)
    leg1 = float(row.get('fighter1_leg_ss', 0) or 0)
    head2 = float(row.get('fighter2_head_ss', 0) or 0)
    body2 = float(row.get('fighter2_body_ss', 0) or 0)
    leg2 = float(row.get('fighter2_leg_ss', 0) or 0)

    # Clinch/ground sig strikes
    clinch1 = float(row.get('fighter1_clinch_ss', 0) or 0)
    ground1 = float(row.get('fighter1_ground_ss', 0) or 0)
    clinch2 = float(row.get('fighter2_clinch_ss', 0) or 0)
    ground2 = float(row.get('fighter2_ground_ss', 0) or 0)

    # Sub attempts, reversals
    sub1 = float(row.get('fighter1_sub_attempts', 0) or 0)
    sub2 = float(row.get('fighter2_sub_attempts', 0) or 0)
    rev1 = float(row.get('fighter1_rev', 0) or 0)
    rev2 = float(row.get('fighter2_rev', 0) or 0)

    # Control seconds
    ctrl1 = float(row.get('fighter1_ctrl', 0) or 0)
    ctrl2 = float(row.get('fighter2_ctrl', 0) or 0)

    # Accuracy proxies (Sig%)
    sigpct1 = _as_pct(row.get('fighter1_sig_strike_percent'))
    sigpct2 = _as_pct(row.get('fighter2_sig_strike_percent'))

    # TD efficiency (percent); try direct percent if present
    tdpct1 = _as_pct(row.get('fighter1_td_percent'))
    tdpct2 = _as_pct(row.get('fighter2_td_percent'))
    # If attempts exist, recompute percent from L/A for robustness
    td_l1 = float(row.get('fighter1_td', 0) or 0)
    td_l2 = float(row.get('fighter2_td', 0) or 0)
    td_a1 = row.get('fighter1_td_attempts')
    td_a2 = row.get('fighter2_td_attempts')
    try:
        if td_a1 is not None:
            tdpct1 = 100.0 * float(td_l1) / max(1.0, float(td_a1))
    except Exception:
        pass
    try:
        if td_a2 is not None:
            tdpct2 = 100.0 * float(td_l2) / max(1.0, float(td_a2))
    except Exception:
        pass

    # Impact components
    S_KD = _share(kd1, kd2, constants.KD_PRIOR)
    wss1 = constants.HEAD_W * head1 + constants.BODY_W * body1 + constants.LEG_W * leg1
    wss2 = constants.HEAD_W * head2 + constants.BODY_W * body2 + constants.LEG_W * leg2
    S_WSS = _share(wss1, wss2, 0.0)
    S_GDMG = _share(ground1, ground2, 0.0)
    S_CDMG = _share(clinch1, clinch2, 0.0)
    S_SUB = _share(sub1, sub2, constants.SUB_PRIOR)
    S_ACC = _share(sigpct1, sigpct2, constants.PCT_PRIOR)

    # Dominance components
    S_CTRL = _share(ctrl1, ctrl2, constants.CTRL_PRIOR)
    # Quality gates (per fighter)
    q_raw_1 = 0.65 * S_GDMG + 0.35 * (1.0 if sub1 > 0 else 0.0)
    q_raw_2 = 0.65 * (1.0 - S_GDMG) + 0.35 * (1.0 if sub2 > 0 else 0.0)
    q_ctrl_1 = 0.10 + 0.90 * min(1.0, max(0.0, q_raw_1))
    q_ctrl_2 = 0.10 + 0.90 * min(1.0, max(0.0, q_raw_2))
    S_CTRLQ1 = S_CTRL * q_ctrl_1
    S_CTRLQ2 = (1.0 - S_CTRL) * q_ctrl_2

    # TD% share and half-credit rule
    S_TDp = _share(tdpct1, tdpct2, constants.PCT_PRIOR)
    S_TDp1 = S_TDp
    S_TDp2 = 1.0 - S_TDp
    if (ground1 + sub1) == 0:
        S_TDp1 *= 0.5
    if (ground2 + sub2) == 0:
        S_TDp2 *= 0.5

    S_REV = _share(rev1, rev2, constants.REV_PRIOR)

    # Duration-quality (shared form as spec for fighter 1)
    S_DUR = S_CTRL * (0.5 + 0.5 * ((S_GDMG + S_CDMG) / 2.0))

    # Assemble PS per fighter
    PS1 = (
        constants.W_KD * S_KD
        + constants.W_WSS * S_WSS
        + constants.W_GDMG * S_GDMG
        + constants.W_CDMG * S_CDMG
        + constants.W_SUB * S_SUB
        + constants.W_ACC * S_ACC
        + constants.W_CTRLQ * S_CTRLQ1
        + constants.W_TDP * S_TDp1
        + constants.W_REV * S_REV
        + constants.W_DURQ * S_DUR
    )
    # As per spec: PS2 is complementary
    PS2 = 1.0 - PS1

    # Clamp into [0,1]
    PS1 = max(0.0, min(1.0, PS1))
    PS2 = max(0.0, min(1.0, PS2))

    return {
        'PS1': PS1,
        'PS2': PS2,
        'shares': {
            'S_KD': S_KD,
            'S_WSS': S_WSS,
            'S_GDMG': S_GDMG,
            'S_CDMG': S_CDMG,
            'S_SUB': S_SUB,
            'S_ACC': S_ACC,
            'S_CTRL': S_CTRL,
            'S_CTRLQ1': S_CTRLQ1,
            'S_CTRLQ2': S_CTRLQ2,
            'S_TDp': S_TDp,
            'S_TDp1': S_TDp1,
            'S_TDp2': S_TDp2,
            'S_REV': S_REV,
            'S_DUR': S_DUR,
        },
    }
