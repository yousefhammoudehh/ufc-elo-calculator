"""
Entry Elo calculation for a fighter based on pre-UFC bouts and promotion strengths.

Algorithm (promotion-aware seeding):
1) For each pre-UFC fight i, map result to ri in {1.0 (W), 0.0 (L), 0.5 (D)}
2) Look up promotion strength sP(Pi) by promotion link; if not found, use a default.
3) Aggregate across fights:
    - wi = sP(Pi)
    - Neff = sum_i wi
    - weff = (sum_i wi * ri) / Neff, if Neff > 0 else 0.5
4) Seed Elo from aggregate:
    - Rseed = 1500 + S * (weff - 0.5) * Gamma(Neff)
    - S = 400, Gamma(N) = 1 - exp(-N/nu), nu = 8
5) Optional prior for micro-samples (k0 = 1): add one pseudo-fight at 0.5
    - weff <- (sum_i wi * ri + 0.5 * k0) / (Neff + k0)

Inputs:
- pre_ufc_bouts: Iterable of objects or dicts with fields:
  - result: FightOutcome enum or str in {"W", "L", "D"} (case-insensitive)
  - promotion: str (link or path), used to look up strength

"""

# ruff: noqa
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from math import exp
from typing import Any, Optional, Protocol, runtime_checkable, cast
import re

from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.application.services.performance_score import compute_ps_from_row


class HasResultPromotion(Protocol):
    result: Any  # Accept enum or string; mapped in result_to_r
    promotion: Any  # Accept str or object with attribute 'link'


@runtime_checkable
class HasLink(Protocol):
    link: str | None


class PromotionsRepoProtocol(Protocol):
    def get_strength_by_link(self, link: str, default: float) -> float:  # pragma: no cover - simple protocol
        ...


def result_to_r(result: Any) -> float | None:
    """Map fight result to r in {1.0 (W), 0.0 (L), 0.5 (D)}; return None for NC/unknown."""
    val: float | None = None
    if result is None:
        return None
    if isinstance(result, FightOutcome):
        mapping = {FightOutcome.WIN: 1.0, FightOutcome.LOSS: 0.0, FightOutcome.DRAW: 0.5}
        val = mapping.get(result)
    else:
        # Handle generic string-like outcomes
        try:
            s = str(result).strip().upper()
            first = s[:1]
        except Exception:
            first = ''
        if first:
            str_map = {'W': 1.0, 'L': 0.0, 'D': 0.5}
            val = str_map.get(first)
    return val


@dataclass
class EntryEloParams:
    S: float = 400.0
    nu: float = 8.0
    use_prior: bool = True
    k0: float = 1.0  # pseudo-fight count for prior at 0.5
    default_strength: float = 0.7


def gamma_growth(n: float, nu: float) -> float:
    """Gamma(N) = 1 - exp(-N/nu)."""
    if n <= 0:
        return 0.0
    return 1.0 - exp(-n / nu)


def compute_entry_elo(
    pre_ufc_bouts: Iterable[HasResultPromotion],
    promotions_repo: PromotionsRepoProtocol | None = None,
    params: EntryEloParams | None = None,
) -> tuple[float, float, float]:
    """Compute entry Elo from pre-UFC bouts and promotion strengths.

    Returns a tuple (Rseed, weff, Neff).
    """
    params = params or EntryEloParams()

    # Default repo: always returns default strength
    class _DefaultRepo:
        def get_strength_by_link(self, _link: str, default: float) -> float:
            return float(default)

    repo: PromotionsRepoProtocol = promotions_repo or _DefaultRepo()

    # Flatten common usage: [get_pre_ufc_record(...)] -> list of bouts
    items = list(pre_ufc_bouts)
    bouts_iter = list(items[0]) if len(items) == 1 and isinstance(items[0], (list, tuple)) else items

    win_weight_sum = 0.0
    weight_sum = 0.0
    # Aggregates

    for _idx, bout in enumerate(bouts_iter):
        r = result_to_r(getattr(bout, 'result', None))
        if r is None:
            # Ignore NC/unknown results
            continue
        promo_val = getattr(bout, 'promotion', None)
        if isinstance(promo_val, str):
            link = promo_val
        elif promo_val is not None and isinstance(promo_val, HasLink):
            link = promo_val.link or ''
        else:
            link = ''
        sp = repo.get_strength_by_link(link, default=params.default_strength)
        w = float(sp)
        win_weight_sum += w * r
        weight_sum += w
        # No tracing/verbose logging in streamlined implementation

    neff = weight_sum
    weff = win_weight_sum / weight_sum if weight_sum > 0 else 0.5
    # weff and neff ready for prior

    # Optional prior at 0.5 for micro-samples
    if params.use_prior:
        denom = weight_sum + params.k0
        weff = (win_weight_sum + 0.5 * params.k0) / denom if denom > 0 else 0.5

    g = gamma_growth(neff, params.nu)
    rseed = 1500.0 + params.S * (weff - 0.5) * g
    return rseed, weff, neff


def compute_starting_elo(
    pre_ufc_bouts: Iterable[HasResultPromotion],
    promotions_repo: PromotionsRepoProtocol | None = None,
    params: EntryEloParams | None = None,
) -> float:
    """Convenience wrapper returning a float starting Elo suitable for persistence."""
    rseed, _weff, _neff = compute_entry_elo(pre_ufc_bouts, promotions_repo=promotions_repo, params=params)
    return float(rseed)


# --- Constants (final for now) ---
SCALE_S = 400.0
CENTER = 1500.0
K0 = 28.0

# Outcome base and floors for winner
OUTCOME_B = {'KO_TKO': 0.925, 'SUB': 0.92, 'UD': 0.75, 'SD': 0.60, 'MD': 0.60, 'DQ': 0.60, 'TKO_DS': 0.88}
OUTCOME_FLOOR = {'KO_TKO': 0.045, 'SUB': 0.040, 'UD': 0.020, 'SD': 0.010, 'MD': 0.010, 'DQ': 0.010, 'TKO_DS': 0.035}
ALPHA = 0.5  # PS nudge slope


def logistic_expect(Ra: float, Rb: float, s: float = SCALE_S) -> float:
    # Ensure mypy sees a float by casting the denominator explicitly
    denom = 1.0 + 10.0 ** (-(Ra - Rb) / s)
    return 1.0 / cast(float, denom)


def method_class(method: str) -> str:
    m = (method or '').lower()
    if 'overturned' in m or 'could not continue' in m:
        return 'NC'
    if 'draw' in m or 'other' in m:
        return 'DRAW'
    if 'dq' in m or 'disqual' in m:
        return 'DQ'
    if 'tko' in m or 'technical knockout' in m or 'ko' in m:
        if 'doctor' in m:
            return 'TKO_DS'
        return 'KO_TKO'
    if 'sub' in m or 'technical submission' in m:
        return 'SUB'
    if 'decision' in m:
        if 'unanimous' in m:
            return 'UD'
        if 'split' in m:
            return 'SD'
        if 'majority' in m:
            return 'MD'
        return 'UD'
    return 'UD'


def winner_floor_and_base(mclass: str) -> tuple[float, float]:
    if mclass in OUTCOME_B:
        return OUTCOME_B[mclass], OUTCOME_FLOOR[mclass]
    return 0.75, 0.02


def compute_targets(PSw: float, Ew: float, method: str) -> tuple[float, float]:
    mclass = method_class(method)
    if mclass == 'DRAW':
        return 0.5, 0.5
    if mclass == 'NC':
        return Ew, 1.0 - Ew  # effectively no change if Ew ~ 0.5
    b, eps = winner_floor_and_base(mclass)
    m = max(0.0, PSw - 0.5)
    Y_base = max(0.55, min(0.99, b + ALPHA * m))
    Yw = max(Y_base, Ew + eps)
    return Yw, 1.0 - Yw


@dataclass
class KContext:
    rounds_scheduled: int  # total number of scheduled segments (incl. OT if present)
    method: str
    ufc_fights_before: int
    days_since_last_fight: int
    round_num: Optional[int] = None
    time_sec: Optional[int] = None
    # New: detailed schedule information
    total_scheduled_seconds: Optional[int] = None
    per_round_seconds: Optional[list[int]] = None
    # Title context (optional; used for transparency if needed)
    is_title_fight: Optional[bool] = None


def k_factor(ctx: KContext) -> float:
    mclass = method_class(ctx.method)
    K = K0
    # Rounds multiplier: scale with total scheduled time versus 3x5 baseline, capped at +5%
    if ctx.total_scheduled_seconds is not None and ctx.total_scheduled_seconds > 0:
        base = 900  # 3 x 5 min baseline
        cap = 1500  # 5 x 5 min cap
        delta = max(0, min(ctx.total_scheduled_seconds, cap) - base)
        span = max(1, cap - base)
        K *= 1.0 + 0.05 * (delta / span)
    else:
        # Fallback to legacy behavior when schedule unknown
        K *= 1.05 if ctx.rounds_scheduled == 5 else 1.00
    # Result class multiplier (include DQ and TKO Doctor's Stoppage)
    if mclass == 'KO_TKO':
        K *= 1.125
    elif mclass in {'SUB', 'TKO_DS'}:
        K *= 1.10
    elif mclass in {'SD', 'MD', 'DQ'}:
        K *= 0.95
    else:  # UD or other
        K *= 1.00
    # Experience
    if ctx.ufc_fights_before < 4:
        K *= 1.20
    elif ctx.ufc_fights_before <= 10:
        K *= 1.00
    else:
        K *= 0.85
    # Recency
    if ctx.days_since_last_fight <= 183:
        K *= 1.05
    elif ctx.days_since_last_fight > 548:
        K *= 0.90
    else:
        K *= 1.00
    # Finish-time boost
    if mclass in {'KO_TKO', 'TKO_DS', 'SUB'} and ctx.round_num and ctx.time_sec is not None:
        # Prefer true scheduled durations if available
        if ctx.per_round_seconds and len(ctx.per_round_seconds) > 0:
            idx = max(0, min(ctx.round_num - 1, len(ctx.per_round_seconds) - 1))
            prior = sum(ctx.per_round_seconds[:idx])
            cur_len = ctx.per_round_seconds[idx]
            t_finish = max(0, prior + min(ctx.time_sec, cur_len))
            T_max = max(1, sum(ctx.per_round_seconds))
        else:
            T_max = max(1, max(1, ctx.rounds_scheduled) * 300)
            t_finish = max(0, (ctx.round_num - 1) * 300 + ctx.time_sec)
        u = max(0.0, min(1.0, t_finish / T_max))
        K *= 1.00 + 0.25 * (1.0 - u) ** 2
    return K


def k_breakdown(ctx: KContext) -> dict[str, float | int | str | list[int]]:
    """Explain K calculation step-by-step for transparency/debugging.

    Returns a dict with base K, applied multipliers, method class, and final K.
    """
    details: dict[str, float | int | str | list[int]] = {}
    mclass = method_class(ctx.method)
    details['rounds_scheduled'] = ctx.rounds_scheduled
    if ctx.total_scheduled_seconds is not None:
        details['schedule_total_seconds'] = ctx.total_scheduled_seconds
    if ctx.per_round_seconds is not None:
        details['schedule_round_seconds'] = ctx.per_round_seconds
    details['method_class'] = mclass
    details['ufc_fights_before'] = ctx.ufc_fights_before
    details['days_since_last_fight'] = ctx.days_since_last_fight
    details['round_num'] = ctx.round_num or 0
    details['time_sec'] = ctx.time_sec or 0

    K = K0
    details['base_K0'] = K0

    # Rounds multiplier
    if ctx.total_scheduled_seconds is not None and ctx.total_scheduled_seconds > 0:
        base = 900
        cap = 1500
        delta = max(0, min(ctx.total_scheduled_seconds, cap) - base)
        span = max(1, cap - base)
        mult_rounds = 1.0 + 0.05 * (delta / span)
    else:
        mult_rounds = 1.05 if ctx.rounds_scheduled == 5 else 1.00
    K *= mult_rounds
    details['mult_rounds'] = mult_rounds

    # Result class multiplier
    if mclass == 'KO_TKO':
        mult_method = 1.125
    elif mclass in {'SUB', 'TKO_DS'}:
        mult_method = 1.10
    elif mclass in {'SD', 'MD', 'DQ'}:
        mult_method = 0.95
    else:
        mult_method = 1.00
    K *= mult_method
    details['mult_method'] = mult_method

    # Experience multiplier
    if ctx.ufc_fights_before < 4:
        mult_exp = 1.20
    elif ctx.ufc_fights_before <= 10:
        mult_exp = 1.00
    else:
        mult_exp = 0.85
    K *= mult_exp
    details['mult_experience'] = mult_exp

    # Recency multiplier
    if ctx.days_since_last_fight <= 183:
        mult_rec = 1.05
    elif ctx.days_since_last_fight > 548:
        mult_rec = 0.90
    else:
        mult_rec = 1.00
    K *= mult_rec
    details['mult_recency'] = mult_rec

    # Finish-time boost
    mult_finish = 1.00
    if mclass in {'KO_TKO', 'TKO_DS', 'SUB'} and ctx.round_num and ctx.time_sec is not None:
        if ctx.per_round_seconds and len(ctx.per_round_seconds) > 0:
            idx = max(0, min(ctx.round_num - 1, len(ctx.per_round_seconds) - 1))
            prior = sum(ctx.per_round_seconds[:idx])
            cur_len = ctx.per_round_seconds[idx]
            t_finish = max(0, prior + min(ctx.time_sec, cur_len))
            T_max = max(1, sum(ctx.per_round_seconds))
        else:
            T_max = max(1, max(1, ctx.rounds_scheduled) * 300)
            t_finish = max(0, (ctx.round_num - 1) * 300 + ctx.time_sec)
        u = max(0.0, min(1.0, t_finish / T_max))
        mult_finish = 1.00 + 0.25 * (1.0 - u) ** 2
        K *= mult_finish
        details['finish_u'] = u
    details['mult_finish'] = mult_finish

    details['K_final'] = K
    return details


@dataclass
class EloInputs:
    R1_before: float
    R2_before: float
    PS1: float
    PS2: float
    method: str
    rounds_scheduled: int
    round_num: Optional[int]
    time_sec: Optional[int]
    ufc_fights_before_1: int
    ufc_fights_before_2: int
    days_since_last_fight_1: int
    days_since_last_fight_2: int
    winner: int  # 1 or 2; 0 for draw; -1 for NC
    # New: true schedule details for accurate K
    schedule_total_seconds: Optional[int] = None
    schedule_round_seconds: Optional[list[int]] = None
    # Title context
    is_title_fight: Optional[bool] = None
    # 'capture' or 'defense' (optional; defaults to 'defense' if is_title_fight True and not provided)
    title_bout_type: Optional[str] = None


@dataclass
class EloOutputs:
    E1: float
    E2: float
    Y1: float
    Y2: float
    K1: float
    K2: float
    R1_after: float
    R2_after: float


def update_elo(inputs: EloInputs) -> EloOutputs:
    E1 = logistic_expect(inputs.R1_before, inputs.R2_before, SCALE_S)
    E2 = 1.0 - E1

    if inputs.winner == 0:  # draw
        Y1, Y2 = 0.5, 0.5
    elif inputs.winner < 0:  # NC
        Y1, Y2 = E1, E2
    else:
        if inputs.winner == 1:
            Yw, Yl = compute_targets(inputs.PS1, E1, inputs.method)
            Y1, Y2 = Yw, Yl
        else:
            Yw, Yl = compute_targets(inputs.PS2, E2, inputs.method)
            Y1, Y2 = Yl, Yw

    # K factors
    K1 = k_factor(
        KContext(
            rounds_scheduled=inputs.rounds_scheduled,
            method=inputs.method,
            ufc_fights_before=inputs.ufc_fights_before_1,
            days_since_last_fight=inputs.days_since_last_fight_1,
            round_num=inputs.round_num,
            time_sec=inputs.time_sec,
            total_scheduled_seconds=inputs.schedule_total_seconds,
            per_round_seconds=inputs.schedule_round_seconds,
            is_title_fight=inputs.is_title_fight,
        )
    )
    K2 = k_factor(
        KContext(
            rounds_scheduled=inputs.rounds_scheduled,
            method=inputs.method,
            ufc_fights_before=inputs.ufc_fights_before_2,
            days_since_last_fight=inputs.days_since_last_fight_2,
            round_num=inputs.round_num,
            time_sec=inputs.time_sec,
            total_scheduled_seconds=inputs.schedule_total_seconds,
            per_round_seconds=inputs.schedule_round_seconds,
            is_title_fight=inputs.is_title_fight,
        )
    )

    # Title stakes multiplier on K (applied once, after base multipliers)
    def _stakes_multiplier(is_title: bool | None, bout_type: str | None, Ew: float, PSw: float) -> float:
        if not is_title:
            return 1.0
        # Explicit opt-out for special cases (e.g., stripped champion wins where belt stays vacant)
        if (bout_type or '').lower() in {'none', 'neither', 'n/a'}:
            return 1.0
        # opponent quality signal (pre-fight parity)
        q_comp = 2.0 * min(Ew, 1.0 - Ew)
        # guardrail for big mismatches
        if Ew >= 0.85:
            q_comp = 0.0
        # decisiveness from performance score
        D = min(1.0, 2.0 * max(0.0, PSw - 0.5))
        avg = 0.5 * (q_comp + D)
        if (bout_type or '').lower() == 'capture':
            f = 1.02 + 0.03 * avg
            return max(1.02, min(1.05, f))
        # default to defense
        f = 1.01 + 0.02 * avg
        return max(1.01, min(1.03, f))

    if inputs.is_title_fight and inputs.winner in (1, 2):
        # Winner-centric signals
        Ew = E1 if inputs.winner == 1 else E2
        PSw = inputs.PS1 if inputs.winner == 1 else inputs.PS2
        f_title = _stakes_multiplier(inputs.is_title_fight, inputs.title_bout_type, Ew, PSw)
        K1 *= f_title
        K2 *= f_title

    # Safety cap on per-fighter K to avoid rare extreme jumps after multipliers
    MAX_K_MULT = 1.50  # cap at 1.5x base K0
    K1 = min(K1, MAX_K_MULT * K0)
    K2 = min(K2, MAX_K_MULT * K0)

    # Elo updates (not necessarily zero-sum when K1 != K2)
    R1_after = inputs.R1_before + K1 * (Y1 - E1)
    R2_after = inputs.R2_before + K2 * (Y2 - E2)

    return EloOutputs(E1=E1, E2=E2, Y1=Y1, Y2=Y2, K1=K1, K2=K2, R1_after=R1_after, R2_after=R2_after)


def _parse_schedule_from_time_format(tf_raw: str) -> tuple[int, list[int], Optional[int]]:
    """Parse UFCStats time_format into (rounds_count, per_round_seconds, total_seconds).

    Supports examples:
    - "3 Rnd (5-5-5)"
    - "5 Rnd (5-5-5-5-5)"
    - "1 Rnd (12)"
    - "1 Rnd + OT (12-3)"
    - "1 Rnd + 2OT (15-3-3)"
    - "No Time Limit"
    """
    tf = (tf_raw or '').strip().lower()
    if not tf:
        return 3, [300, 300, 300], 900
    if 'no time limit' in tf:
        return 3, [], None
    # Extract numbers inside parentheses as minutes tokens
    mins: list[int] = []
    m = re.search(r'\(([^)]*)\)', tf)
    if m:
        body = m.group(1)
        for tok in body.split('-'):
            tok = tok.strip()
            if tok.isdigit():
                mins.append(int(tok))
    # Determine number of segments
    if mins:
        per_round_sec = [max(0, mi * 60) for mi in mins]
        total = sum(per_round_sec)
        return len(per_round_sec), per_round_sec, total
    # Fallback: infer from leading "N Rnd" without details; assume 5-min rounds
    m2 = re.search(r'\b(\d+)\s*rnd\b', tf)
    if m2:
        n = int(m2.group(1))
        n = max(1, min(n, 5))
        per_round_sec = [300] * n
        return n, per_round_sec, sum(per_round_sec)
    # Default to 3 x 5
    return 3, [300, 300, 300], 900


def _rounds_scheduled_from_row(row: dict[str, Any]) -> int:
    n, _secs, _total = _parse_schedule_from_time_format(row.get('time_format') or '')
    # Title-fight override: if ambiguous 3 vs 5 and flagged title, bump to 5 segments
    if n in (0, 3) and bool(row.get('is_title_fight', False)):
        return 5
    return n


def _winner_from_row(row: dict[str, Any]) -> int:
    r1 = (row.get('fighter1_result') or '').strip().upper()
    r2 = (row.get('fighter2_result') or '').strip().upper()
    if r1 == 'W' and r2 != 'W':
        return 1
    if r2 == 'W' and r1 != 'W':
        return 2
    if r1.startswith('D') or r2.startswith('D'):
        return 0  # draw
    if r1.startswith('NC') or r2.startswith('NC'):
        return -1  # no contest
    return 0  # default to draw if unclear


def compute_elo_from_row(row: dict[str, Any], extras: dict[str, Any]) -> EloOutputs:
    """Compute ELO update directly from a fight row and required extras.

    Required row keys:
      - method, time_format, is_title_fight, round_num, time_sec,
      - fighter1_result, fighter2_result
      - fighter1_* and fighter2_* stat columns as used by performance_score.compute_ps_from_row

    Required extras keys:
      - R1_before, R2_before
      - ufc_fights_before_1, ufc_fights_before_2
      - days_since_last_fight_1, days_since_last_fight_2
    """
    ps = compute_ps_from_row(row)
    PS1 = float(ps['PS1'])
    PS2 = float(ps['PS2'])

    rounds_scheduled = _rounds_scheduled_from_row(row)
    # Parse detailed schedule for K calculations
    _n, per_round_seconds, total_scheduled_seconds = _parse_schedule_from_time_format(row.get('time_format') or '')
    round_num = row.get('round_num')
    time_sec = row.get('time_sec')
    try:
        round_num = int(round_num) if round_num is not None else None
    except Exception:
        round_num = None
    try:
        time_sec = int(time_sec) if time_sec is not None else None
    except Exception:
        time_sec = None

    winner = _winner_from_row(row)
    method = row.get('method', '')

    inp = EloInputs(
        R1_before=float(extras['R1_before']),
        R2_before=float(extras['R2_before']),
        PS1=PS1,
        PS2=PS2,
        method=method,
        rounds_scheduled=rounds_scheduled,
        round_num=round_num,
        time_sec=time_sec,
        ufc_fights_before_1=int(extras['ufc_fights_before_1']),
        ufc_fights_before_2=int(extras['ufc_fights_before_2']),
        days_since_last_fight_1=int(extras['days_since_last_fight_1']),
        days_since_last_fight_2=int(extras['days_since_last_fight_2']),
        winner=winner,
        schedule_total_seconds=total_scheduled_seconds,
        schedule_round_seconds=per_round_seconds,
        is_title_fight=bool(row.get('is_title_fight', False)),
        title_bout_type=str(row.get('title_bout_type', '') or '').lower() or None,
    )
    return update_elo(inp)
