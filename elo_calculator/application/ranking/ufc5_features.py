"""UFC 5 bout-level feature extraction.

Converts raw per-round or fight-total statistics into the normalised feature
set consumed by System H (``UFC5BoutFeatures``).  Features align with the
stat-plan formulas: per-15-minute rates, accuracy percentages, share
distributions, and late-game ratios.
"""

from __future__ import annotations

from elo_calculator.application.ranking.types import FighterRoundStats, UFC5BoutFeatures

_EPS = 1e-9
_MIN_ROUNDS_FOR_PHASE_RATIO = 3
_EARLY_ROUND_CUTOFF = 2


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_bout_features(
    fighter_stats: FighterRoundStats,
    opponent_stats: FighterRoundStats,
    fight_minutes: float,
    *,
    round_stats_list: list[tuple[FighterRoundStats, FighterRoundStats, int]] | None = None,
    total_fight_seconds: float | None = None,
) -> UFC5BoutFeatures:
    """Build a ``UFC5BoutFeatures`` instance from one bout's data.

    Parameters
    ----------
    fighter_stats:
        Aggregated fight-level stats for *this* fighter.
    opponent_stats:
        Aggregated fight-level stats for the *opponent*.
    fight_minutes:
        Total elapsed fight time in minutes.
    round_stats_list:
        Optional list of ``(fighter_round, opponent_round, round_seconds)``
        tuples.  When provided, late-game and early-game ratios are computed.
    total_fight_seconds:
        Total fight duration in seconds (for control-share).  Falls back to
        ``fight_minutes * 60`` when *None*.
    """
    fm = max(fight_minutes, _EPS)
    tfs = total_fight_seconds if total_fight_seconds is not None else fm * 60.0
    tfs = max(tfs, _EPS)

    f = fighter_stats
    o = opponent_stats

    # --- Striking accuracy / defence ---
    sig_acc = _safe_rate(f.sig_landed, f.sig_attempted)
    sig_def = 1.0 - _safe_rate(o.sig_landed, o.sig_attempted)
    head_evasion = 1.0 - _safe_rate(o.head_landed, max(o.sig_attempted, _EPS))
    slpm = f.sig_landed / fm
    sapm = o.sig_landed / fm
    str_diff = slpm - sapm

    # --- Takedown / grappling ---
    td15 = 15.0 * f.td_landed / fm
    td_acc = _safe_rate(f.td_landed, f.td_attempted)
    td_def = 1.0 - _safe_rate(o.td_landed, o.td_attempted)
    sub15 = 15.0 * f.sub_attempts / fm
    opp_sub15 = 15.0 * o.sub_attempts / fm
    ctrl_share = f.ctrl_seconds / tfs
    opp_ctrl_share = o.ctrl_seconds / tfs
    rev15 = 15.0 * f.rev / fm
    opp_rev15 = 15.0 * o.rev / fm

    # --- Striking distribution (shares of sig_landed) ---
    sl = max(f.sig_landed, _EPS)
    head_share = f.head_landed / sl
    body_share = f.body_landed / sl
    leg_share = f.leg_landed / sl
    dist_share = f.distance_landed / sl
    clinch_share = f.clinch_landed / sl
    ground_share = f.ground_landed / sl

    # --- Per-minute rates by target/position ---
    head_sig_per_min = f.head_landed / fm
    body_sig_per_min = f.body_landed / fm
    leg_sig_per_min = f.leg_landed / fm
    dist_sig_per_min = f.distance_landed / fm
    clinch_sig_per_min = f.clinch_landed / fm
    ground_sig_per_min = f.ground_landed / fm

    # --- Knockdowns / damage ---
    kd15 = 15.0 * f.kd / fm
    kd_abs15 = 15.0 * o.kd / fm
    head_abs15 = o.head_landed / fm
    body_abs15 = o.body_landed / fm
    leg_abs15 = o.leg_landed / fm

    # --- Late-game / early-game ratios (tier A only) ---
    late_output_ratio: float | None = None
    late_abs_ratio: float | None = None
    early_output_ratio: float | None = None
    if round_stats_list is not None and len(round_stats_list) >= _MIN_ROUNDS_FOR_PHASE_RATIO:
        late_output_ratio, late_abs_ratio, early_output_ratio = _compute_round_phase_ratios(round_stats_list)

    return UFC5BoutFeatures(
        fight_minutes=fight_minutes,
        sig_acc=sig_acc,
        sig_def=sig_def,
        head_evasion=head_evasion,
        slpm=slpm,
        sapm=sapm,
        str_diff=str_diff,
        td15=td15,
        td_acc=td_acc,
        td_def=td_def,
        sub15=sub15,
        opp_sub15=opp_sub15,
        ctrl_share=ctrl_share,
        opp_ctrl_share=opp_ctrl_share,
        rev15=rev15,
        opp_rev15=opp_rev15,
        head_share=head_share,
        body_share=body_share,
        leg_share=leg_share,
        dist_share=dist_share,
        clinch_share=clinch_share,
        ground_share=ground_share,
        head_sig_per_min=head_sig_per_min,
        body_sig_per_min=body_sig_per_min,
        leg_sig_per_min=leg_sig_per_min,
        dist_sig_per_min=dist_sig_per_min,
        clinch_sig_per_min=clinch_sig_per_min,
        ground_sig_per_min=ground_sig_per_min,
        kd15=kd15,
        kd_abs15=kd_abs15,
        head_abs15=head_abs15,
        body_abs15=body_abs15,
        leg_abs15=leg_abs15,
        late_output_ratio=late_output_ratio,
        late_abs_ratio=late_abs_ratio,
        early_output_ratio=early_output_ratio,
    )


# ---------------------------------------------------------------------------
# Aggregate fight-totals from per-round stats
# ---------------------------------------------------------------------------


def aggregate_round_stats(round_stats: list[FighterRoundStats]) -> FighterRoundStats:
    """Sum a list of per-round stats into a single fight-total."""
    if not round_stats:
        return FighterRoundStats()
    return FighterRoundStats(
        kd=sum(r.kd for r in round_stats),
        sig_landed=sum(r.sig_landed for r in round_stats),
        sig_attempted=sum(r.sig_attempted for r in round_stats),
        total_landed=sum(r.total_landed for r in round_stats),
        total_attempted=sum(r.total_attempted for r in round_stats),
        td_landed=sum(r.td_landed for r in round_stats),
        td_attempted=sum(r.td_attempted for r in round_stats),
        sub_attempts=sum(r.sub_attempts for r in round_stats),
        rev=sum(r.rev for r in round_stats),
        ctrl_seconds=sum(r.ctrl_seconds for r in round_stats),
        head_landed=sum(r.head_landed for r in round_stats),
        body_landed=sum(r.body_landed for r in round_stats),
        leg_landed=sum(r.leg_landed for r in round_stats),
        distance_landed=sum(r.distance_landed for r in round_stats),
        clinch_landed=sum(r.clinch_landed for r in round_stats),
        ground_landed=sum(r.ground_landed for r in round_stats),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_rate(numerator: float, denominator: float) -> float:
    if denominator <= _EPS:
        return 0.0
    return numerator / denominator


def _per15(count: float, minutes: float) -> float:
    return 15.0 * count / max(minutes, _EPS)


def _round_output(stats: FighterRoundStats) -> float:
    """Total offensive output in a round."""
    return stats.sig_landed + stats.td_landed + stats.sub_attempts


def _compute_round_phase_ratios(
    round_stats_list: list[tuple[FighterRoundStats, FighterRoundStats, int]],
) -> tuple[float | None, float | None, float | None]:
    """Compute late-game vs early-game output and absorption ratios.

    Returns (late_output_ratio, late_abs_ratio, early_output_ratio).
    """
    n = len(round_stats_list)
    if n < _MIN_ROUNDS_FOR_PHASE_RATIO:
        return None, None, None

    # Early = rounds 1-2, Late = rounds 3+
    early_output = 0.0
    early_abs = 0.0
    early_seconds = 0.0
    late_output = 0.0
    late_abs = 0.0
    late_seconds = 0.0

    for idx, (fstats, ostats, round_secs) in enumerate(round_stats_list):
        out = _round_output(fstats)
        absorbed = ostats.sig_landed
        if idx < _EARLY_ROUND_CUTOFF:
            early_output += out
            early_abs += absorbed
            early_seconds += round_secs
        else:
            late_output += out
            late_abs += absorbed
            late_seconds += round_secs

    early_opm = early_output / max(early_seconds / 60.0, _EPS)
    late_opm = late_output / max(late_seconds / 60.0, _EPS)
    late_output_ratio = late_opm / max(early_opm, _EPS)

    early_apm = early_abs / max(early_seconds / 60.0, _EPS)
    late_apm = late_abs / max(late_seconds / 60.0, _EPS)
    late_abs_ratio = late_apm / max(early_apm, _EPS)

    early_output_ratio = early_opm / max(late_opm, _EPS)

    return late_output_ratio, late_abs_ratio, early_output_ratio
