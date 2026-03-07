"""System G GOAT analysis helpers.

This module computes GOAT pillar metrics and a composite score for MMA fighters.
It is designed to be imported by the System G pipeline, not run as a script.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from statistics import median, stdev

from loguru import logger
from sqlalchemy import Connection, text

W_PEAK = 0.25
W_LONGEVITY = 0.20
W_SOS = 0.15
W_DOMINANCE = 0.20
W_CHAMPIONSHIP = 0.10
W_CONSISTENCY = 0.10

MIN_MMA_BOUTS = 8
BASELINE_UNIFIED = 1500.0
ELITE_PERCENTILE = 0.90
UPSET_PROB_THRESHOLD = 0.40
TOP_OPPONENT_PERCENTILE = 0.75
MIN_DELTAS_FOR_STD = 3
MIN_STDEV_VALUES = 2
MIN_TIMELINE_POINTS = 2
ROLLING_POINTS = 3
WINDOW_DAYS = 365
MAX_GAP_DAYS = 180
Z_CAP = 4.0
MAD_TO_STD = 1.4826
EPSILON = 1e-9


@dataclass(frozen=True)
class FighterBout:
    bout_id: str
    event_date: date
    opponent_id: str | None
    outcome: str
    is_title_fight: bool
    method_group: str | None


@dataclass
class FighterProfile:
    fighter_id: str
    display_name: str

    mma_bouts: int = 0
    mma_wins: int = 0
    mma_losses: int = 0
    mma_draws: int = 0
    mma_nc: int = 0
    mma_finish_wins: int = 0
    mma_decision_wins: int = 0
    mma_ko_wins: int = 0
    mma_sub_wins: int = 0
    title_fights: int = 0
    title_wins: int = 0
    title_defenses: int = 0

    win_rate: float = 0.0
    finish_rate: float = 0.0
    title_win_rate: float = 0.0
    max_win_streak: int = 0

    career_start: date | None = None
    career_end: date | None = None
    career_span_years: float = 0.0

    elo_rating: float = 0.0
    glicko_rating: float = 0.0
    dynamic_rating: float = 0.0
    stacked_rating: float = 0.0
    ewr_rating: float = 0.0
    unified_rating: float = 0.0

    peak_elo: float = 0.0
    peak_glicko: float = 0.0
    peak_dynamic: float = 0.0
    peak_unified: float = 0.0
    peak_3fight_avg: float = 0.0
    peak_dominance_window: float = 0.0

    auc_above_baseline: float = 0.0
    time_in_elite_days: float = 0.0

    avg_opponent_rating: float = 0.0
    avg_opponent_rating_in_wins: float = 0.0
    top_opp_wins: int = 0
    upset_wins: int = 0
    rating_variance: float = 0.0

    avg_qow: float = 0.0
    avg_ps_fight: float = 0.0

    peak_score: float = 0.0
    longevity_score: float = 0.0
    sos_score: float = 0.0
    dominance_score: float = 0.0
    championship_score: float = 0.0
    consistency_score: float = 0.0

    z_peak: float = 0.0
    z_longevity: float = 0.0
    z_sos: float = 0.0
    z_dominance: float = 0.0
    z_championship: float = 0.0
    z_consistency: float = 0.0

    goat_score: float = 0.0


@dataclass(frozen=True)
class BoutSummaryStats:
    wins: int
    losses: int
    draws: int
    ncs: int
    finish_wins: int
    decision_wins: int
    ko_wins: int
    sub_wins: int
    title_fights: int
    title_wins: int
    max_streak: int


def compute_goat_profiles(conn: Connection, *, min_mma_bouts: int = MIN_MMA_BOUTS) -> dict[str, FighterProfile]:
    profiles = _load_profiles(conn)
    bouts_by_fighter = _load_mma_bouts(conn)

    _apply_bout_stats(profiles, bouts_by_fighter)
    _load_latest_ratings(conn, profiles)
    _load_peak_ratings(conn, profiles)
    _load_ps_aggregates(conn, profiles)

    timelines = _load_unified_timeline(conn)
    _apply_longevity_metrics(profiles, timelines)

    delta_map = _load_unified_delta_map(conn)
    _apply_sos_metrics(profiles, bouts_by_fighter, delta_map)

    qualified = {
        fighter_id: profile
        for fighter_id, profile in profiles.items()
        if profile.mma_bouts >= min_mma_bouts and profile.mma_wins > 0
    }
    logger.info('Computed GOAT inputs for {} qualified fighters.', len(qualified))

    _compute_peak_pillar(qualified)
    _compute_longevity_pillar(qualified)
    _compute_sos_pillar(qualified)
    _compute_dominance_pillar(qualified)
    _compute_championship_pillar(qualified)
    _compute_consistency_pillar(qualified)
    _compute_composite(qualified)
    return qualified


def _load_profiles(conn: Connection) -> dict[str, FighterProfile]:
    rows = conn.execute(text('SELECT fighter_id, display_name FROM dim_fighter')).fetchall()
    return {
        str(row.fighter_id): FighterProfile(fighter_id=str(row.fighter_id), display_name=row.display_name or '')
        for row in rows
    }


def _load_mma_bouts(conn: Connection) -> dict[str, list[FighterBout]]:
    rows = conn.execute(
        text(
            """
            SELECT
                bp.bout_id,
                bp.fighter_id,
                bp.outcome_key,
                e.event_date,
                b.is_title_fight,
                b.method_group,
                bp2.fighter_id AS opponent_id
            FROM fact_bout_participant bp
            JOIN fact_bout b ON b.bout_id = bp.bout_id
            JOIN fact_event e ON e.event_id = b.event_id
            JOIN dim_sport s ON s.sport_id = b.sport_id
            LEFT JOIN fact_bout_participant bp2 ON bp2.bout_id = bp.bout_id AND bp2.fighter_id != bp.fighter_id
            WHERE s.sport_key = 'mma'
            ORDER BY bp.fighter_id, e.event_date, bp.bout_id
            """
        )
    ).fetchall()

    bouts: dict[str, list[FighterBout]] = defaultdict(list)
    for row in rows:
        fighter_id = str(row.fighter_id)
        bouts[fighter_id].append(
            FighterBout(
                bout_id=str(row.bout_id),
                event_date=row.event_date,
                opponent_id=str(row.opponent_id) if row.opponent_id else None,
                outcome=(row.outcome_key or '').upper(),
                is_title_fight=bool(row.is_title_fight),
                method_group=row.method_group,
            )
        )
    return dict(bouts)


def _apply_bout_stats(profiles: dict[str, FighterProfile], bouts_by_fighter: dict[str, list[FighterBout]]) -> None:
    for fighter_id, bouts in bouts_by_fighter.items():
        profile = profiles.get(fighter_id)
        if profile is None:
            continue
        _summarize_bouts(profile, bouts)


def _summarize_bouts(profile: FighterProfile, bouts: list[FighterBout]) -> None:
    profile.mma_bouts = len(bouts)
    if not bouts:
        return

    wins = losses = draws = ncs = 0
    finish_wins = decision_wins = ko_wins = sub_wins = 0
    title_fights = title_wins = 0
    current_streak = max_streak = 0

    for bout in bouts:
        outcome = bout.outcome
        if bout.is_title_fight:
            title_fights += 1
            if outcome == 'W':
                title_wins += 1

        if outcome == 'W':
            wins += 1
            current_streak += 1
            max_streak = max(max_streak, current_streak)
            if bout.method_group in {'TKO', 'KO', 'SUB'}:
                finish_wins += 1
            if bout.method_group in {'TKO', 'KO'}:
                ko_wins += 1
            if bout.method_group == 'SUB':
                sub_wins += 1
            if bout.method_group == 'DEC':
                decision_wins += 1
        elif outcome == 'L':
            losses += 1
            current_streak = 0
        elif outcome == 'D':
            draws += 1
            current_streak = 0
        elif outcome == 'NC':
            ncs += 1

    _apply_bout_summary(
        profile,
        bouts,
        BoutSummaryStats(
            wins=wins,
            losses=losses,
            draws=draws,
            ncs=ncs,
            finish_wins=finish_wins,
            decision_wins=decision_wins,
            ko_wins=ko_wins,
            sub_wins=sub_wins,
            title_fights=title_fights,
            title_wins=title_wins,
            max_streak=max_streak,
        ),
    )


def _apply_bout_summary(profile: FighterProfile, bouts: list[FighterBout], stats: BoutSummaryStats) -> None:
    profile.mma_wins = stats.wins
    profile.mma_losses = stats.losses
    profile.mma_draws = stats.draws
    profile.mma_nc = stats.ncs
    profile.mma_finish_wins = stats.finish_wins
    profile.mma_decision_wins = stats.decision_wins
    profile.mma_ko_wins = stats.ko_wins
    profile.mma_sub_wins = stats.sub_wins
    profile.title_fights = stats.title_fights
    profile.title_wins = stats.title_wins
    profile.title_defenses = max(0, stats.title_wins - 1)
    profile.max_win_streak = stats.max_streak

    profile.career_start = bouts[0].event_date
    profile.career_end = bouts[-1].event_date
    span_days = max(0, (profile.career_end - profile.career_start).days)
    profile.career_span_years = span_days / 365.25

    total_results = stats.wins + stats.losses
    profile.win_rate = (stats.wins / total_results) if total_results > 0 else 0.0
    profile.finish_rate = (stats.finish_wins / stats.wins) if stats.wins > 0 else 0.0
    profile.title_win_rate = (stats.title_wins / stats.title_fights) if stats.title_fights > 0 else 0.0


def _load_latest_ratings(conn: Connection, profiles: dict[str, FighterProfile]) -> None:
    rows = conn.execute(
        text(
            """
            SELECT rs.system_key, s.fighter_id, s.rating_mean
            FROM fact_rating_snapshot s
            JOIN dim_rating_system rs ON rs.system_id = s.system_id
            WHERE s.as_of_date = (SELECT MAX(as_of_date) FROM fact_rating_snapshot)
            """
        )
    ).fetchall()

    for row in rows:
        profile = profiles.get(str(row.fighter_id))
        if profile is None:
            continue
        rating = float(row.rating_mean) if row.rating_mean else 0.0
        key = row.system_key
        if key == 'elo_ps':
            profile.elo_rating = rating
        elif key == 'glicko2_ps':
            profile.glicko_rating = rating
        elif key == 'dynamic_factor_bt':
            profile.dynamic_rating = rating
        elif key == 'stacked_logit_mixture':
            profile.stacked_rating = rating
        elif key == 'expected_win_rate_pool':
            profile.ewr_rating = rating
        elif key == 'unified_composite_elo':
            profile.unified_rating = rating


def _load_peak_ratings(conn: Connection, profiles: dict[str, FighterProfile]) -> None:
    rows = conn.execute(
        text(
            """
            SELECT rs.system_key, s.fighter_id, MAX(s.rating_mean) AS peak_rating
            FROM fact_rating_snapshot s
            JOIN dim_rating_system rs ON rs.system_id = s.system_id
            WHERE rs.system_key IN ('elo_ps', 'glicko2_ps', 'dynamic_factor_bt', 'unified_composite_elo')
            GROUP BY rs.system_key, s.fighter_id
            """
        )
    ).fetchall()

    for row in rows:
        profile = profiles.get(str(row.fighter_id))
        if profile is None:
            continue
        peak = float(row.peak_rating) if row.peak_rating else 0.0
        if row.system_key == 'elo_ps':
            profile.peak_elo = peak
        elif row.system_key == 'glicko2_ps':
            profile.peak_glicko = peak
        elif row.system_key == 'dynamic_factor_bt':
            profile.peak_dynamic = peak
        elif row.system_key == 'unified_composite_elo':
            profile.peak_unified = peak


def _load_ps_aggregates(conn: Connection, profiles: dict[str, FighterProfile]) -> None:
    rows = conn.execute(
        text(
            """
            SELECT fighter_id, AVG(ps_fight) AS avg_ps, AVG(quality_of_win) AS avg_qow
            FROM fact_fight_ps
            GROUP BY fighter_id
            """
        )
    ).fetchall()

    for row in rows:
        profile = profiles.get(str(row.fighter_id))
        if profile is None:
            continue
        profile.avg_ps_fight = float(row.avg_ps) if row.avg_ps else 0.0
        profile.avg_qow = float(row.avg_qow) if row.avg_qow else 0.0


def _load_unified_timeline(conn: Connection) -> dict[str, list[tuple[date, float]]]:
    rows = conn.execute(
        text(
            """
            SELECT s.fighter_id, s.as_of_date, s.rating_mean
            FROM fact_rating_snapshot s
            JOIN dim_rating_system rs ON rs.system_id = s.system_id
            WHERE rs.system_key = 'unified_composite_elo'
            ORDER BY s.fighter_id, s.as_of_date
            """
        )
    ).fetchall()

    timeline: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for row in rows:
        timeline[str(row.fighter_id)].append((row.as_of_date, float(row.rating_mean) if row.rating_mean else 0.0))
    return dict(timeline)


def _apply_longevity_metrics(
    profiles: dict[str, FighterProfile], timelines: dict[str, list[tuple[date, float]]]
) -> None:
    peaks = [profile.peak_unified for profile in profiles.values() if profile.peak_unified > BASELINE_UNIFIED]
    elite_threshold = _percentile(peaks, ELITE_PERCENTILE, fallback=1700.0)

    for fighter_id, timeline in timelines.items():
        profile = profiles.get(fighter_id)
        if profile is None or len(timeline) < MIN_TIMELINE_POINTS:
            continue
        _populate_timeline_metrics(profile, timeline, elite_threshold)


def _populate_timeline_metrics(
    profile: FighterProfile, timeline: list[tuple[date, float]], elite_threshold: float
) -> None:
    ratings = [rating for _, rating in timeline]
    profile.peak_3fight_avg = _rolling_peak(ratings, ROLLING_POINTS)
    profile.peak_dominance_window = _window_peak_average(timeline, WINDOW_DAYS)

    auc = 0.0
    elite_days = 0.0
    previous_date, previous_rating = timeline[0]
    for current_date, current_rating in timeline[1:]:
        gap_days = (current_date - previous_date).days
        capped_gap = min(gap_days, MAX_GAP_DAYS) if gap_days > 0 else 0
        if capped_gap > 0:
            midpoint = (previous_rating + current_rating) / 2.0
            auc += max(0.0, midpoint - BASELINE_UNIFIED) * capped_gap
            if midpoint >= elite_threshold:
                elite_days += capped_gap
        previous_date, previous_rating = current_date, current_rating

    profile.auc_above_baseline = auc
    profile.time_in_elite_days = elite_days


def _load_unified_delta_map(conn: Connection) -> dict[tuple[str, str], tuple[float, float, float]]:
    rows = conn.execute(
        text(
            """
            SELECT rd.fighter_id, rd.bout_id, rd.pre_rating, rd.expected_win_prob, rd.delta_rating
            FROM fact_rating_delta rd
            JOIN dim_rating_system rs ON rs.system_id = rd.system_id
            WHERE rs.system_key = 'unified_composite_elo'
            """
        )
    ).fetchall()

    delta_map: dict[tuple[str, str], tuple[float, float, float]] = {}
    for row in rows:
        key = (str(row.bout_id), str(row.fighter_id))
        delta_map[key] = (
            float(row.pre_rating) if row.pre_rating else 0.0,
            float(row.expected_win_prob) if row.expected_win_prob else 0.5,
            float(row.delta_rating) if row.delta_rating else 0.0,
        )
    return delta_map


def _apply_sos_metrics(
    profiles: dict[str, FighterProfile],
    bouts_by_fighter: dict[str, list[FighterBout]],
    delta_map: dict[tuple[str, str], tuple[float, float, float]],
) -> None:
    for fighter_id, bouts in bouts_by_fighter.items():
        profile = profiles.get(fighter_id)
        if profile is None:
            continue
        _apply_fighter_sos(profile, bouts, delta_map)

    averages = [profile.avg_opponent_rating for profile in profiles.values() if profile.avg_opponent_rating > 0]
    top_threshold = _percentile(averages, TOP_OPPONENT_PERCENTILE, fallback=1600.0)
    _apply_top_opponent_wins(profiles, bouts_by_fighter, delta_map, top_threshold)


def _apply_fighter_sos(
    profile: FighterProfile, bouts: list[FighterBout], delta_map: dict[tuple[str, str], tuple[float, float, float]]
) -> None:
    opponent_ratings: list[float] = []
    opponent_ratings_in_wins: list[float] = []
    deltas: list[float] = []
    upset_wins = 0

    for bout in bouts:
        if not bout.opponent_id:
            continue
        opp_pre, my_expected, my_delta = delta_map.get((bout.bout_id, bout.opponent_id), (0.0, 0.5, 0.0))
        if opp_pre <= 0:
            continue

        opponent_ratings.append(opp_pre)
        deltas.append(my_delta)
        if bout.outcome == 'W':
            opponent_ratings_in_wins.append(opp_pre)
            if my_expected < UPSET_PROB_THRESHOLD:
                upset_wins += 1

    profile.avg_opponent_rating = _safe_mean(opponent_ratings)
    profile.avg_opponent_rating_in_wins = _safe_mean(opponent_ratings_in_wins)
    profile.upset_wins = upset_wins
    if len(deltas) >= MIN_DELTAS_FOR_STD:
        profile.rating_variance = stdev(deltas)


def _apply_top_opponent_wins(
    profiles: dict[str, FighterProfile],
    bouts_by_fighter: dict[str, list[FighterBout]],
    delta_map: dict[tuple[str, str], tuple[float, float, float]],
    top_threshold: float,
) -> None:
    for fighter_id, bouts in bouts_by_fighter.items():
        profile = profiles.get(fighter_id)
        if profile is None:
            continue

        top_wins = 0
        for bout in bouts:
            if bout.outcome != 'W' or not bout.opponent_id:
                continue
            opp_pre, _, _ = delta_map.get((bout.bout_id, bout.opponent_id), (0.0, 0.5, 0.0))
            if opp_pre >= top_threshold:
                top_wins += 1
        profile.top_opp_wins = top_wins


def _compute_peak_pillar(profiles: dict[str, FighterProfile]) -> None:
    attrs = ['peak_unified', 'peak_dynamic', 'peak_elo', 'peak_glicko', 'peak_3fight_avg', 'peak_dominance_window']
    weights = [0.30, 0.25, 0.15, 0.10, 0.10, 0.10]
    _compute_weighted_pillar(profiles, attrs, weights, 'peak_score', 'z_peak')


def _compute_longevity_pillar(profiles: dict[str, FighterProfile]) -> None:
    attrs = ['auc_above_baseline', 'time_in_elite_days', 'career_span_years', 'mma_bouts']
    weights = [0.40, 0.30, 0.15, 0.15]
    _compute_weighted_pillar(profiles, attrs, weights, 'longevity_score', 'z_longevity')


def _compute_sos_pillar(profiles: dict[str, FighterProfile]) -> None:
    attrs = ['avg_opponent_rating', 'avg_opponent_rating_in_wins', 'top_opp_wins', 'upset_wins']
    weights = [0.35, 0.30, 0.20, 0.15]
    _compute_weighted_pillar(profiles, attrs, weights, 'sos_score', 'z_sos')


def _compute_dominance_pillar(profiles: dict[str, FighterProfile]) -> None:
    attrs = ['win_rate', 'finish_rate', 'avg_qow', 'avg_ps_fight']
    weights = [0.30, 0.25, 0.25, 0.20]
    _compute_weighted_pillar(profiles, attrs, weights, 'dominance_score', 'z_dominance')


def _compute_championship_pillar(profiles: dict[str, FighterProfile]) -> None:
    attrs = ['title_wins', 'title_defenses', 'title_fights']
    weights = [0.45, 0.35, 0.20]
    _compute_weighted_pillar(profiles, attrs, weights, 'championship_score', 'z_championship')


def _compute_consistency_pillar(profiles: dict[str, FighterProfile]) -> None:
    fighter_ids = list(profiles)
    variance_values = [profiles[fighter_id].rating_variance for fighter_id in fighter_ids]
    streak_values = [float(profiles[fighter_id].max_win_streak) for fighter_id in fighter_ids]

    z_var_inv = [-value for value in _robust_z(variance_values)]
    z_streak = _robust_z(streak_values)

    for fighter_id, inv_var, streak in zip(fighter_ids, z_var_inv, z_streak, strict=True):
        profiles[fighter_id].consistency_score = 0.50 * inv_var + 0.50 * streak

    z_consistency = _robust_z([profiles[fighter_id].consistency_score for fighter_id in fighter_ids])
    for fighter_id, z_value in zip(fighter_ids, z_consistency, strict=True):
        profiles[fighter_id].z_consistency = z_value


def _compute_weighted_pillar(
    profiles: dict[str, FighterProfile], attrs: list[str], weights: list[float], raw_attr: str, z_attr: str
) -> None:
    fighter_ids = list(profiles)
    z_maps = [_z_scores_for_attr(profiles, attr) for attr in attrs]

    for fighter_id in fighter_ids:
        weighted_score = sum(weight * z_map.get(fighter_id, 0.0) for weight, z_map in zip(weights, z_maps, strict=True))
        setattr(profiles[fighter_id], raw_attr, weighted_score)

    z_values = _robust_z([getattr(profiles[fighter_id], raw_attr) for fighter_id in fighter_ids])
    for fighter_id, z_value in zip(fighter_ids, z_values, strict=True):
        setattr(profiles[fighter_id], z_attr, z_value)


def _compute_composite(profiles: dict[str, FighterProfile]) -> None:
    for profile in profiles.values():
        profile.goat_score = (
            W_PEAK * profile.z_peak
            + W_LONGEVITY * profile.z_longevity
            + W_SOS * profile.z_sos
            + W_DOMINANCE * profile.z_dominance
            + W_CHAMPIONSHIP * profile.z_championship
            + W_CONSISTENCY * profile.z_consistency
        )


def _z_scores_for_attr(profiles: dict[str, FighterProfile], attr: str) -> dict[str, float]:
    fighter_ids = list(profiles)
    values = [getattr(profiles[fighter_id], attr) for fighter_id in fighter_ids]
    return dict(zip(fighter_ids, _robust_z(values), strict=True))


def _robust_z(values: list[float]) -> list[float]:
    if not values:
        return []

    med = median(values)
    deviations = [abs(value - med) for value in values]
    mad = median(deviations) if deviations else 1.0
    scaled = mad * MAD_TO_STD

    if scaled < EPSILON:
        scaled = stdev(values) if len(values) >= MIN_STDEV_VALUES else 1.0
    if scaled < EPSILON:
        scaled = 1.0

    return [max(-Z_CAP, min(Z_CAP, (value - med) / scaled)) for value in values]


def _rolling_peak(values: list[float], window: int) -> float:
    if not values:
        return 0.0
    if len(values) < window:
        return _safe_mean(values)
    rolling = [sum(values[index : index + window]) / window for index in range(len(values) - window + 1)]
    return max(rolling)


def _window_peak_average(timeline: list[tuple[date, float]], window_days: int) -> float:
    best = 0.0
    for start_index, (window_start, _) in enumerate(timeline):
        values: list[float] = []
        prev_date: date | None = None
        for end_date, rating in timeline[start_index:]:
            if (end_date - window_start).days > window_days:
                break
            if prev_date is not None and (end_date - prev_date).days > MAX_GAP_DAYS:
                prev_date = end_date
                continue
            values.append(rating)
            prev_date = end_date
        if values:
            best = max(best, _safe_mean(values))
    return best


def _percentile(values: list[float], percentile: float, *, fallback: float) -> float:
    if not values:
        return fallback
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * percentile))
    return ordered[index]


def _safe_mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0
