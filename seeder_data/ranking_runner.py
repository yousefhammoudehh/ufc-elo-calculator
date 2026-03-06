"""Standalone ranking runner.

Reads fight data from the database, computes PS artifacts, runs **all six**
rating systems (A-F) chronologically, and writes final snapshots for *every*
fighter at the end date so that a simple ``WHERE as_of_date = <latest>`` query
always returns the complete leaderboard.

Usage::

    python -m seeder_data.ranking_runner          # default DB from env
    python -m seeder_data.ranking_runner --help   # options
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger
from sqlalchemy import Connection, create_engine, delete, select, text

from elo_calculator.application.ranking import (
    BoutEvidence,
    BoutOutcome,
    DynamicFactorBradleyTerrySystem,
    EloPerformanceRatingSystem,
    EvidenceTier,
    ExpectedWinRatePoolSystem,
    FighterRoundStats,
    FinishMethod,
    Glicko2PerformanceRatingSystem,
    StackedLogitMixtureSystem,
    StackingSample,
    UnifiedCompositeEloSystem,
    new_dynamic_factor_state,
    new_elo_state,
    new_glicko2_state,
    new_unified_state,
)
from elo_calculator.infrastructure.database import schema as db_schema
from elo_calculator.infrastructure.database.engine import metadata

# Re-use the data-loading helpers already written in step_ranking.
from seeder_data.normalized_seed.helpers import get_sync_db_url
from seeder_data.normalized_seed.step_ranking import (
    REFERENCE_POOL_LIMIT,
    STACK_TRAIN_MIN_SAMPLES,
    _BoutContext,
    _BoutPair,
    _compute_ps_artifacts,
    _load_bout_contexts,
    _load_bout_pairs,
    _load_fight_totals,
    _load_round_stats,
    _ordered_contexts,
    _PsArtifacts,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SYSTEM_VERSION = 'v2'  # bump version so old v1 rows are distinct

_SYSTEM_A_KEY = 'elo_ps'
_SYSTEM_B_KEY = 'glicko2_ps'
_SYSTEM_C_KEY = 'dynamic_factor_bt'
_SYSTEM_D_KEY = 'stacked_logit_mixture'
_SYSTEM_E_KEY = 'expected_win_rate_pool'
_SYSTEM_F_KEY = 'unified_composite_elo'


# ---------------------------------------------------------------------------
# Runtime state container
# ---------------------------------------------------------------------------
@dataclass
class _RatingRuntime:
    elo_system: EloPerformanceRatingSystem
    glicko_system: Glicko2PerformanceRatingSystem
    dynamic_system: DynamicFactorBradleyTerrySystem
    stacked_system: StackedLogitMixtureSystem
    ewr_system: ExpectedWinRatePoolSystem
    unified_system: UnifiedCompositeEloSystem

    elo_states: dict[Any, Any]
    glicko_states: dict[Any, Any]
    dynamic_states: dict[Any, Any]
    unified_states: dict[Any, Any]

    mma_fight_counts: dict[Any, int]
    mma_last_date: dict[Any, date]

    delta_rows: list[dict]
    snapshot_map: dict[tuple[Any, Any, date], dict]
    stacked_samples: list[StackingSample]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = _parse_args()
    engine = create_engine(get_sync_db_url(), future=True)

    with engine.begin() as conn:
        metadata.create_all(conn)
        run_rankings(conn, truncate=not args.no_truncate)

    logger.info('Ranking runner complete.')


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run all rating systems and write snapshots to DB.')
    parser.add_argument(
        '--no-truncate', action='store_true', help='Do not delete existing rating artifacts before running.'
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------
def run_rankings(conn: Connection, *, truncate: bool = True) -> None:
    """Execute the full rating pipeline: PS → systems A-F → snapshots."""
    if truncate:
        _reset_computed_artifacts(conn)
    _ensure_snapshot_default_partition(conn)

    system_ids = _upsert_rating_systems(conn)

    logger.info('Loading bout contexts …')
    contexts = _load_bout_contexts(conn)
    bout_pairs = _load_bout_pairs(conn)
    round_stats = _load_round_stats(conn)
    fight_totals = _load_fight_totals(conn)
    logger.info('Loaded {} bouts, {} pairs.', len(contexts), len(bout_pairs))

    logger.info('Computing PS artifacts …')
    ps_artifacts = _compute_ps_for_runner(conn, contexts, bout_pairs, round_stats, fight_totals)

    final_date = max((c.checkpoint_date for c in contexts), default=date(1900, 1, 1))
    logger.info('Running rating systems (final date = {}) …', final_date)
    _run_all_systems(conn, contexts, bout_pairs, ps_artifacts, system_ids, final_date)

    logger.info('Done.')


# ---------------------------------------------------------------------------
# DB housekeeping
# ---------------------------------------------------------------------------
def _reset_computed_artifacts(conn: Connection) -> None:
    logger.info('Truncating computed rating artifacts …')
    for table in (
        db_schema.serving_fighter_timeseries_table,
        db_schema.serving_fighter_profile_table,
        db_schema.serving_current_rankings_table,
        db_schema.fact_fight_ps_table,
        db_schema.fact_round_ps_table,
        db_schema.fact_rating_delta_table,
        db_schema.fact_rating_snapshot_table,
        db_schema.dim_rating_system_table,
    ):
        conn.execute(delete(table))


def _ensure_snapshot_default_partition(conn: Connection) -> None:
    conn.execute(
        text('CREATE TABLE IF NOT EXISTS fact_rating_snapshot_default PARTITION OF fact_rating_snapshot DEFAULT')
    )


# ---------------------------------------------------------------------------
# Rating system dimension rows
# ---------------------------------------------------------------------------
def _upsert_rating_systems(conn: Connection) -> dict[str, Any]:
    rows = [
        {
            'system_key': _SYSTEM_A_KEY,
            'description': 'System A: Elo-PS with tiered evidence.',
            'param_json': {},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_B_KEY,
            'description': 'System B: Glicko-2-PS uncertainty-aware.',
            'param_json': {},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_C_KEY,
            'description': 'System C: Dynamic factor Bradley-Terry with cross-sport.',
            'param_json': {},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_D_KEY,
            'description': 'System D: Stacked logistic mixture of A/B/C.',
            'param_json': {},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_E_KEY,
            'description': 'System E: Expected-win-rate pool (uncertainty adjusted).',
            'param_json': {},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_F_KEY,
            'description': 'System F: Unified Composite Elo combining all signals.',
            'param_json': {},
            'code_version': SYSTEM_VERSION,
        },
    ]

    # Get MMA sport_id for the system rows
    mma_row = conn.execute(
        select(db_schema.dim_sport_table.c.sport_id).where(db_schema.dim_sport_table.c.sport_key == 'mma')
    ).first()
    sport_id = mma_row.sport_id if mma_row else None

    for row in rows:
        row['sport_id'] = sport_id

    conn.execute(db_schema.dim_rating_system_table.insert(), rows)

    result = conn.execute(
        select(db_schema.dim_rating_system_table.c.system_id, db_schema.dim_rating_system_table.c.system_key).where(
            db_schema.dim_rating_system_table.c.code_version == SYSTEM_VERSION
        )
    ).all()

    return {str(r.system_key): r.system_id for r in result}


# ---------------------------------------------------------------------------
# PS computation (wraps existing logic, writes to DB)
# ---------------------------------------------------------------------------
def _compute_ps_for_runner(
    conn: Connection,
    contexts: list[_BoutContext],
    bout_pairs: dict[Any, _BoutPair],
    round_stats: dict[Any, dict[int, dict[Any, FighterRoundStats]]],
    fight_totals: dict[Any, dict[Any, FighterRoundStats]],
) -> _PsArtifacts:
    """Compute PS artifacts and insert round/fight PS rows into the DB."""

    # We build a lightweight shim that provides bulk_insert to reuse the
    # existing _compute_ps_artifacts helper, which expects a seeder object.
    class _BulkInserter:
        @staticmethod
        def bulk_insert(_conn: Connection, table: Any, rows: list[dict], chunk_size: int = 5000) -> None:
            if not rows:
                return
            for idx in range(0, len(rows), chunk_size):
                batch = rows[idx : idx + chunk_size]
                keyset = {k for r in batch for k in r}
                normalised = [{k: r.get(k) for k in keyset} for r in batch]
                _conn.execute(table.insert(), normalised)

    inserter = _BulkInserter()
    # The existing helper expects (seeder, conn, contexts, pairs, round_stats, fight_totals)
    # but only uses seeder.bulk_insert — so our shim works.
    return _compute_ps_artifacts(inserter, conn, contexts, bout_pairs, round_stats, fight_totals)


# ---------------------------------------------------------------------------
# Main rating loop
# ---------------------------------------------------------------------------
def _run_all_systems(
    conn: Connection,
    contexts: list[_BoutContext],
    bout_pairs: dict[Any, _BoutPair],
    ps_artifacts: _PsArtifacts,
    system_ids: dict[str, Any],
    final_date: date,
) -> None:
    runtime = _RatingRuntime(
        elo_system=EloPerformanceRatingSystem(),
        glicko_system=Glicko2PerformanceRatingSystem(),
        dynamic_system=DynamicFactorBradleyTerrySystem(),
        stacked_system=StackedLogitMixtureSystem(),
        ewr_system=ExpectedWinRatePoolSystem(),
        unified_system=UnifiedCompositeEloSystem(),
        elo_states={},
        glicko_states={},
        dynamic_states={},
        unified_states={},
        mma_fight_counts=defaultdict(int),
        mma_last_date={},
        delta_rows=[],
        snapshot_map={},
        stacked_samples=[],
    )

    ordered = _ordered_contexts(contexts, bout_pairs)
    total = len(ordered)
    log_interval = max(1, total // 20)

    for idx, context in enumerate(ordered):
        pair = bout_pairs.get(context.bout_id)
        if pair is None:
            continue

        tier = ps_artifacts.tier_by_bout.get(context.bout_id, EvidenceTier.C)
        evidence_a = BoutEvidence(
            bout_id=str(context.bout_id),
            event_date=context.processing_date,
            sport_key=context.sport_key,
            outcome_for_a=pair.outcome_for_a,
            tier=tier,
            method=context.method,
            finish_round=context.finish_round,
            finish_time_seconds=context.finish_time_seconds,
            scheduled_rounds=context.scheduled_rounds,
            is_title_fight=context.is_title_fight,
            promotion_strength=context.promotion_strength,
            ps_margin_a=ps_artifacts.ps_margin_by_key.get((context.bout_id, pair.fighter_a_id)),
            ps_fight_a=ps_artifacts.ps_fight_by_key.get((context.bout_id, pair.fighter_a_id)),
        )

        # --- Systems A & B (MMA only) ---
        if context.sport_key == 'mma':
            _update_system_a(runtime, context, pair, evidence_a, system_ids)
            _update_system_b(runtime, context, pair, evidence_a, system_ids)
            _append_stacking_sample(runtime, context, pair, evidence_a)

        # --- System C (all sports) ---
        _update_system_c(runtime, context, pair, evidence_a, system_ids)

        # --- System F (all sports) ---
        _update_system_f(runtime, context, pair, evidence_a, system_ids)

        # --- MMA fight tracking ---
        if context.sport_key == 'mma':
            runtime.mma_fight_counts[pair.fighter_a_id] += 1
            runtime.mma_fight_counts[pair.fighter_b_id] += 1
            runtime.mma_last_date[pair.fighter_a_id] = context.processing_date
            runtime.mma_last_date[pair.fighter_b_id] = context.processing_date

        if (idx + 1) % log_interval == 0:
            logger.info('  processed {}/{} bouts …', idx + 1, total)

    # --- Flush per-bout deltas ---
    if runtime.delta_rows:
        _bulk_insert(conn, db_schema.fact_rating_delta_table, runtime.delta_rows)
        logger.info('Wrote {} rating delta rows.', len(runtime.delta_rows))

    # --- Per-bout snapshots (for history/timeseries) ---
    if runtime.snapshot_map:
        _bulk_insert(conn, db_schema.fact_rating_snapshot_table, list(runtime.snapshot_map.values()))
        logger.info('Wrote {} per-bout snapshot rows.', len(runtime.snapshot_map))

    # --- Systems D & E (computed after all bouts) ---
    _compute_stacked_and_ewr(conn, runtime, system_ids, final_date)

    # --- Final "as-of" snapshots for A/B/C/F (every fighter at final_date) ---
    _write_final_snapshots(conn, runtime, system_ids, final_date)


# ---------------------------------------------------------------------------
# System update helpers
# ---------------------------------------------------------------------------
def _update_system_a(
    runtime: _RatingRuntime, context: _BoutContext, pair: _BoutPair, evidence: BoutEvidence, system_ids: dict[str, Any]
) -> None:
    elo_a = runtime.elo_states.get(pair.fighter_a_id) or new_elo_state(str(pair.fighter_a_id))
    elo_b = runtime.elo_states.get(pair.fighter_b_id) or new_elo_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = runtime.elo_system.update_bout(elo_a, elo_b, evidence)
    runtime.elo_states[pair.fighter_a_id] = post_a
    runtime.elo_states[pair.fighter_b_id] = post_b

    sid = system_ids[_SYSTEM_A_KEY]
    runtime.delta_rows.extend(
        [
            _delta_row(sid, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(sid, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    _upsert_snapshot(
        runtime.snapshot_map, sid, pair.fighter_a_id, context.checkpoint_date, post_a.rating, None, None, {}
    )
    _upsert_snapshot(
        runtime.snapshot_map, sid, pair.fighter_b_id, context.checkpoint_date, post_b.rating, None, None, {}
    )


def _update_system_b(
    runtime: _RatingRuntime, context: _BoutContext, pair: _BoutPair, evidence: BoutEvidence, system_ids: dict[str, Any]
) -> None:
    g_a = runtime.glicko_states.get(pair.fighter_a_id) or new_glicko2_state(str(pair.fighter_a_id))
    g_b = runtime.glicko_states.get(pair.fighter_b_id) or new_glicko2_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = runtime.glicko_system.update_bout(g_a, g_b, evidence)
    runtime.glicko_states[pair.fighter_a_id] = post_a
    runtime.glicko_states[pair.fighter_b_id] = post_b

    sid = system_ids[_SYSTEM_B_KEY]
    runtime.delta_rows.extend(
        [
            _delta_row(sid, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(sid, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    _upsert_snapshot(
        runtime.snapshot_map,
        sid,
        pair.fighter_a_id,
        context.checkpoint_date,
        post_a.rating,
        post_a.rd,
        post_a.volatility,
        {},
    )
    _upsert_snapshot(
        runtime.snapshot_map,
        sid,
        pair.fighter_b_id,
        context.checkpoint_date,
        post_b.rating,
        post_b.rd,
        post_b.volatility,
        {},
    )


def _update_system_c(
    runtime: _RatingRuntime, context: _BoutContext, pair: _BoutPair, evidence: BoutEvidence, system_ids: dict[str, Any]
) -> None:
    d_a = runtime.dynamic_states.get(pair.fighter_a_id) or new_dynamic_factor_state(str(pair.fighter_a_id))
    d_b = runtime.dynamic_states.get(pair.fighter_b_id) or new_dynamic_factor_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = runtime.dynamic_system.update_bout(d_a, d_b, evidence)
    runtime.dynamic_states[pair.fighter_a_id] = post_a
    runtime.dynamic_states[pair.fighter_b_id] = post_b

    sid = system_ids[_SYSTEM_C_KEY]
    runtime.delta_rows.extend(
        [
            _delta_row(sid, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(sid, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    _upsert_snapshot(
        runtime.snapshot_map,
        sid,
        pair.fighter_a_id,
        context.checkpoint_date,
        runtime.dynamic_system.implied_mma_rating(post_a),
        runtime.dynamic_system.mma_uncertainty(post_a),
        None,
        {'striking': float(post_a.mean[1]), 'grappling': float(post_a.mean[2]), 'durability': float(post_a.mean[3])},
    )
    _upsert_snapshot(
        runtime.snapshot_map,
        sid,
        pair.fighter_b_id,
        context.checkpoint_date,
        runtime.dynamic_system.implied_mma_rating(post_b),
        runtime.dynamic_system.mma_uncertainty(post_b),
        None,
        {'striking': float(post_b.mean[1]), 'grappling': float(post_b.mean[2]), 'durability': float(post_b.mean[3])},
    )


def _update_system_f(
    runtime: _RatingRuntime, context: _BoutContext, pair: _BoutPair, evidence: BoutEvidence, system_ids: dict[str, Any]
) -> None:
    u_a = runtime.unified_states.get(pair.fighter_a_id) or new_unified_state(str(pair.fighter_a_id))
    u_b = runtime.unified_states.get(pair.fighter_b_id) or new_unified_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = runtime.unified_system.update_bout(u_a, u_b, evidence)
    runtime.unified_states[pair.fighter_a_id] = post_a
    runtime.unified_states[pair.fighter_b_id] = post_b

    sid = system_ids[_SYSTEM_F_KEY]
    runtime.delta_rows.extend(
        [
            _delta_row(sid, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(sid, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    composite_a = runtime.unified_system.composite_rating(post_a)
    composite_b = runtime.unified_system.composite_rating(post_b)
    _upsert_snapshot(
        runtime.snapshot_map,
        sid,
        pair.fighter_a_id,
        context.checkpoint_date,
        composite_a,
        post_a.rd,
        post_a.volatility,
        {
            'elo_backbone': post_a.rating,
            'mma_skill': float(post_a.skill_mean[0]),
            'striking': float(post_a.skill_mean[1]),
            'grappling': float(post_a.skill_mean[2]),
            'durability': float(post_a.skill_mean[3]),
            'cardio': float(post_a.skill_mean[4]),
            'streak': post_a.win_streak,
            'peak': post_a.peak_rating,
            'consistency': runtime.unified_system.finish_rate(post_a),
        },
    )
    _upsert_snapshot(
        runtime.snapshot_map,
        sid,
        pair.fighter_b_id,
        context.checkpoint_date,
        composite_b,
        post_b.rd,
        post_b.volatility,
        {
            'elo_backbone': post_b.rating,
            'mma_skill': float(post_b.skill_mean[0]),
            'striking': float(post_b.skill_mean[1]),
            'grappling': float(post_b.skill_mean[2]),
            'durability': float(post_b.skill_mean[3]),
            'cardio': float(post_b.skill_mean[4]),
            'streak': post_b.win_streak,
            'peak': post_b.peak_rating,
            'consistency': runtime.unified_system.finish_rate(post_b),
        },
    )


def _append_stacking_sample(
    runtime: _RatingRuntime, context: _BoutContext, pair: _BoutPair, evidence: BoutEvidence
) -> None:
    if pair.outcome_for_a not in {BoutOutcome.WIN, BoutOutcome.LOSS}:
        return

    elo_a = runtime.elo_states.get(pair.fighter_a_id) or new_elo_state(str(pair.fighter_a_id))
    elo_b = runtime.elo_states.get(pair.fighter_b_id) or new_elo_state(str(pair.fighter_b_id))
    g_a = runtime.glicko_states.get(pair.fighter_a_id) or new_glicko2_state(str(pair.fighter_a_id))
    g_b = runtime.glicko_states.get(pair.fighter_b_id) or new_glicko2_state(str(pair.fighter_b_id))
    d_a = runtime.dynamic_states.get(pair.fighter_a_id) or new_dynamic_factor_state(str(pair.fighter_a_id))
    d_b = runtime.dynamic_states.get(pair.fighter_b_id) or new_dynamic_factor_state(str(pair.fighter_b_id))

    last_date = runtime.mma_last_date.get(pair.fighter_a_id)
    days_since = float((context.processing_date - last_date).days) if last_date else 999.0

    runtime.stacked_samples.append(
        StackingSample(
            p_a=runtime.elo_system.expected_win_probability(elo_a.rating, elo_b.rating),
            p_b=runtime.glicko_system.expected_win_probability(g_a, g_b),
            p_c=runtime.dynamic_system.expected_win_probability(d_a, d_b, evidence),
            p_n=None,
            outcome_a_win=1.0 if pair.outcome_for_a == BoutOutcome.WIN else 0.0,
            tier=evidence.tier,
            scheduled_rounds=context.scheduled_rounds,
            is_title_fight=context.is_title_fight,
            days_since_last_mma=days_since,
            missing_weight_class=not context.has_division,
            missing_promotion=not context.has_promotion,
            opponent_connectivity=float(runtime.mma_fight_counts.get(pair.fighter_b_id, 0)),
        )
    )


# ---------------------------------------------------------------------------
# Systems D & E (post-loop, uses final states from A/B/C)
# ---------------------------------------------------------------------------
def _compute_stacked_and_ewr(
    conn: Connection, runtime: _RatingRuntime, system_ids: dict[str, Any], final_date: date
) -> None:
    if len(runtime.stacked_samples) >= STACK_TRAIN_MIN_SAMPLES:
        runtime.stacked_system.fit(runtime.stacked_samples)
        logger.info('System D fitted on {} samples.', len(runtime.stacked_samples))

    active = sorted(runtime.mma_fight_counts, key=lambda f: runtime.mma_fight_counts[f], reverse=True)
    if not active:
        return

    ref_pool = active[: min(REFERENCE_POOL_LIMIT, len(active))]
    uncertainty_map = {
        fid: min(0.49, runtime.dynamic_system.mma_uncertainty(st) / 800.0)
        for fid, st in runtime.dynamic_states.items()
        if fid in active
    }
    fid_by_str = {str(fid): fid for fid in active}
    ref_pool_str = [str(f) for f in ref_pool]

    def prob_fn(a_str: str, b_str: str) -> float:
        fa, fb = fid_by_str.get(a_str), fid_by_str.get(b_str)
        if fa is None or fb is None:
            return 0.5

        elo_a = runtime.elo_states.get(fa) or new_elo_state(str(fa))
        elo_b = runtime.elo_states.get(fb) or new_elo_state(str(fb))
        g_a = runtime.glicko_states.get(fa) or new_glicko2_state(str(fa))
        g_b = runtime.glicko_states.get(fb) or new_glicko2_state(str(fb))
        d_a = runtime.dynamic_states.get(fa) or new_dynamic_factor_state(str(fa))
        d_b = runtime.dynamic_states.get(fb) or new_dynamic_factor_state(str(fb))

        p_a = runtime.elo_system.expected_win_probability(elo_a.rating, elo_b.rating)
        p_b = runtime.glicko_system.expected_win_probability(g_a, g_b)
        ev = BoutEvidence(
            bout_id='neutral',
            event_date=final_date,
            sport_key='mma',
            outcome_for_a=BoutOutcome.DRAW,
            tier=EvidenceTier.B,
            method=FinishMethod.DEC,
            scheduled_rounds=3,
            is_title_fight=False,
            promotion_strength=0.5,
            ps_margin_a=0.0,
        )
        p_c = runtime.dynamic_system.expected_win_probability(d_a, d_b, ev)

        if len(runtime.stacked_samples) < STACK_TRAIN_MIN_SAMPLES:
            return (p_a + p_b + p_c) / 3.0

        sample = StackingSample(
            p_a=p_a,
            p_b=p_b,
            p_c=p_c,
            p_n=None,
            outcome_a_win=0.0,
            tier=EvidenceTier.B,
            scheduled_rounds=3,
            is_title_fight=False,
            days_since_last_mma=120.0,
            missing_weight_class=False,
            missing_promotion=False,
            opponent_connectivity=float(runtime.mma_fight_counts.get(fb, 0)),
        )
        return runtime.stacked_system.predict_probability(sample)

    # System D snapshots (all fighters, final date)
    d_rows = []
    for fid in active:
        ewr_val = runtime.ewr_system.compute_ewr(str(fid), ref_pool_str, prob_fn)
        d_rows.append(
            {
                'system_id': system_ids[_SYSTEM_D_KEY],
                'fighter_id': fid,
                'as_of_date': final_date,
                'rating_mean': ewr_val,
                'rating_rd': None,
                'rating_vol': None,
                'extra_json': {'ref_pool_size': len(ref_pool)},
            }
        )

    # System E snapshots (all fighters, final date)
    e_rows = []
    ranked = runtime.ewr_system.rank(
        [str(f) for f in active], ref_pool_str, prob_fn, {str(k): v for k, v in uncertainty_map.items()}
    )
    for row in ranked:
        fid = fid_by_str.get(row.fighter_id)
        if fid is None:
            continue
        e_rows.append(
            {
                'system_id': system_ids[_SYSTEM_E_KEY],
                'fighter_id': fid,
                'as_of_date': final_date,
                'rating_mean': row.adjusted_score,
                'rating_rd': uncertainty_map.get(fid),
                'rating_vol': None,
                'extra_json': {'ewr': row.ewr, 'adjusted_score': row.adjusted_score, 'ref_pool_size': len(ref_pool)},
            }
        )

    _bulk_insert(conn, db_schema.fact_rating_snapshot_table, d_rows)
    _bulk_insert(conn, db_schema.fact_rating_snapshot_table, e_rows)
    logger.info('Wrote {} System D and {} System E snapshots.', len(d_rows), len(e_rows))


# ---------------------------------------------------------------------------
# Final "as-of" snapshots for A/B/C/F (every fighter at final_date)
# ---------------------------------------------------------------------------
def _write_final_snapshots(
    conn: Connection, runtime: _RatingRuntime, system_ids: dict[str, Any], final_date: date
) -> None:
    """Write one snapshot per (system, fighter) at ``final_date``.

    This ensures that a simple ``WHERE as_of_date = <latest>`` query returns
    the complete leaderboard for *every* system, not just fighters who
    happened to fight on that day.
    """
    rows: list[dict] = []

    # System A — Elo-PS
    sid_a = system_ids[_SYSTEM_A_KEY]
    for fid, state in runtime.elo_states.items():
        key = (sid_a, fid, final_date)
        if key in runtime.snapshot_map:
            continue  # already written via per-bout snapshot
        rows.append(
            {
                'system_id': sid_a,
                'fighter_id': fid,
                'as_of_date': final_date,
                'rating_mean': state.rating,
                'rating_rd': None,
                'rating_vol': None,
                'extra_json': {},
            }
        )

    # System B — Glicko-2-PS
    sid_b = system_ids[_SYSTEM_B_KEY]
    for fid, state in runtime.glicko_states.items():
        key = (sid_b, fid, final_date)
        if key in runtime.snapshot_map:
            continue
        rows.append(
            {
                'system_id': sid_b,
                'fighter_id': fid,
                'as_of_date': final_date,
                'rating_mean': state.rating,
                'rating_rd': state.rd,
                'rating_vol': state.volatility,
                'extra_json': {},
            }
        )

    # System C — Dynamic Factor BT
    sid_c = system_ids[_SYSTEM_C_KEY]
    for fid, state in runtime.dynamic_states.items():
        key = (sid_c, fid, final_date)
        if key in runtime.snapshot_map:
            continue
        rows.append(
            {
                'system_id': sid_c,
                'fighter_id': fid,
                'as_of_date': final_date,
                'rating_mean': runtime.dynamic_system.implied_mma_rating(state),
                'rating_rd': runtime.dynamic_system.mma_uncertainty(state),
                'rating_vol': None,
                'extra_json': {
                    'striking': float(state.mean[1]),
                    'grappling': float(state.mean[2]),
                    'durability': float(state.mean[3]),
                },
            }
        )

    # System F — Unified Composite Elo
    sid_f = system_ids[_SYSTEM_F_KEY]
    for fid, state in runtime.unified_states.items():
        key = (sid_f, fid, final_date)
        if key in runtime.snapshot_map:
            continue
        composite = runtime.unified_system.composite_rating(state)
        rows.append(
            {
                'system_id': sid_f,
                'fighter_id': fid,
                'as_of_date': final_date,
                'rating_mean': composite,
                'rating_rd': state.rd,
                'rating_vol': state.volatility,
                'extra_json': {
                    'elo_backbone': state.rating,
                    'conservative': runtime.unified_system.conservative_rating(state),
                    'mma_skill': float(state.skill_mean[0]),
                    'striking': float(state.skill_mean[1]),
                    'grappling': float(state.skill_mean[2]),
                    'durability': float(state.skill_mean[3]),
                    'cardio': float(state.skill_mean[4]),
                    'streak': state.win_streak,
                    'peak': state.peak_rating,
                    'finish_rate': runtime.unified_system.finish_rate(state),
                    'mma_fights': state.mma_fights_count,
                },
            }
        )

    _bulk_insert(conn, db_schema.fact_rating_snapshot_table, rows)
    logger.info('Wrote {} final "as-of" snapshots for A/B/C/F at {}.', len(rows), final_date)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _delta_row(system_id: Any, bout_id: Any, fighter_id: Any, delta: Any) -> dict:
    return {
        'system_id': system_id,
        'bout_id': bout_id,
        'fighter_id': fighter_id,
        'pre_rating': delta.pre_rating,
        'post_rating': delta.post_rating,
        'delta_rating': delta.delta_rating,
        'expected_win_prob': delta.expected_win_prob,
        'target_score': delta.target_score,
        'k_effective': delta.k_effective,
        'tier_used': delta.tier_used.value,
        'debug_json': {},
    }


def _upsert_snapshot(
    snapshot_map: dict[tuple[Any, Any, date], dict],
    system_id: Any,
    fighter_id: Any,
    as_of_date: date,
    rating_mean: float,
    rating_rd: float | None,
    rating_vol: float | None,
    extra_json: dict[str, Any],
) -> None:
    key = (system_id, fighter_id, as_of_date)
    snapshot_map[key] = {
        'system_id': system_id,
        'fighter_id': fighter_id,
        'as_of_date': as_of_date,
        'rating_mean': rating_mean,
        'rating_rd': rating_rd,
        'rating_vol': rating_vol,
        'extra_json': extra_json,
    }


def _bulk_insert(conn: Connection, table: Any, rows: list[dict], chunk_size: int = 5000) -> None:
    if not rows:
        return
    for idx in range(0, len(rows), chunk_size):
        batch = rows[idx : idx + chunk_size]
        keyset = {k for r in batch for k in r}
        normalised = [{k: r.get(k) for k in keyset} for r in batch]
        conn.execute(table.insert(), normalised)


# ---------------------------------------------------------------------------
if __name__ == '__main__':
    main()
