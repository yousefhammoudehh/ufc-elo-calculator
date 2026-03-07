"""System G GOAT pipeline integration.

This module computes GOAT scores and writes them into ``fact_rating_snapshot``
under ``system_key='goat_composite'`` as part of the ranking pipeline.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger
from sqlalchemy import Connection, delete, select, text

from elo_calculator.application.ranking.system_g_goat_analysis import (
    MIN_MMA_BOUTS,
    W_CHAMPIONSHIP,
    W_CONSISTENCY,
    W_DOMINANCE,
    W_LONGEVITY,
    W_PEAK,
    W_SOS,
    FighterProfile,
    compute_goat_profiles,
)
from elo_calculator.infrastructure.database import schema as db

_SYSTEM_G_KEY = 'goat_composite'
_SYSTEM_G_VERSION = 'v2'
_PRIMARY_SYSTEM_KEY = 'unified_composite_elo'
_CHUNK = 5000


def compute_and_ingest_goat(conn: Connection) -> None:
    """Compute GOAT rankings and persist them as rating snapshots."""
    system_id = _upsert_goat_system(conn)
    logger.info('System G ID: {}', system_id)

    primary_system_id = _get_system_id(conn, _PRIMARY_SYSTEM_KEY)
    if primary_system_id is None:
        logger.error('Primary system "{}" not found. Run A-F first.', _PRIMARY_SYSTEM_KEY)
        return

    qualified_profiles = compute_goat_profiles(conn, min_mma_bouts=MIN_MMA_BOUTS)
    eligible_profiles = list(qualified_profiles.values())
    logger.info('Computed GOAT scores for {} eligible fighters.', len(eligible_profiles))

    final_date = _get_final_date(conn)
    logger.info('Final date: {}', final_date)

    _delete_old_goat_snapshots(conn, system_id)
    _write_goat_snapshots(conn, system_id, eligible_profiles, final_date)
    logger.info('Done. Wrote {} GOAT snapshot rows.', len(eligible_profiles))

    _log_top_fighters(eligible_profiles)


def _upsert_goat_system(conn: Connection) -> Any:
    param_json = {
        'w_peak': W_PEAK,
        'w_longevity': W_LONGEVITY,
        'w_sos': W_SOS,
        'w_dominance': W_DOMINANCE,
        'w_championship': W_CHAMPIONSHIP,
        'w_consistency': W_CONSISTENCY,
        'primary_system': _PRIMARY_SYSTEM_KEY,
        'min_mma_bouts': MIN_MMA_BOUTS,
        'algorithm': 'system_g_goat_analysis_v2',
    }

    existing = conn.execute(
        select(db.dim_rating_system_table.c.system_id).where(
            db.dim_rating_system_table.c.system_key == _SYSTEM_G_KEY,
            db.dim_rating_system_table.c.code_version == _SYSTEM_G_VERSION,
        )
    ).scalar()
    if existing:
        return existing

    mma_sport_id = _get_mma_sport_id(conn)
    conn.execute(
        db.dim_rating_system_table.insert().values(
            system_key=_SYSTEM_G_KEY,
            sport_id=mma_sport_id,
            description='System G: GOAT composite (peak, longevity, SoS, dominance, championship, consistency).',
            param_json=param_json,
            code_version=_SYSTEM_G_VERSION,
        )
    )

    return conn.execute(
        select(db.dim_rating_system_table.c.system_id).where(
            db.dim_rating_system_table.c.system_key == _SYSTEM_G_KEY,
            db.dim_rating_system_table.c.code_version == _SYSTEM_G_VERSION,
        )
    ).scalar()


def _get_system_id(conn: Connection, system_key: str) -> Any | None:
    return conn.execute(
        select(db.dim_rating_system_table.c.system_id).where(db.dim_rating_system_table.c.system_key == system_key)
    ).scalar()


def _get_mma_sport_id(conn: Connection) -> Any:
    return conn.execute(select(db.dim_sport_table.c.sport_id).where(db.dim_sport_table.c.sport_key == 'mma')).scalar()


def _get_final_date(conn: Connection) -> date:
    result = conn.execute(select(text('MAX(as_of_date)')).select_from(db.fact_rating_snapshot_table)).scalar()
    return result or date.today()


def _delete_old_goat_snapshots(conn: Connection, system_id: Any) -> None:
    conn.execute(delete(db.fact_rating_snapshot_table).where(db.fact_rating_snapshot_table.c.system_id == system_id))
    logger.info('Deleted old GOAT snapshots.')


def _profile_to_extra_json(profile: FighterProfile) -> dict[str, Any]:
    return {
        'peak': {
            'raw': round(profile.peak_score, 6),
            'z': round(profile.z_peak, 6),
            'peak_unified': round(profile.peak_unified, 6),
            'peak_dynamic': round(profile.peak_dynamic, 6),
            'peak_elo': round(profile.peak_elo, 6),
            'peak_glicko': round(profile.peak_glicko, 6),
            'peak_3fight_avg': round(profile.peak_3fight_avg, 6),
            'peak_dominance_window': round(profile.peak_dominance_window, 6),
        },
        'longevity': {
            'raw': round(profile.longevity_score, 6),
            'z': round(profile.z_longevity, 6),
            'auc_above_baseline': round(profile.auc_above_baseline, 6),
            'time_in_elite_days': round(profile.time_in_elite_days, 6),
            'career_span_years': round(profile.career_span_years, 6),
            'mma_bouts': profile.mma_bouts,
        },
        'sos': {
            'raw': round(profile.sos_score, 6),
            'z': round(profile.z_sos, 6),
            'avg_opponent_rating': round(profile.avg_opponent_rating, 6),
            'avg_opponent_rating_in_wins': round(profile.avg_opponent_rating_in_wins, 6),
            'top_opp_wins': profile.top_opp_wins,
            'upset_wins': profile.upset_wins,
        },
        'dominance': {
            'raw': round(profile.dominance_score, 6),
            'z': round(profile.z_dominance, 6),
            'win_rate': round(profile.win_rate, 6),
            'finish_rate': round(profile.finish_rate, 6),
            'avg_qow': round(profile.avg_qow, 6),
            'avg_ps_fight': round(profile.avg_ps_fight, 6),
        },
        'championship': {
            'raw': round(profile.championship_score, 6),
            'z': round(profile.z_championship, 6),
            'title_wins': profile.title_wins,
            'title_fights': profile.title_fights,
            'title_defenses': profile.title_defenses,
            'title_win_rate': round(profile.title_win_rate, 6),
        },
        'consistency': {
            'raw': round(profile.consistency_score, 6),
            'z': round(profile.z_consistency, 6),
            'rating_variance': round(profile.rating_variance, 6),
            'max_win_streak': profile.max_win_streak,
        },
        'record': {
            'wins': profile.mma_wins,
            'losses': profile.mma_losses,
            'draws': profile.mma_draws,
            'nc': profile.mma_nc,
            'finish_wins': profile.mma_finish_wins,
            'decision_wins': profile.mma_decision_wins,
        },
        'composite_score': round(profile.goat_score, 6),
    }


def _write_goat_snapshots(conn: Connection, system_id: Any, profiles: list[FighterProfile], final_date: date) -> None:
    rows: list[dict[str, Any]] = []
    for profile in profiles:
        rows.append(
            {
                'system_id': system_id,
                'fighter_id': profile.fighter_id,
                'as_of_date': final_date,
                'rating_mean': round(profile.goat_score, 4),
                'rating_rd': None,
                'rating_vol': None,
                'extra_json': _profile_to_extra_json(profile),
            }
        )

    _bulk_insert(conn, db.fact_rating_snapshot_table, rows)


def _bulk_insert(conn: Connection, table: Any, rows: list[dict[str, Any]], chunk_size: int = _CHUNK) -> None:
    if not rows:
        return
    for idx in range(0, len(rows), chunk_size):
        batch = rows[idx : idx + chunk_size]
        keyset = {k for row in batch for k in row}
        normalized = [{k: row.get(k) for k in keyset} for row in batch]
        conn.execute(table.insert(), normalized)


def _log_top_fighters(profiles: list[FighterProfile]) -> None:
    ranked = sorted(profiles, key=lambda profile: profile.goat_score, reverse=True)

    logger.info('\n=== TOP 25 GOAT COMPOSITE ===')
    logger.info(
        '{:>4s}  {:<30s} {:>7s}  {:>6s} {:>6s} {:>6s} {:>6s} {:>6s} {:>6s}',
        'Rank',
        'Fighter',
        'GOAT',
        'Peak',
        'Long',
        'SoS',
        'Dom',
        'Champ',
        'Cons',
    )
    for rank, profile in enumerate(ranked[:25], 1):
        logger.info(
            '{:4d}  {:<30s} {:7.3f}  {:6.2f} {:6.2f} {:6.2f} {:6.2f} {:6.2f} {:6.2f}',
            rank,
            profile.display_name[:30],
            profile.goat_score,
            profile.z_peak,
            profile.z_longevity,
            profile.z_sos,
            profile.z_dominance,
            profile.z_championship,
            profile.z_consistency,
        )
