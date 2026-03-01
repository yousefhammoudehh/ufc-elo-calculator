from __future__ import annotations

import importlib
import re
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

from elo_calculator.application.ranking.ps import aggregate_fight_ps, compute_ps_round
from elo_calculator.application.ranking.system_a_elo_ps import EloPerformanceRatingSystem, new_elo_state
from elo_calculator.application.ranking.system_b_glicko2_ps import Glicko2PerformanceRatingSystem, new_glicko2_state
from elo_calculator.application.ranking.system_c_dynamic_factor_bt import (
    DynamicFactorBradleyTerrySystem,
    new_dynamic_factor_state,
)
from elo_calculator.application.ranking.system_d_stacked_logit import StackedLogitMixtureSystem
from elo_calculator.application.ranking.system_e_expected_win_rate import ExpectedWinRatePoolSystem
from elo_calculator.application.ranking.types import (
    BoutEvidence,
    BoutOutcome,
    EvidenceTier,
    FighterRoundStats,
    FinishMethod,
    RoundMeta,
    StackingSample,
)
from elo_calculator.domain.shared.enumerations import BoutResultEnum, inverse_bout_result, normalize_bout_result
from seeder_data.normalized_seed.helpers import (
    clean_text,
    parse_date_value,
    parse_int,
    parse_mmss_to_seconds,
    parse_prefight_record_total,
)

pd: Any = importlib.import_module('pandas')

DEFAULT_UFC1_EVENT_ID = '6420efac0578988b'
# UFC 1 has too few samples for stable stacking fit; keep a conservative floor.
MIN_STACKING_SAMPLES = 30
ProbabilityFn = Callable[[str, str], float]


@dataclass(frozen=True)
class UFC1WalkthroughContext:
    data_dir: Path
    event_id: str
    event_name: str
    event_date: date
    ordered_ufc1: list[dict[str, Any]]
    pre_bouts: list[dict[str, Any]]
    ufc1_pool_ids: list[str]
    ufc1_pool_names: dict[str, str]
    all_record_strings: list[str]


@dataclass(frozen=True)
class _RoundStatsSources:
    fight_rounds: pd.DataFrame
    sig_target: pd.DataFrame
    sig_position: pd.DataFrame


@dataclass(frozen=True)
class _WalkthroughTables:
    events: pd.DataFrame
    fights: pd.DataFrame
    fighters: pd.DataFrame
    tapology_bouts: pd.DataFrame
    tapology_bout_fighters: pd.DataFrame
    ufc_name_by_id: dict[str, str]
    tap_name_by_id: dict[str, str]
    ufc_to_tap: dict[str, str]
    fight_to_tap_bout: dict[str, str]
    round_sources: _RoundStatsSources


@dataclass(frozen=True)
class _EventBoutBuildContext:
    event_date: date
    round_sources: _RoundStatsSources
    fight_to_tap_bout: dict[str, str]
    event_tbf_lookup: dict[tuple[str, str], dict[str, str]]
    ufc_to_tap: dict[str, str]
    ufc_name_by_id: dict[str, str]
    tap_name_by_id: dict[str, str]


def build_ufc1_context(data_dir: Path | None = None, event_id: str = DEFAULT_UFC1_EVENT_ID) -> UFC1WalkthroughContext:
    resolved_data_dir = data_dir or Path('seeder_data/data')
    tables = _read_walkthrough_tables(resolved_data_dir)

    event_name, event_date, event_fights = _resolve_ufc_event(tables.events, tables.fights, event_id)
    ordered_event_bouts = _build_ordered_event_bouts(
        event_fights=event_fights,
        event_date=event_date,
        round_sources=tables.round_sources,
        fight_to_tap_bout=tables.fight_to_tap_bout,
        ufc_to_tap=tables.ufc_to_tap,
        ufc_name_by_id=tables.ufc_name_by_id,
        tap_name_by_id=tables.tap_name_by_id,
        tapology_bout_fighters=tables.tapology_bout_fighters,
    )

    pool_ids, pool_names = _pool_metadata(ordered_event_bouts)
    pre_bouts = _build_pre_bouts(
        ordered_event_bouts=ordered_event_bouts,
        tapology_bout_fighters=tables.tapology_bout_fighters,
        tapology_bouts=tables.tapology_bouts,
        tap_name_by_id=tables.tap_name_by_id,
        event_date=event_date,
    )
    all_record_strings = _collect_record_strings(tables.tapology_bout_fighters, tables.fighters)

    return UFC1WalkthroughContext(
        data_dir=resolved_data_dir,
        event_id=event_id,
        event_name=event_name,
        event_date=event_date,
        ordered_ufc1=ordered_event_bouts,
        pre_bouts=pre_bouts,
        ufc1_pool_ids=pool_ids,
        ufc1_pool_names=pool_names,
        all_record_strings=all_record_strings,
    )


def _read_walkthrough_tables(data_dir: Path) -> _WalkthroughTables:
    events = pd.read_csv(data_dir / 'ufcstats_events_2025.csv', dtype=str).fillna('')
    fights = pd.read_csv(data_dir / 'ufcstats_fights.csv', dtype=str).fillna('')
    fighters = pd.read_csv(data_dir / 'ufcstats_fighters.csv', dtype=str).fillna('')
    fight_rounds = pd.read_csv(data_dir / 'ufcstats_fight_rounds.csv', dtype=str).fillna('')
    sig_target = pd.read_csv(data_dir / 'ufcstats_sig_strikes_by_target.csv', dtype=str).fillna('')
    sig_position = pd.read_csv(data_dir / 'ufcstats_sig_strikes_by_position.csv', dtype=str).fillna('')

    bout_map = pd.read_csv(data_dir / 'bout_id_map.csv', dtype=str).fillna('')
    fighter_map = pd.read_csv(data_dir / 'fighter_id_map.csv', dtype=str).fillna('')
    tapology_bouts = pd.read_csv(data_dir / 'tapology_bouts.csv', dtype=str).fillna('')
    tapology_bout_fighters = pd.read_csv(data_dir / 'tapology_bout_fighters.csv', dtype=str).fillna('')
    tapology_fighters = pd.read_csv(data_dir / 'tapology_fighters.csv', dtype=str).fillna('')

    ufc_name_by_id = dict(zip(fighters['ufcstats_fighter_id'], fighters['name'], strict=False))
    tap_name_by_id = dict(zip(tapology_fighters['tapology_fighter_id'], tapology_fighters['name'], strict=False))

    ufc_to_tap = {
        str(row.ufcstats_fighter_id): str(row.tapology_fighter_id)
        for row in fighter_map.itertuples(index=False)
        if str(row.ufcstats_fighter_id).strip() and str(row.tapology_fighter_id).strip()
    }
    fight_to_tap_bout = {
        str(row.ufcstats_fight_id): str(row.tapology_bout_id)
        for row in bout_map.itertuples(index=False)
        if str(row.ufcstats_fight_id).strip() and str(row.tapology_bout_id).strip()
    }

    return _WalkthroughTables(
        events=events,
        fights=fights,
        fighters=fighters,
        tapology_bouts=tapology_bouts,
        tapology_bout_fighters=tapology_bout_fighters,
        ufc_name_by_id=ufc_name_by_id,
        tap_name_by_id=tap_name_by_id,
        ufc_to_tap=ufc_to_tap,
        fight_to_tap_bout=fight_to_tap_bout,
        round_sources=_RoundStatsSources(fight_rounds=fight_rounds, sig_target=sig_target, sig_position=sig_position),
    )


def _resolve_ufc_event(events: pd.DataFrame, fights: pd.DataFrame, event_id: str) -> tuple[str, date, pd.DataFrame]:
    ufc_event = events.loc[events['event_id'] == event_id].iloc[0]
    event_date = date.fromisoformat(str(ufc_event['date']))
    event_name = str(ufc_event['name'])
    event_fights = fights[fights['ufcstats_event_id'] == event_id].copy().reset_index(drop=True)
    return event_name, event_date, event_fights


def _build_ordered_event_bouts(
    event_fights: pd.DataFrame,
    event_date: date,
    round_sources: _RoundStatsSources,
    fight_to_tap_bout: dict[str, str],
    ufc_to_tap: dict[str, str],
    ufc_name_by_id: dict[str, str],
    tap_name_by_id: dict[str, str],
    tapology_bout_fighters: pd.DataFrame,
) -> list[dict[str, Any]]:
    event_tbf_lookup = _event_tbf_lookup(event_fights, fight_to_tap_bout, tapology_bout_fighters)
    build_ctx = _EventBoutBuildContext(
        event_date=event_date,
        round_sources=round_sources,
        fight_to_tap_bout=fight_to_tap_bout,
        event_tbf_lookup=event_tbf_lookup,
        ufc_to_tap=ufc_to_tap,
        ufc_name_by_id=ufc_name_by_id,
        tap_name_by_id=tap_name_by_id,
    )
    event_bouts = [
        _build_event_bout(fight=fight, build_ctx=build_ctx) for fight in event_fights.itertuples(index=False)
    ]
    return _infer_same_day_order(event_bouts)


def _event_tbf_lookup(
    event_fights: pd.DataFrame, fight_to_tap_bout: dict[str, str], tapology_bout_fighters: pd.DataFrame
) -> dict[tuple[str, str], dict[str, str]]:
    event_tap_bout_ids = {
        fight_to_tap_bout.get(str(fight_id), '') for fight_id in set(event_fights['ufcstats_fight_id'])
    }
    event_tap_bout_ids.discard('')
    event_tbf = tapology_bout_fighters[tapology_bout_fighters['tapology_bout_id'].isin(event_tap_bout_ids)].copy()
    return {
        (str(row.get('tapology_bout_id', '')), str(row.get('tapology_fighter_id', ''))): row
        for row in event_tbf.to_dict('records')
    }


def _build_event_bout(fight: Any, build_ctx: _EventBoutBuildContext) -> dict[str, Any]:
    fight_id = str(fight.ufcstats_fight_id)
    red_ufc = str(fight.red_fighter_id)
    blue_ufc = str(fight.blue_fighter_id)
    winner_ufc = str(fight.winner_fighter_id)

    fighter_a_id, fighter_a_name, fighter_a_tap = _resolve_canonical_from_ufc(
        red_ufc, build_ctx.ufc_to_tap, build_ctx.ufc_name_by_id, build_ctx.tap_name_by_id
    )
    fighter_b_id, fighter_b_name, fighter_b_tap = _resolve_canonical_from_ufc(
        blue_ufc, build_ctx.ufc_to_tap, build_ctx.ufc_name_by_id, build_ctx.tap_name_by_id
    )

    tap_bout_id = build_ctx.fight_to_tap_bout.get(fight_id, '')
    record_a_raw, record_b_raw = _prefight_records_for_bout(
        tap_bout_id, fighter_a_tap, fighter_b_tap, build_ctx.event_tbf_lookup
    )

    scheduled_rounds = max(3, _parse_int_with_default(str(fight.scheduled_rounds), default=3))
    finish_round = _parse_int_with_default(str(fight.round), default=1)
    finish_time_seconds = _parse_seconds_value(str(fight.time))
    ps_margin_a, ps_fight_a = _compute_ps_for_ufc_fight(
        fight_id=fight_id,
        fighter_a_ufc=red_ufc,
        fighter_b_ufc=blue_ufc,
        winner_ufc=winner_ufc,
        finish_round=finish_round,
        finish_time_seconds=finish_time_seconds,
        sources=build_ctx.round_sources,
    )

    return {
        'fight_id': fight_id,
        'bout_id': fight_id,
        'tapology_bout_id': tap_bout_id,
        'event_date': build_ctx.event_date,
        'sport': 'mma',
        'fighter_a_id': fighter_a_id,
        'fighter_a_name': fighter_a_name,
        'fighter_b_id': fighter_b_id,
        'fighter_b_name': fighter_b_name,
        'outcome_for_a': _parse_outcome_from_winner(winner_ufc, red_ufc, blue_ufc),
        'method': _parse_finish_method(str(fight.method)),
        'scheduled_rounds': scheduled_rounds,
        'is_title_fight': str(fight.is_title_bout).lower() == 'true',
        'promotion_strength': 1.0,
        'finish_round': finish_round,
        'finish_time_seconds': finish_time_seconds,
        'ps_margin_a': float(ps_margin_a),
        'ps_fight_a': float(ps_fight_a),
        'prefight_record_a': record_a_raw,
        'prefight_record_b': record_b_raw,
        'prefight_total_a': parse_prefight_record_total(record_a_raw),
        'prefight_total_b': parse_prefight_record_total(record_b_raw),
    }


def _prefight_records_for_bout(
    tap_bout_id: str,
    fighter_a_tap: str | None,
    fighter_b_tap: str | None,
    event_tbf_lookup: dict[tuple[str, str], dict[str, str]],
) -> tuple[str | None, str | None]:
    record_a_raw = None
    record_b_raw = None
    if tap_bout_id and fighter_a_tap:
        row_a = event_tbf_lookup.get((tap_bout_id, fighter_a_tap), {})
        record_a_raw = clean_text(str(row_a.get('fighter_record_before_raw') or ''))
    if tap_bout_id and fighter_b_tap:
        row_b = event_tbf_lookup.get((tap_bout_id, fighter_b_tap), {})
        record_b_raw = clean_text(str(row_b.get('fighter_record_before_raw') or ''))
    return record_a_raw, record_b_raw


def _pool_metadata(ordered_event_bouts: list[dict[str, Any]]) -> tuple[list[str], dict[str, str]]:
    pool_ids = sorted(
        {str(row['fighter_a_id']) for row in ordered_event_bouts}
        | {str(row['fighter_b_id']) for row in ordered_event_bouts}
    )
    pool_names = {str(row['fighter_a_id']): str(row['fighter_a_name']) for row in ordered_event_bouts} | {
        str(row['fighter_b_id']): str(row['fighter_b_name']) for row in ordered_event_bouts
    }
    return pool_ids, pool_names


def _build_pre_bouts(
    ordered_event_bouts: list[dict[str, Any]],
    tapology_bout_fighters: pd.DataFrame,
    tapology_bouts: pd.DataFrame,
    tap_name_by_id: dict[str, str],
    event_date: date,
) -> list[dict[str, Any]]:
    focus_tap_ids = {
        str(row['fighter_a_id']).replace('tap:', '')
        for row in ordered_event_bouts
        if str(row['fighter_a_id']).startswith('tap:')
    } | {
        str(row['fighter_b_id']).replace('tap:', '')
        for row in ordered_event_bouts
        if str(row['fighter_b_id']).startswith('tap:')
    }
    prehistory_rows = tapology_bout_fighters[tapology_bout_fighters['tapology_fighter_id'].isin(focus_tap_ids)].copy()
    prehistory_rows = prehistory_rows.merge(
        tapology_bouts[['tapology_bout_id', 'sport', 'event_date', 'event_name', 'method', 'is_title_fight']],
        on='tapology_bout_id',
        how='left',
    )
    prehistory_rows = prehistory_rows[prehistory_rows['event_date'].astype(str) < str(event_date)].copy()
    prehistory_rows = prehistory_rows.sort_values(['event_date', 'tapology_bout_id', 'tapology_fighter_id'])
    prehistory_rows = prehistory_rows.drop_duplicates(subset=['tapology_bout_id'], keep='first')
    return _normalize_precheckpoint_bouts(prehistory_rows, tap_name_by_id)


def _collect_record_strings(tapology_bout_fighters: pd.DataFrame, fighters: pd.DataFrame) -> list[str]:
    return sorted(
        {
            str(value).strip()
            for value in set(tapology_bout_fighters['fighter_record_before_raw'])
            .union(set(tapology_bout_fighters['opponent_record_before_raw']))
            .union(set(fighters['record']))
            if str(value).strip()
        }
    )


def order_dataframe(context: UFC1WalkthroughContext) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'fight_order': row['fight_order'],
                'fight_id': row['fight_id'],
                'matchup': f'{row["fighter_a_name"]} vs {row["fighter_b_name"]}',
                'tapology_bout_id': row['tapology_bout_id'],
                'prefight_a': row['prefight_record_a'],
                'prefight_b': row['prefight_record_b'],
                'stage_from_record_before': row['same_day_stage'],
            }
            for row in context.ordered_ufc1
        ]
    )


def record_nc_samples(context: UFC1WalkthroughContext, limit: int = 20) -> list[str]:
    return sorted([value for value in context.all_record_strings if 'nc' in value.lower()])[:limit]


def precheckpoint_dataframe(context: UFC1WalkthroughContext) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'event_date': row['event_date'],
                'sport': row['sport'],
                'fighter': row['fighter_a_name'],
                'result': row['outcome_for_a'].value,
                'opponent': row['fighter_b_name'],
                'record_before': row['record_before_a'],
                'event_name': row['event_name'],
            }
            for row in context.pre_bouts
            if str(row['fighter_a_id']) in context.ufc1_pool_ids
        ]
    )


def coverage_dataframe(context: UFC1WalkthroughContext) -> pd.DataFrame:
    rows = []
    for fighter_id in context.ufc1_pool_ids:
        fighter_name = context.ufc1_pool_names.get(fighter_id, fighter_id)
        fighter_bouts = [row for row in context.pre_bouts if str(row['fighter_a_id']) == fighter_id]
        sports = sorted({str(row['sport']) for row in fighter_bouts})
        rows.append(
            {
                'fighter': fighter_name,
                'pre_checkpoint_bouts': len(fighter_bouts),
                'mma_bouts': sum(1 for row in fighter_bouts if str(row['sport']) == 'mma'),
                'non_mma_bouts': sum(1 for row in fighter_bouts if str(row['sport']) != 'mma'),
                'sports_seen': ', '.join(sports),
            }
        )
    return pd.DataFrame(rows).sort_values(['pre_checkpoint_bouts', 'fighter'], ascending=[False, True])


def run_system_a(
    context: UFC1WalkthroughContext,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float], dict[str, Any]]:
    system = EloPerformanceRatingSystem()
    states: dict[str, Any] = {}

    def state(fighter_id: str) -> Any:
        if fighter_id not in states:
            states[fighter_id] = new_elo_state(fighter_id)
        return states[fighter_id]

    prior_rows = []
    for bout in context.pre_bouts:
        if str(bout['sport']) != 'mma':
            continue
        evidence = _evidence_from_bout(bout, EvidenceTier.C)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)
        post_a, post_b, delta_a, _delta_b = system.update_bout(pre_a, pre_b, evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

        if fighter_a_id in context.ufc1_pool_ids:
            prior_rows.append(
                {
                    'event_date': bout['event_date'],
                    'sport': bout['sport'],
                    'fighter': bout['fighter_a_name'],
                    'opponent': bout['fighter_b_name'],
                    'result': bout['outcome_for_a'].value,
                    'pre_rating': round(delta_a.pre_rating, 2),
                    'post_rating': round(delta_a.post_rating, 2),
                    'delta': round(delta_a.delta_rating, 4),
                }
            )

    fight_rows = []
    probs_by_fight: dict[str, float] = {}
    for bout in context.ordered_ufc1:
        evidence = _evidence_from_bout(bout, EvidenceTier.A)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)
        prob_a = system.expected_win_probability(pre_a.rating, pre_b.rating)
        probs_by_fight[str(bout['fight_id'])] = prob_a

        post_a, post_b, delta_a, delta_b = system.update_bout(pre_a, pre_b, evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

        fight_rows.extend(_two_sided_rows_for_standard_system(bout, delta_a, delta_b))

    return pd.DataFrame(prior_rows), pd.DataFrame(fight_rows), probs_by_fight, states


def run_system_b(
    context: UFC1WalkthroughContext,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float], dict[str, Any]]:
    system = Glicko2PerformanceRatingSystem()
    states: dict[str, Any] = {}

    def state(fighter_id: str) -> Any:
        if fighter_id not in states:
            states[fighter_id] = new_glicko2_state(fighter_id)
        return states[fighter_id]

    prior_rows = []
    for bout in context.pre_bouts:
        if str(bout['sport']) != 'mma':
            continue
        evidence = _evidence_from_bout(bout, EvidenceTier.C)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)
        post_a, post_b, delta_a, _delta_b = system.update_bout(pre_a, pre_b, evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

        if fighter_a_id in context.ufc1_pool_ids:
            prior_rows.append(
                {
                    'event_date': bout['event_date'],
                    'sport': bout['sport'],
                    'fighter': bout['fighter_a_name'],
                    'opponent': bout['fighter_b_name'],
                    'result': bout['outcome_for_a'].value,
                    'pre_rating': round(delta_a.pre_rating, 2),
                    'post_rating': round(delta_a.post_rating, 2),
                    'delta': round(delta_a.delta_rating, 4),
                }
            )

    fight_rows = []
    probs_by_fight: dict[str, float] = {}
    for bout in context.ordered_ufc1:
        evidence = _evidence_from_bout(bout, EvidenceTier.A)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)
        prob_a = system.expected_win_probability(pre_a, pre_b)
        probs_by_fight[str(bout['fight_id'])] = prob_a

        pre_a_rd = pre_a.rd
        pre_b_rd = pre_b.rd
        post_a, post_b, delta_a, delta_b = system.update_bout(pre_a, pre_b, evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

        row_a = _one_sided_row(bout, delta_a, for_fighter_a=True)
        row_b = _one_sided_row(bout, delta_b, for_fighter_a=False)
        row_a['pre_rd'] = round(pre_a_rd, 4)
        row_a['post_rd'] = round(post_a.rd, 4)
        row_b['pre_rd'] = round(pre_b_rd, 4)
        row_b['post_rd'] = round(post_b.rd, 4)
        fight_rows.extend([row_a, row_b])

    return pd.DataFrame(prior_rows), pd.DataFrame(fight_rows), probs_by_fight, states


def run_system_c(
    context: UFC1WalkthroughContext,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float], dict[str, Any]]:
    system = DynamicFactorBradleyTerrySystem()
    states: dict[str, Any] = {}

    prior_rows: list[dict[str, Any]] = []
    for bout in context.pre_bouts:
        prior_row = _dynamic_prior_row(system=system, states=states, bout=bout, in_pool=context.ufc1_pool_ids)
        if prior_row is not None:
            prior_rows.append(prior_row)

    fight_rows: list[dict[str, Any]] = []
    probs_by_fight: dict[str, float] = {}
    for bout in context.ordered_ufc1:
        prob_a, rows = _dynamic_event_rows(system=system, states=states, bout=bout)
        probs_by_fight[str(bout['fight_id'])] = prob_a
        fight_rows.extend(rows)

    return pd.DataFrame(prior_rows), pd.DataFrame(fight_rows), probs_by_fight, states


def _get_dynamic_state(states: dict[str, Any], fighter_id: str) -> Any:
    if fighter_id not in states:
        states[fighter_id] = new_dynamic_factor_state(fighter_id)
    return states[fighter_id]


def _dynamic_prior_row(
    system: DynamicFactorBradleyTerrySystem, states: dict[str, Any], bout: dict[str, Any], in_pool: list[str]
) -> dict[str, Any] | None:
    evidence = _evidence_from_bout(bout, EvidenceTier.C)
    fighter_a_id = str(bout['fighter_a_id'])
    fighter_b_id = str(bout['fighter_b_id'])
    pre_a = _get_dynamic_state(states, fighter_a_id)
    pre_b = _get_dynamic_state(states, fighter_b_id)

    pre_mma = system.implied_mma_rating(pre_a)
    pre_striking = float(pre_a.mean[1])
    pre_grappling = float(pre_a.mean[2])
    pre_durability = float(pre_a.mean[3])

    post_a, post_b, delta_a, _delta_b = system.update_bout(pre_a, pre_b, evidence)
    states[fighter_a_id] = post_a
    states[fighter_b_id] = post_b

    if fighter_a_id not in in_pool:
        return None
    return {
        'event_date': bout['event_date'],
        'sport': bout['sport'],
        'fighter': bout['fighter_a_name'],
        'opponent': bout['fighter_b_name'],
        'result': bout['outcome_for_a'].value,
        'pre_mma_rating': round(pre_mma, 2),
        'post_mma_rating': round(system.implied_mma_rating(post_a), 2),
        'delta_mma': round(delta_a.delta_rating, 4),
        'pre_striking': round(pre_striking, 4),
        'post_striking': round(float(post_a.mean[1]), 4),
        'pre_grappling': round(pre_grappling, 4),
        'post_grappling': round(float(post_a.mean[2]), 4),
        'pre_durability': round(pre_durability, 4),
        'post_durability': round(float(post_a.mean[3]), 4),
    }


def _dynamic_event_rows(
    system: DynamicFactorBradleyTerrySystem, states: dict[str, Any], bout: dict[str, Any]
) -> tuple[float, list[dict[str, Any]]]:
    evidence = _evidence_from_bout(bout, EvidenceTier.A)
    fighter_a_id = str(bout['fighter_a_id'])
    fighter_b_id = str(bout['fighter_b_id'])
    pre_a = _get_dynamic_state(states, fighter_a_id)
    pre_b = _get_dynamic_state(states, fighter_b_id)

    prob_a = system.expected_win_probability(pre_a, pre_b, evidence)
    pre_a_mma = system.implied_mma_rating(pre_a)
    pre_b_mma = system.implied_mma_rating(pre_b)

    post_a, post_b, delta_a, delta_b = system.update_bout(pre_a, pre_b, evidence)
    states[fighter_a_id] = post_a
    states[fighter_b_id] = post_b

    row_a = _dynamic_enriched_row(
        bout=bout,
        delta=delta_a,
        pre_state=pre_a,
        post_state=post_a,
        pre_mma=pre_a_mma,
        for_fighter_a=True,
        system=system,
    )
    row_b = _dynamic_enriched_row(
        bout=bout,
        delta=delta_b,
        pre_state=pre_b,
        post_state=post_b,
        pre_mma=pre_b_mma,
        for_fighter_a=False,
        system=system,
    )
    return prob_a, [row_a, row_b]


def _dynamic_enriched_row(
    bout: dict[str, Any],
    delta: Any,
    pre_state: Any,
    post_state: Any,
    pre_mma: float,
    for_fighter_a: bool,
    system: DynamicFactorBradleyTerrySystem,
) -> dict[str, Any]:
    row = _one_sided_row(bout, delta, for_fighter_a=for_fighter_a)
    row['pre_mma_rating'] = round(pre_mma, 2)
    row['post_mma_rating'] = round(system.implied_mma_rating(post_state), 2)
    row['delta_mma'] = row.pop('delta')
    row['pre_striking'] = round(float(pre_state.mean[1]), 4)
    row['post_striking'] = round(float(post_state.mean[1]), 4)
    row['pre_grappling'] = round(float(pre_state.mean[2]), 4)
    row['post_grappling'] = round(float(post_state.mean[2]), 4)
    row.pop('pre_rating', None)
    row.pop('post_rating', None)
    return row


def run_system_d(
    context: UFC1WalkthroughContext, probs_a: dict[str, float], probs_b: dict[str, float], probs_c: dict[str, float]
) -> pd.DataFrame:
    stacked = StackedLogitMixtureSystem()
    samples: list[StackingSample] = []
    rows = []

    for bout in context.ordered_ufc1:
        fight_id = str(bout['fight_id'])
        prob_a = float(probs_a[fight_id])
        prob_b = float(probs_b[fight_id])
        prob_c = float(probs_c[fight_id])

        prediction_sample = StackingSample(
            p_a=prob_a,
            p_b=prob_b,
            p_c=prob_c,
            p_n=None,
            outcome_a_win=0.0,
            tier=EvidenceTier.A,
            scheduled_rounds=int(bout['scheduled_rounds']),
            is_title_fight=bool(bout['is_title_fight']),
            days_since_last_mma=0.0,
            missing_weight_class=False,
            missing_promotion=False,
            opponent_connectivity=0.0,
        )

        if len(samples) >= MIN_STACKING_SAMPLES:
            stacked.fit(samples)
            prob_d = stacked.predict_probability(prediction_sample)
            mode = 'stacked'
        else:
            prob_d = (prob_a + prob_b + prob_c) / 3.0
            mode = 'avg(A,B,C)'

        y_a = (
            1.0
            if bout['outcome_for_a'] == BoutOutcome.WIN
            else 0.0
            if bout['outcome_for_a'] == BoutOutcome.LOSS
            else 0.5
        )

        rows.append(
            {
                'fight_order': bout['fight_order'],
                'fighter': bout['fighter_a_name'],
                'opponent': bout['fighter_b_name'],
                'outcome': bout['outcome_for_a'].value,
                'p_A': round(prob_a, 4),
                'p_B': round(prob_b, 4),
                'p_C': round(prob_c, 4),
                'p_D': round(prob_d, 4),
                'actual_minus_pD': round(y_a - prob_d, 4),
                'mode': mode,
            }
        )
        rows.append(
            {
                'fight_order': bout['fight_order'],
                'fighter': bout['fighter_b_name'],
                'opponent': bout['fighter_a_name'],
                'outcome': _invert_outcome(bout['outcome_for_a']).value,
                'p_A': round(1.0 - prob_a, 4),
                'p_B': round(1.0 - prob_b, 4),
                'p_C': round(1.0 - prob_c, 4),
                'p_D': round(1.0 - prob_d, 4),
                'actual_minus_pD': round((1.0 - y_a) - (1.0 - prob_d), 4),
                'mode': mode,
            }
        )

        samples.append(
            StackingSample(
                p_a=prob_a,
                p_b=prob_b,
                p_c=prob_c,
                p_n=None,
                outcome_a_win=y_a,
                tier=EvidenceTier.A,
                scheduled_rounds=int(bout['scheduled_rounds']),
                is_title_fight=bool(bout['is_title_fight']),
                days_since_last_mma=0.0,
                missing_weight_class=False,
                missing_promotion=False,
                opponent_connectivity=0.0,
            )
        )

    return pd.DataFrame(rows)


def run_system_e(context: UFC1WalkthroughContext) -> tuple[pd.DataFrame, pd.DataFrame]:
    dynamic_system = DynamicFactorBradleyTerrySystem()
    states: dict[str, Any] = {}

    def state(fighter_id: str) -> Any:
        if fighter_id not in states:
            states[fighter_id] = new_dynamic_factor_state(fighter_id)
        return states[fighter_id]

    for bout in context.pre_bouts:
        evidence = _evidence_from_bout(bout, EvidenceTier.C)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)
        post_a, post_b, _delta_a, _delta_b = dynamic_system.update_bout(pre_a, pre_b, evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

    ewr_system = ExpectedWinRatePoolSystem()

    def neutral_evidence(as_of_date: date) -> BoutEvidence:
        return BoutEvidence(
            bout_id='neutral',
            event_date=as_of_date,
            sport_key='mma',
            outcome_for_a=BoutOutcome.DRAW,
            tier=EvidenceTier.B,
            method=FinishMethod.DEC,
            scheduled_rounds=3,
            is_title_fight=False,
            promotion_strength=0.5,
            ps_margin_a=0.0,
            ps_fight_a=0.5,
        )

    def prob_c(fighter_a_id: str, fighter_b_id: str, as_of_date: date) -> float:
        return dynamic_system.expected_win_probability(
            state(fighter_a_id), state(fighter_b_id), neutral_evidence(as_of_date)
        )

    movement_rows = []
    for bout in context.ordered_ufc1:
        as_of_date = bout['event_date']
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        prob_fn = _probability_fn_for_date(prob_c, as_of_date)

        ewr_a_before = ewr_system.compute_ewr(fighter_a_id, context.ufc1_pool_ids, prob_fn)
        ewr_b_before = ewr_system.compute_ewr(fighter_b_id, context.ufc1_pool_ids, prob_fn)

        evidence = _evidence_from_bout(bout, EvidenceTier.A)
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)
        post_a, post_b, _delta_a, _delta_b = dynamic_system.update_bout(pre_a, pre_b, evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

        ewr_a_after = ewr_system.compute_ewr(fighter_a_id, context.ufc1_pool_ids, prob_fn)
        ewr_b_after = ewr_system.compute_ewr(fighter_b_id, context.ufc1_pool_ids, prob_fn)

        movement_rows.append(
            {
                'fight_order': bout['fight_order'],
                'fighter': bout['fighter_a_name'],
                'opponent': bout['fighter_b_name'],
                'outcome': bout['outcome_for_a'].value,
                'ewr_before': round(ewr_a_before, 4),
                'ewr_after': round(ewr_a_after, 4),
                'ewr_delta': round(ewr_a_after - ewr_a_before, 4),
            }
        )
        movement_rows.append(
            {
                'fight_order': bout['fight_order'],
                'fighter': bout['fighter_b_name'],
                'opponent': bout['fighter_a_name'],
                'outcome': _invert_outcome(bout['outcome_for_a']).value,
                'ewr_before': round(ewr_b_before, 4),
                'ewr_after': round(ewr_b_after, 4),
                'ewr_delta': round(ewr_b_after - ewr_b_before, 4),
            }
        )

    uncertainty_map = {
        fighter_id: min(0.49, dynamic_system.mma_uncertainty(state(fighter_id)) / 800.0)
        for fighter_id in context.ufc1_pool_ids
    }
    ranking_rows = ewr_system.rank(
        fighter_ids=context.ufc1_pool_ids,
        reference_pool=context.ufc1_pool_ids,
        probability_fn=lambda x, y: prob_c(x, y, context.event_date),
        uncertainty_map=uncertainty_map,
    )

    ranking_df = pd.DataFrame(
        [
            {
                'fighter': context.ufc1_pool_names.get(row.fighter_id, row.fighter_id),
                'ewr': round(row.ewr, 4),
                'adjusted_score': round(row.adjusted_score, 4),
                'uncertainty_component': round(uncertainty_map.get(row.fighter_id, 0.0), 4),
            }
            for row in ranking_rows
        ]
    )

    return pd.DataFrame(movement_rows), ranking_df


def _probability_fn_for_date(prob_fn: Callable[[str, str, date], float], as_of_date: date) -> ProbabilityFn:
    def _inner(fighter_a_id: str, fighter_b_id: str) -> float:
        return prob_fn(fighter_a_id, fighter_b_id, as_of_date)

    return _inner


def post_ufc1_rankings(
    context: UFC1WalkthroughContext,
    system_a_states: dict[str, Any],
    system_b_states: dict[str, Any],
    system_c_states: dict[str, Any],
    system_e_ranking: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    dynamic = DynamicFactorBradleyTerrySystem()

    rows_a = []
    rows_b = []
    rows_c = []
    for fighter_id in context.ufc1_pool_ids:
        fighter_name = context.ufc1_pool_names.get(fighter_id, fighter_id)

        state_a = system_a_states.get(fighter_id, new_elo_state(fighter_id))
        rows_a.append(
            {
                'fighter': fighter_name,
                'rating': round(float(state_a.rating), 2),
                'mma_fights_count': int(state_a.mma_fights_count),
            }
        )

        state_b = system_b_states.get(fighter_id, new_glicko2_state(fighter_id))
        conservative_b = float(state_b.rating) - float(state_b.rd)
        rows_b.append(
            {
                'fighter': fighter_name,
                'rating': round(float(state_b.rating), 2),
                'rd': round(float(state_b.rd), 2),
                'conservative_rating': round(conservative_b, 2),
                'mma_fights_count': int(state_b.mma_fights_count),
            }
        )

        state_c = system_c_states.get(fighter_id, new_dynamic_factor_state(fighter_id))
        mma_rating = dynamic.implied_mma_rating(state_c)
        mma_uncertainty = dynamic.mma_uncertainty(state_c)
        conservative_c = mma_rating - mma_uncertainty
        rows_c.append(
            {
                'fighter': fighter_name,
                'mma_rating': round(float(mma_rating), 2),
                'mma_uncertainty': round(float(mma_uncertainty), 2),
                'conservative_rating': round(float(conservative_c), 2),
                'mma_fights_count': int(state_c.mma_fights_count),
            }
        )

    rankings = {
        'system_a': pd.DataFrame(rows_a)
        .sort_values(['rating', 'fighter'], ascending=[False, True])
        .reset_index(drop=True),
        'system_b': pd.DataFrame(rows_b)
        .sort_values(['conservative_rating', 'rating', 'fighter'], ascending=[False, False, True])
        .reset_index(drop=True),
        'system_c': pd.DataFrame(rows_c)
        .sort_values(['conservative_rating', 'mma_rating', 'fighter'], ascending=[False, False, True])
        .reset_index(drop=True),
    }

    if system_e_ranking is not None and not system_e_ranking.empty:
        rankings['system_e'] = (
            system_e_ranking.sort_values(['adjusted_score', 'ewr', 'fighter'], ascending=[False, False, True])
            .reset_index(drop=True)
            .copy()
        )

    return rankings


def system_a_ps_margin_impact(context: UFC1WalkthroughContext) -> pd.DataFrame:
    system = EloPerformanceRatingSystem()
    states: dict[str, Any] = {}

    def state(fighter_id: str) -> Any:
        if fighter_id not in states:
            states[fighter_id] = new_elo_state(fighter_id)
        return states[fighter_id]

    for bout in context.pre_bouts:
        if str(bout['sport']) != 'mma':
            continue
        evidence = _evidence_from_bout(bout, EvidenceTier.C)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        post_a, post_b, _delta_a, _delta_b = system.update_bout(state(fighter_a_id), state(fighter_b_id), evidence)
        states[fighter_a_id] = post_a
        states[fighter_b_id] = post_b

    rows = []
    for bout in context.ordered_ufc1:
        evidence = _evidence_from_bout(bout, EvidenceTier.A)
        fighter_a_id = str(bout['fighter_a_id'])
        fighter_b_id = str(bout['fighter_b_id'])
        pre_a = state(fighter_a_id)
        pre_b = state(fighter_b_id)

        _post_a, _post_b, delta_a, delta_b = system.update_bout(pre_a, pre_b, evidence)

        zero_margin = replace(evidence, ps_margin_a=0.0)
        _post_a_zero, _post_b_zero, delta_a_zero, delta_b_zero = system.update_bout(pre_a, pre_b, zero_margin)

        rows.append(
            {
                'fight_order': bout['fight_order'],
                'fighter': bout['fighter_a_name'],
                'opponent': bout['fighter_b_name'],
                'outcome': bout['outcome_for_a'].value,
                'method': bout['method'].value,
                'ps_margin': round(float(bout['ps_margin_a']), 4),
                'target_score': round(float(delta_a.target_score), 4),
                'target_score_no_ps': round(float(delta_a_zero.target_score), 4),
                'delta_with_ps': round(float(delta_a.delta_rating), 4),
                'delta_no_ps': round(float(delta_a_zero.delta_rating), 4),
                'delta_gain_from_ps': round(float(delta_a.delta_rating - delta_a_zero.delta_rating), 4),
            }
        )
        rows.append(
            {
                'fight_order': bout['fight_order'],
                'fighter': bout['fighter_b_name'],
                'opponent': bout['fighter_a_name'],
                'outcome': _invert_outcome(bout['outcome_for_a']).value,
                'method': bout['method'].value,
                'ps_margin': round(-float(bout['ps_margin_a']), 4),
                'target_score': round(float(delta_b.target_score), 4),
                'target_score_no_ps': round(float(delta_b_zero.target_score), 4),
                'delta_with_ps': round(float(delta_b.delta_rating), 4),
                'delta_no_ps': round(float(delta_b_zero.delta_rating), 4),
                'delta_gain_from_ps': round(float(delta_b.delta_rating - delta_b_zero.delta_rating), 4),
            }
        )

        states[fighter_a_id], states[fighter_b_id], _d1, _d2 = system.update_bout(pre_a, pre_b, evidence)

    return pd.DataFrame(rows)


def _as_float(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if text in {'', '--'} or text.lower() == 'nan':
        return default
    return float(text)


def _parse_int_with_default(value: str | None, default: int = 0) -> int:
    parsed = parse_int(value)
    return default if parsed is None else parsed


def _parse_seconds_value(value: str | None) -> int | None:
    parsed_mmss = parse_mmss_to_seconds(value)
    if parsed_mmss is not None:
        return parsed_mmss
    return parse_int(value)


def _parse_finish_method(raw_method: str | None) -> FinishMethod:
    text = (raw_method or '').strip().upper()
    if 'SUB' in text:
        return FinishMethod.SUB
    if 'KO' in text and 'TKO' not in text:
        return FinishMethod.KO
    if 'TKO' in text or 'KO/TKO' in text:
        return FinishMethod.TKO
    if 'DEC' in text:
        return FinishMethod.DEC
    if 'DQ' in text:
        return FinishMethod.DQ
    return FinishMethod.OTHER


def _parse_outcome_from_winner(winner_id: str, fighter_a: str, fighter_b: str) -> BoutOutcome:
    if winner_id == fighter_a:
        return BoutOutcome.WIN
    if winner_id == fighter_b:
        return BoutOutcome.LOSS
    return BoutOutcome.DRAW


def _round_stats_for(sources: _RoundStatsSources, fight_id: str, fighter_id: str, round_num: int) -> FighterRoundStats:
    base = sources.fight_rounds[
        (sources.fight_rounds['ufcstats_fight_id'] == fight_id)
        & (sources.fight_rounds['ufcstats_fighter_id'] == fighter_id)
        & (sources.fight_rounds['round'] == str(round_num))
    ]
    target = sources.sig_target[
        (sources.sig_target['ufcstats_fight_id'] == fight_id)
        & (sources.sig_target['ufcstats_fighter_id'] == fighter_id)
        & (sources.sig_target['round'] == str(round_num))
    ]
    position = sources.sig_position[
        (sources.sig_position['ufcstats_fight_id'] == fight_id)
        & (sources.sig_position['ufcstats_fighter_id'] == fighter_id)
        & (sources.sig_position['round'] == str(round_num))
    ]

    base_row = base.iloc[0] if not base.empty else pd.Series(dtype='object')
    target_row = target.iloc[0] if not target.empty else pd.Series(dtype='object')
    position_row = position.iloc[0] if not position.empty else pd.Series(dtype='object')

    return FighterRoundStats(
        kd=_as_float(base_row.get('kd')),
        sig_landed=_as_float(base_row.get('sig_str_land')),
        sig_attempted=_as_float(base_row.get('sig_str_att')),
        total_landed=_as_float(base_row.get('total_str_land')),
        total_attempted=_as_float(base_row.get('total_str_att')),
        td_landed=_as_float(base_row.get('td_land')),
        td_attempted=_as_float(base_row.get('td_att')),
        sub_attempts=_as_float(base_row.get('sub_att')),
        rev=_as_float(base_row.get('rev')),
        ctrl_seconds=float(_parse_seconds_value(base_row.get('ctrl')) or 0),
        head_landed=_as_float(target_row.get('head_land')),
        body_landed=_as_float(target_row.get('body_land')),
        leg_landed=_as_float(target_row.get('leg_land')),
        distance_landed=_as_float(position_row.get('distance_land')),
        clinch_landed=_as_float(position_row.get('clinch_land')),
        ground_landed=_as_float(position_row.get('ground_land')),
    )


def _compute_ps_for_ufc_fight(
    fight_id: str,
    fighter_a_ufc: str,
    fighter_b_ufc: str,
    winner_ufc: str,
    finish_round: int,
    finish_time_seconds: int | None,
    sources: _RoundStatsSources,
) -> tuple[float, float]:
    round_values = sources.fight_rounds[sources.fight_rounds['ufcstats_fight_id'] == fight_id]['round']
    round_nums = sorted({_parse_int_with_default(value) for value in round_values if str(value).strip().isdigit()})
    if not round_nums:
        round_nums = [1]

    round_results = []
    round_metas = []
    for round_num in round_nums:
        a_stats = _round_stats_for(sources, fight_id, fighter_a_ufc, round_num)
        b_stats = _round_stats_for(sources, fight_id, fighter_b_ufc, round_num)
        meta = RoundMeta(
            round_num=round_num,
            round_seconds=300,
            is_finish_round=(round_num == finish_round),
            finish_elapsed_seconds=finish_time_seconds if round_num == finish_round else None,
            winner_is_a=(winner_ufc == fighter_a_ufc),
        )
        round_results.append(compute_ps_round(a_stats, b_stats, meta))
        round_metas.append(meta)

    fight_ps = aggregate_fight_ps(round_results, round_metas)
    return fight_ps.ps_fight_a - fight_ps.ps_fight_b, fight_ps.ps_fight_a


def _resolve_canonical_from_ufc(
    ufc_fighter_id: str, ufc_to_tap: dict[str, str], ufc_name_by_id: dict[str, str], tap_name_by_id: dict[str, str]
) -> tuple[str, str, str | None]:
    tap_id = ufc_to_tap.get(ufc_fighter_id)
    if tap_id:
        return f'tap:{tap_id}', tap_name_by_id.get(tap_id, ufc_name_by_id.get(ufc_fighter_id, ufc_fighter_id)), tap_id
    return f'ufc:{ufc_fighter_id}', ufc_name_by_id.get(ufc_fighter_id, ufc_fighter_id), None


def _infer_same_day_order(bouts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline: dict[str, int] = {}
    for bout in bouts:
        for fighter_key, total_key in (('fighter_a_id', 'prefight_total_a'), ('fighter_b_id', 'prefight_total_b')):
            fighter_id = str(bout[fighter_key])
            total = bout[total_key]
            if total is None:
                continue
            total_int = int(total)
            existing = baseline.get(fighter_id)
            if existing is None or total_int < existing:
                baseline[fighter_id] = total_int

    ordered_rows = []
    for bout in bouts:
        totals = [value for value in [bout['prefight_total_a'], bout['prefight_total_b']] if value is not None]
        deltas = []
        if bout['prefight_total_a'] is not None:
            base_a = baseline.get(str(bout['fighter_a_id']), int(bout['prefight_total_a']))
            deltas.append(max(0, int(bout['prefight_total_a']) - base_a))
        if bout['prefight_total_b'] is not None:
            base_b = baseline.get(str(bout['fighter_b_id']), int(bout['prefight_total_b']))
            deltas.append(max(0, int(bout['prefight_total_b']) - base_b))

        stage = max(deltas) if deltas else 0
        min_total = min(totals) if totals else 10**9
        max_total = max(totals) if totals else 10**9
        pair_label = ' vs '.join(sorted([str(bout['fighter_a_name']), str(bout['fighter_b_name'])], key=str.lower))

        row = dict(bout)
        row['same_day_stage'] = stage
        row['same_day_min_total'] = min_total
        row['same_day_max_total'] = max_total
        row['pair_label'] = pair_label
        ordered_rows.append(row)

    ordered_rows.sort(
        key=lambda row: (
            int(row['same_day_stage']),
            int(row['same_day_min_total']),
            int(row['same_day_max_total']),
            str(row['pair_label']),
            str(row['fight_id']),
        )
    )

    for idx, row in enumerate(ordered_rows, start=1):
        row['fight_order'] = idx
    return ordered_rows


def _normalize_precheckpoint_bouts(pre_rows: pd.DataFrame, tap_name_by_id: dict[str, str]) -> list[dict[str, Any]]:
    bouts = []
    for row in pre_rows.itertuples(index=False):
        tap_a = str(row.tapology_fighter_id)
        fighter_a_id = f'tap:{tap_a}'
        fighter_a_name = tap_name_by_id.get(tap_a, tap_a)

        opponent_slug = str(row.opponent_tapology_slug or '').strip()
        opponent_name = str(row.opponent_name or '').strip() or opponent_slug or 'Unknown Opponent'
        if opponent_slug:
            opponent_tap = opponent_slug
        else:
            fallback = re.sub(r'[^a-z0-9]+', '-', opponent_name.lower()).strip('-')
            opponent_tap = fallback if fallback else f'unknown-opponent-{row.tapology_bout_id}'
        fighter_b_id = f'tap:{opponent_tap}'
        fighter_b_name = tap_name_by_id.get(opponent_tap, opponent_name)

        event_date = parse_date_value(str(row.event_date))
        if event_date is None:
            continue

        normalized_outcome = normalize_bout_result(clean_text(str(row.result)))
        bouts.append(
            {
                'bout_id': f'tap_pre_{row.tapology_bout_id}',
                'event_date': event_date,
                'sport': str(row.sport or 'unknown').lower(),
                'fighter_a_id': fighter_a_id,
                'fighter_a_name': fighter_a_name,
                'fighter_b_id': fighter_b_id,
                'fighter_b_name': fighter_b_name,
                'outcome_for_a': _bout_outcome_from_result(normalized_outcome),
                'method': _parse_finish_method(str(row.method)),
                'scheduled_rounds': 3,
                'is_title_fight': str(row.is_title_fight).lower() == 'true',
                'promotion_strength': 0.5,
                'finish_round': None,
                'finish_time_seconds': None,
                'ps_margin_a': None,
                'ps_fight_a': None,
                'record_before_a': str(row.fighter_record_before_raw or '').strip() or None,
                'event_name': str(row.event_name or ''),
            }
        )

    bouts.sort(key=lambda row: (row['event_date'], str(row['bout_id']), str(row['fighter_a_id'])))
    return bouts


def _evidence_from_bout(bout: dict[str, Any], tier: EvidenceTier) -> BoutEvidence:
    return BoutEvidence(
        bout_id=str(bout['bout_id']),
        event_date=bout['event_date'],
        sport_key=str(bout['sport']),
        outcome_for_a=bout['outcome_for_a'],
        tier=tier,
        method=bout['method'],
        finish_round=bout['finish_round'],
        finish_time_seconds=bout['finish_time_seconds'],
        scheduled_rounds=int(bout['scheduled_rounds']),
        is_title_fight=bool(bout['is_title_fight']),
        promotion_strength=float(bout['promotion_strength']) if bout['promotion_strength'] is not None else None,
        ps_margin_a=bout['ps_margin_a'],
        ps_fight_a=bout['ps_fight_a'],
    )


def _invert_outcome(outcome: BoutOutcome) -> BoutOutcome:
    return _bout_outcome_from_result(inverse_bout_result(_bout_result_from_outcome(outcome)))


def _bout_outcome_from_result(result: BoutResultEnum) -> BoutOutcome:
    if result == BoutResultEnum.WIN:
        return BoutOutcome.WIN
    if result == BoutResultEnum.LOSS:
        return BoutOutcome.LOSS
    if result == BoutResultEnum.NO_CONTEST:
        return BoutOutcome.NO_CONTEST
    return BoutOutcome.DRAW


def _bout_result_from_outcome(outcome: BoutOutcome) -> BoutResultEnum:
    if outcome == BoutOutcome.WIN:
        return BoutResultEnum.WIN
    if outcome == BoutOutcome.LOSS:
        return BoutResultEnum.LOSS
    if outcome == BoutOutcome.NO_CONTEST:
        return BoutResultEnum.NO_CONTEST
    return BoutResultEnum.DRAW


def _one_sided_row(bout: dict[str, Any], delta: Any, for_fighter_a: bool) -> dict[str, Any]:
    if for_fighter_a:
        fighter = bout['fighter_a_name']
        opponent = bout['fighter_b_name']
        outcome = bout['outcome_for_a'].value
        ps_margin = round(float(bout['ps_margin_a']), 4)
    else:
        fighter = bout['fighter_b_name']
        opponent = bout['fighter_a_name']
        outcome = _invert_outcome(bout['outcome_for_a']).value
        ps_margin = round(-float(bout['ps_margin_a']), 4)

    return {
        'fight_order': bout['fight_order'],
        'fight_id': bout['fight_id'],
        'fighter': fighter,
        'opponent': opponent,
        'outcome': outcome,
        'pre_rating': round(delta.pre_rating, 2),
        'post_rating': round(delta.post_rating, 2),
        'delta': round(delta.delta_rating, 4),
        'expected_win_prob': round(delta.expected_win_prob, 4),
        'target_score': round(delta.target_score, 4),
        'k_effective': round(delta.k_effective, 4),
        'ps_margin': ps_margin,
    }


def _two_sided_rows_for_standard_system(bout: dict[str, Any], delta_a: Any, delta_b: Any) -> list[dict[str, Any]]:
    return [_one_sided_row(bout, delta_a, for_fighter_a=True), _one_sided_row(bout, delta_b, for_fighter_a=False)]
