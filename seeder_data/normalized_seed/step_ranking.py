from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import Connection, and_, delete, select, text

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
    RoundMeta,
    StackedLogitMixtureSystem,
    StackingSample,
    aggregate_fight_ps,
    compute_ps_round,
    new_dynamic_factor_state,
    new_elo_state,
    new_glicko2_state,
)
from elo_calculator.domain.shared.enumerations import BoutResultEnum, MethodGroupEnum
from elo_calculator.infrastructure.database import schema as db_schema
from seeder_data.normalized_seed.helpers import parse_date_value, parse_prefight_record_total

if TYPE_CHECKING:
    from seeder_data.normalized_seed.core import NormalizedCsvSeeder

PS_VERSION = 'ps_round_v1'
SYSTEM_VERSION = 'v1'
_SYSTEM_A_KEY = 'elo_ps'
_SYSTEM_B_KEY = 'glicko2_ps'
_SYSTEM_C_KEY = 'dynamic_factor_bt'
_SYSTEM_D_KEY = 'stacked_logit_mixture'
_SYSTEM_E_KEY = 'expected_win_rate_pool'
PARTICIPANTS_PER_BOUT = 2
STACK_TRAIN_MIN_SAMPLES = 20
REFERENCE_POOL_LIMIT = 300


@dataclass(frozen=True)
class _BoutContext:
    bout_id: Any
    checkpoint_date: date
    processing_date: date
    sport_key: str
    method: FinishMethod
    is_title_fight: bool
    scheduled_rounds: int
    promotion_strength: float | None
    finish_round: int | None
    finish_time_seconds: int | None
    has_division: bool
    has_promotion: bool
    division_id: Any | None


@dataclass(frozen=True)
class _BoutPair:
    fighter_a_id: Any
    fighter_b_id: Any
    outcome_for_a: BoutOutcome
    fighter_a_name: str
    fighter_b_name: str
    prefight_record_a: str | None
    prefight_record_b: str | None
    prefight_total_a: int | None
    prefight_total_b: int | None


@dataclass(frozen=True)
class _PsArtifacts:
    ps_margin_by_key: dict[tuple[Any, Any], float]
    ps_fight_by_key: dict[tuple[Any, Any], float]
    tier_by_bout: dict[Any, EvidenceTier]


@dataclass
class _RatingRuntime:
    elo_system: EloPerformanceRatingSystem
    glicko_system: Glicko2PerformanceRatingSystem
    dynamic_system: DynamicFactorBradleyTerrySystem
    stacked_system: StackedLogitMixtureSystem
    ewr_system: ExpectedWinRatePoolSystem
    elo_states: dict[Any, Any]
    glicko_states: dict[Any, Any]
    dynamic_states: dict[Any, Any]
    mma_fight_counts: dict[Any, int]
    mma_last_date: dict[Any, date]
    delta_rows: list[dict]
    snapshot_map: dict[tuple[Any, Any, date], dict]
    stacked_samples: list[StackingSample]


def seed_ranking_artifacts(seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    _reset_computed_artifacts(conn)
    _ensure_snapshot_default_partition(conn)

    system_ids = _seed_rating_systems(seeder, conn)
    contexts = _load_bout_contexts(conn)
    bout_pairs = _load_bout_pairs(conn)
    round_stats_by_bout = _load_round_stats(conn)
    fight_totals_by_bout = _load_fight_totals(conn)

    ps_artifacts = _compute_ps_artifacts(seeder, conn, contexts, bout_pairs, round_stats_by_bout, fight_totals_by_bout)

    final_context_date = max((context.checkpoint_date for context in contexts), default=date(1900, 1, 1))
    _seed_rating_artifacts(seeder, conn, contexts, bout_pairs, ps_artifacts, system_ids, final_context_date)


def _reset_computed_artifacts(conn: Connection) -> None:
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


def _seed_rating_systems(seeder: NormalizedCsvSeeder, conn: Connection) -> dict[str, Any]:
    rows = [
        {
            'system_key': _SYSTEM_A_KEY,
            'sport_id': seeder.sport_id_by_key.get('mma'),
            'description': 'System A: Elo-PS MMA updates with tiered evidence.',
            'param_json': {'tier_multipliers': {'A': 1.0, 'B': 0.85, 'C': 0.70}},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_B_KEY,
            'sport_id': seeder.sport_id_by_key.get('mma'),
            'description': 'System B: Glicko-2-PS uncertainty-aware MMA updates.',
            'param_json': {'tau': 0.5, 'rd_growth': 0.06},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_C_KEY,
            'sport_id': seeder.sport_id_by_key.get('mma'),
            'description': 'System C: Dynamic factor Bradley-Terry with cross-sport transfer.',
            'param_json': {'latents': ['mma', 'striking', 'grappling', 'durability']},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_D_KEY,
            'sport_id': seeder.sport_id_by_key.get('mma'),
            'description': 'System D: Stacked logistic mixture of A/B/C probabilities.',
            'param_json': {'features': ['tier', 'rounds', 'title', 'inactivity', 'missingness', 'connectivity']},
            'code_version': SYSTEM_VERSION,
        },
        {
            'system_key': _SYSTEM_E_KEY,
            'sport_id': seeder.sport_id_by_key.get('mma'),
            'description': 'System E: Expected-win-rate pool ranking (uncertainty adjusted).',
            'param_json': {'reference_pool': 'most_active_mma_fighters'},
            'code_version': SYSTEM_VERSION,
        },
    ]
    seeder.bulk_insert(conn, db_schema.dim_rating_system_table, rows)

    result = conn.execute(
        select(
            db_schema.dim_rating_system_table.c.system_id,
            db_schema.dim_rating_system_table.c.system_key,
            db_schema.dim_rating_system_table.c.code_version,
        )
    ).all()

    system_ids: dict[str, Any] = {}
    for row in result:
        if row.code_version == SYSTEM_VERSION:
            system_ids[str(row.system_key)] = row.system_id
    return system_ids


def _load_bout_contexts(conn: Connection) -> list[_BoutContext]:
    query = (
        select(
            db_schema.fact_bout_table.c.bout_id,
            db_schema.fact_event_table.c.event_date,
            db_schema.dim_sport_table.c.sport_key,
            db_schema.fact_bout_table.c.method_group,
            db_schema.fact_bout_table.c.is_title_fight,
            db_schema.fact_bout_table.c.scheduled_rounds,
            db_schema.fact_bout_table.c.finish_round,
            db_schema.fact_bout_table.c.finish_time_seconds,
            db_schema.fact_bout_table.c.division_id,
            db_schema.fact_event_table.c.promotion_id,
            db_schema.dim_promotion_table.c.strength,
            db_schema.fact_bout_table.c.source_meta_json,
        )
        .select_from(
            db_schema.fact_bout_table.join(
                db_schema.fact_event_table,
                db_schema.fact_bout_table.c.event_id == db_schema.fact_event_table.c.event_id,
            )
            .join(
                db_schema.dim_sport_table, db_schema.fact_bout_table.c.sport_id == db_schema.dim_sport_table.c.sport_id
            )
            .outerjoin(
                db_schema.dim_promotion_table,
                db_schema.fact_event_table.c.promotion_id == db_schema.dim_promotion_table.c.promotion_id,
            )
        )
        .order_by(db_schema.fact_event_table.c.event_date, db_schema.fact_bout_table.c.bout_id)
    )

    contexts: list[_BoutContext] = []
    for row in conn.execute(query).all():
        checkpoint_date = row.event_date
        processing_date = _resolve_processing_date(row.source_meta_json, checkpoint_date)
        contexts.append(
            _BoutContext(
                bout_id=row.bout_id,
                checkpoint_date=checkpoint_date,
                processing_date=processing_date,
                sport_key=str(row.sport_key or 'unknown'),
                method=_method_from_group(row.method_group),
                is_title_fight=bool(row.is_title_fight),
                scheduled_rounds=int(row.scheduled_rounds or 3),
                promotion_strength=float(row.strength) if row.strength is not None else None,
                finish_round=int(row.finish_round) if row.finish_round is not None else None,
                finish_time_seconds=int(row.finish_time_seconds) if row.finish_time_seconds is not None else None,
                has_division=row.division_id is not None,
                has_promotion=row.promotion_id is not None,
                division_id=row.division_id,
            )
        )
    return contexts


def _resolve_processing_date(source_meta_json: Any, fallback_date: date) -> date:
    if isinstance(source_meta_json, dict):
        tapology_date = source_meta_json.get('tapology_event_date')
        parsed_date = parse_date_value(str(tapology_date)) if tapology_date else None
        if parsed_date is not None:
            return parsed_date
    return fallback_date


def _method_from_group(method_group: MethodGroupEnum | str | None) -> FinishMethod:
    mapping = {
        MethodGroupEnum.KO: FinishMethod.KO,
        MethodGroupEnum.TKO: FinishMethod.TKO,
        MethodGroupEnum.SUB: FinishMethod.SUB,
        MethodGroupEnum.DEC: FinishMethod.DEC,
        MethodGroupEnum.DQ: FinishMethod.DQ,
        MethodGroupEnum.OVERTURNED: FinishMethod.OVERTURNED,
    }
    if method_group in mapping:
        return mapping[method_group]
    if method_group is None:
        return FinishMethod.UNKNOWN
    normalized = str(method_group).upper()
    return FinishMethod(normalized) if normalized in {item.value for item in FinishMethod} else FinishMethod.UNKNOWN


def _load_bout_pairs(conn: Connection) -> dict[Any, _BoutPair]:
    rows = conn.execute(
        select(
            db_schema.fact_bout_participant_table.c.bout_id,
            db_schema.fact_bout_participant_table.c.fighter_id,
            db_schema.fact_bout_participant_table.c.outcome_key,
            db_schema.fact_bout_participant_table.c.prefight_record,
            db_schema.dim_fighter_table.c.display_name,
        ).select_from(
            db_schema.fact_bout_participant_table.outerjoin(
                db_schema.dim_fighter_table,
                db_schema.fact_bout_participant_table.c.fighter_id == db_schema.dim_fighter_table.c.fighter_id,
            )
        )
    ).all()

    grouped: dict[Any, list[tuple[Any, BoutOutcome, str | None, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.bout_id].append(
            (
                row.fighter_id,
                _coerce_outcome(row.outcome_key),
                str(row.prefight_record) if row.prefight_record is not None else None,
                str(row.display_name or row.fighter_id),
            )
        )

    pairs: dict[Any, _BoutPair] = {}
    for bout_id, participants in grouped.items():
        if len(participants) != PARTICIPANTS_PER_BOUT:
            continue
        ordered = sorted(participants, key=lambda item: str(item[0]))
        (fighter_a, outcome_a, record_a, name_a), (fighter_b, outcome_b, record_b, name_b) = ordered
        resolved = _resolve_outcome(outcome_a, outcome_b)
        pairs[bout_id] = _BoutPair(
            fighter_a_id=fighter_a,
            fighter_b_id=fighter_b,
            outcome_for_a=resolved,
            fighter_a_name=name_a,
            fighter_b_name=name_b,
            prefight_record_a=record_a,
            prefight_record_b=record_b,
            prefight_total_a=parse_prefight_record_total(record_a),
            prefight_total_b=parse_prefight_record_total(record_b),
        )

    return pairs


def _coerce_outcome(value: BoutResultEnum | str | None) -> BoutOutcome:
    if value == BoutResultEnum.WIN or str(value) == BoutResultEnum.WIN.value:
        return BoutOutcome.WIN
    if value == BoutResultEnum.LOSS or str(value) == BoutResultEnum.LOSS.value:
        return BoutOutcome.LOSS
    if value == BoutResultEnum.DRAW or str(value) == BoutResultEnum.DRAW.value:
        return BoutOutcome.DRAW
    if value == BoutResultEnum.NO_CONTEST or str(value) == BoutResultEnum.NO_CONTEST.value:
        return BoutOutcome.NO_CONTEST
    return BoutOutcome.DRAW


def _resolve_outcome(outcome_a: BoutOutcome, outcome_b: BoutOutcome) -> BoutOutcome:
    if outcome_a in {BoutOutcome.WIN, BoutOutcome.LOSS, BoutOutcome.DRAW, BoutOutcome.NO_CONTEST}:
        return outcome_a
    if outcome_b == BoutOutcome.WIN:
        return BoutOutcome.LOSS
    if outcome_b == BoutOutcome.LOSS:
        return BoutOutcome.WIN
    return outcome_b


def _ordered_contexts(contexts: list[_BoutContext], bout_pairs: dict[Any, _BoutPair]) -> list[_BoutContext]:
    grouped: dict[tuple[date, date], list[_BoutContext]] = defaultdict(list)
    for context in contexts:
        grouped[(context.processing_date, context.checkpoint_date)].append(context)

    ordered: list[_BoutContext] = []
    for _group_key, same_day_contexts in sorted(grouped.items(), key=lambda item: item[0]):
        ordered.extend(_ordered_same_day_contexts(same_day_contexts, bout_pairs))
    return ordered


def _ordered_same_day_contexts(contexts: list[_BoutContext], bout_pairs: dict[Any, _BoutPair]) -> list[_BoutContext]:
    day_baseline: dict[Any, int] = {}
    for context in contexts:
        pair = bout_pairs.get(context.bout_id)
        if pair is None:
            continue
        _update_day_baseline(day_baseline, pair.fighter_a_id, pair.prefight_total_a)
        _update_day_baseline(day_baseline, pair.fighter_b_id, pair.prefight_total_b)

    decorated = []
    for context in contexts:
        pair = bout_pairs.get(context.bout_id)
        stage, min_total, max_total, pair_label = _same_day_order_features(pair, day_baseline)
        decorated.append((stage, min_total, max_total, pair_label, str(context.bout_id), context))

    decorated.sort(key=lambda row: (row[0], row[1], row[2], row[3], row[4]))
    return [row[5] for row in decorated]


def _update_day_baseline(day_baseline: dict[Any, int], fighter_id: Any, prefight_total: int | None) -> None:
    if prefight_total is None:
        return
    existing = day_baseline.get(fighter_id)
    if existing is None or prefight_total < existing:
        day_baseline[fighter_id] = prefight_total


def _same_day_order_features(pair: _BoutPair | None, day_baseline: dict[Any, int]) -> tuple[int, int, int, str]:
    if pair is None:
        return 0, 10**9, 10**9, 'unknown'

    totals: list[int] = []
    if pair.prefight_total_a is not None:
        totals.append(pair.prefight_total_a)
    if pair.prefight_total_b is not None:
        totals.append(pair.prefight_total_b)

    stage_deltas: list[int] = []
    if pair.prefight_total_a is not None:
        base_a = day_baseline.get(pair.fighter_a_id, pair.prefight_total_a)
        stage_deltas.append(max(0, pair.prefight_total_a - base_a))
    if pair.prefight_total_b is not None:
        base_b = day_baseline.get(pair.fighter_b_id, pair.prefight_total_b)
        stage_deltas.append(max(0, pair.prefight_total_b - base_b))

    stage = max(stage_deltas) if stage_deltas else 0
    min_total = min(totals) if totals else 10**9
    max_total = max(totals) if totals else 10**9

    names = sorted((pair.fighter_a_name, pair.fighter_b_name), key=lambda value: value.lower())
    pair_label = f'{names[0]} vs {names[1]}'
    return stage, min_total, max_total, pair_label


def _load_round_stats(conn: Connection) -> dict[Any, dict[int, dict[Any, FighterRoundStats]]]:
    query = (
        select(
            db_schema.fact_round_stats_table.c.bout_id,
            db_schema.fact_round_stats_table.c.round_num,
            db_schema.fact_round_stats_table.c.fighter_id,
            db_schema.fact_round_stats_table.c.kd,
            db_schema.fact_round_stats_table.c.sig_landed,
            db_schema.fact_round_stats_table.c.sig_attempted,
            db_schema.fact_round_stats_table.c.total_landed,
            db_schema.fact_round_stats_table.c.total_attempted,
            db_schema.fact_round_stats_table.c.td_landed,
            db_schema.fact_round_stats_table.c.td_attempted,
            db_schema.fact_round_stats_table.c.sub_attempts,
            db_schema.fact_round_stats_table.c.rev,
            db_schema.fact_round_stats_table.c.ctrl_seconds,
            db_schema.fact_round_sig_by_target_table.c.head_landed,
            db_schema.fact_round_sig_by_target_table.c.body_landed,
            db_schema.fact_round_sig_by_target_table.c.leg_landed,
            db_schema.fact_round_sig_by_position_table.c.distance_landed,
            db_schema.fact_round_sig_by_position_table.c.clinch_landed,
            db_schema.fact_round_sig_by_position_table.c.ground_landed,
        )
        .select_from(
            db_schema.fact_round_stats_table.outerjoin(
                db_schema.fact_round_sig_by_target_table,
                and_(
                    db_schema.fact_round_stats_table.c.bout_id == db_schema.fact_round_sig_by_target_table.c.bout_id,
                    db_schema.fact_round_stats_table.c.round_num
                    == db_schema.fact_round_sig_by_target_table.c.round_num,
                    db_schema.fact_round_stats_table.c.fighter_id
                    == db_schema.fact_round_sig_by_target_table.c.fighter_id,
                ),
            ).outerjoin(
                db_schema.fact_round_sig_by_position_table,
                and_(
                    db_schema.fact_round_stats_table.c.bout_id == db_schema.fact_round_sig_by_position_table.c.bout_id,
                    db_schema.fact_round_stats_table.c.round_num
                    == db_schema.fact_round_sig_by_position_table.c.round_num,
                    db_schema.fact_round_stats_table.c.fighter_id
                    == db_schema.fact_round_sig_by_position_table.c.fighter_id,
                ),
            )
        )
        .where(db_schema.fact_round_stats_table.c.round_num > 0)
    )

    stats_by_bout: dict[Any, dict[int, dict[Any, FighterRoundStats]]] = defaultdict(lambda: defaultdict(dict))
    for row in conn.execute(query).all():
        stats_by_bout[row.bout_id][int(row.round_num)][row.fighter_id] = FighterRoundStats(
            kd=float(row.kd or 0),
            sig_landed=float(row.sig_landed or 0),
            sig_attempted=float(row.sig_attempted or 0),
            total_landed=float(row.total_landed or 0),
            total_attempted=float(row.total_attempted or 0),
            td_landed=float(row.td_landed or 0),
            td_attempted=float(row.td_attempted or 0),
            sub_attempts=float(row.sub_attempts or 0),
            rev=float(row.rev or 0),
            ctrl_seconds=float(row.ctrl_seconds or 0),
            head_landed=float(row.head_landed or 0),
            body_landed=float(row.body_landed or 0),
            leg_landed=float(row.leg_landed or 0),
            distance_landed=float(row.distance_landed or 0),
            clinch_landed=float(row.clinch_landed or 0),
            ground_landed=float(row.ground_landed or 0),
        )
    return stats_by_bout


def _load_fight_totals(conn: Connection) -> dict[Any, dict[Any, FighterRoundStats]]:
    rows = conn.execute(
        select(
            db_schema.fact_fight_totals_table.c.bout_id,
            db_schema.fact_fight_totals_table.c.fighter_id,
            db_schema.fact_fight_totals_table.c.kd,
            db_schema.fact_fight_totals_table.c.sig_landed,
            db_schema.fact_fight_totals_table.c.sig_attempted,
            db_schema.fact_fight_totals_table.c.total_landed,
            db_schema.fact_fight_totals_table.c.total_attempted,
            db_schema.fact_fight_totals_table.c.td_landed,
            db_schema.fact_fight_totals_table.c.td_attempted,
            db_schema.fact_fight_totals_table.c.sub_attempts,
            db_schema.fact_fight_totals_table.c.rev,
            db_schema.fact_fight_totals_table.c.ctrl_seconds,
        )
    ).all()

    totals: dict[Any, dict[Any, FighterRoundStats]] = defaultdict(dict)
    for row in rows:
        sig_landed = float(row.sig_landed or 0)
        totals[row.bout_id][row.fighter_id] = FighterRoundStats(
            kd=float(row.kd or 0),
            sig_landed=sig_landed,
            sig_attempted=float(row.sig_attempted or 0),
            total_landed=float(row.total_landed or 0),
            total_attempted=float(row.total_attempted or 0),
            td_landed=float(row.td_landed or 0),
            td_attempted=float(row.td_attempted or 0),
            sub_attempts=float(row.sub_attempts or 0),
            rev=float(row.rev or 0),
            ctrl_seconds=float(row.ctrl_seconds or 0),
            head_landed=sig_landed,
            body_landed=0.0,
            leg_landed=0.0,
            ground_landed=0.0,
        )
    return totals


def _compute_ps_artifacts(
    seeder: NormalizedCsvSeeder,
    conn: Connection,
    contexts: list[_BoutContext],
    bout_pairs: dict[Any, _BoutPair],
    round_stats_by_bout: dict[Any, dict[int, dict[Any, FighterRoundStats]]],
    fight_totals_by_bout: dict[Any, dict[Any, FighterRoundStats]],
) -> _PsArtifacts:
    round_rows: list[dict] = []
    fight_rows: list[dict] = []
    ps_margin_by_key: dict[tuple[Any, Any], float] = {}
    ps_fight_by_key: dict[tuple[Any, Any], float] = {}
    tier_by_bout: dict[Any, EvidenceTier] = {}

    context_by_bout = {context.bout_id: context for context in contexts}

    for bout_id, pair in bout_pairs.items():
        context = context_by_bout.get(bout_id)
        if context is None or context.sport_key != 'mma':
            tier_by_bout[bout_id] = EvidenceTier.C
            continue

        round_payload = _compute_round_level_ps_for_bout(context, pair, round_stats_by_bout.get(bout_id, {}))
        if round_payload is not None:
            round_rows.extend(round_payload['round_rows'])
            fight_rows.extend(round_payload['fight_rows'])
            ps_margin_by_key.update(round_payload['ps_margin'])
            ps_fight_by_key.update(round_payload['ps_fight'])
            tier_by_bout[bout_id] = EvidenceTier.A
            continue

        totals_payload = _compute_totals_level_ps_for_bout(context, pair, fight_totals_by_bout.get(bout_id, {}))
        if totals_payload is not None:
            fight_rows.extend(totals_payload['fight_rows'])
            ps_margin_by_key.update(totals_payload['ps_margin'])
            ps_fight_by_key.update(totals_payload['ps_fight'])
            tier_by_bout[bout_id] = EvidenceTier.B
            continue

        tier_by_bout[bout_id] = EvidenceTier.C

    if round_rows:
        seeder.bulk_insert(conn, db_schema.fact_round_ps_table, round_rows)
    if fight_rows:
        seeder.bulk_insert(conn, db_schema.fact_fight_ps_table, fight_rows)

    return _PsArtifacts(ps_margin_by_key=ps_margin_by_key, ps_fight_by_key=ps_fight_by_key, tier_by_bout=tier_by_bout)


def _compute_round_level_ps_for_bout(
    context: _BoutContext, pair: _BoutPair, round_stats: dict[int, dict[Any, FighterRoundStats]]
) -> dict[str, Any] | None:
    round_results = []
    round_metas = []
    round_rows = []

    for round_num in sorted(round_stats):
        fighter_stats = round_stats[round_num]
        a_stats = fighter_stats.get(pair.fighter_a_id)
        b_stats = fighter_stats.get(pair.fighter_b_id)
        if a_stats is None or b_stats is None:
            continue

        meta = _round_meta(context, pair, round_num)
        result = compute_ps_round(a_stats, b_stats, meta)
        round_results.append(result)
        round_metas.append(meta)
        round_rows.extend(_round_ps_rows(context.bout_id, round_num, pair, result))

    if not round_results:
        return None

    fight = aggregate_fight_ps(round_results, round_metas)
    return {
        'round_rows': round_rows,
        'fight_rows': _fight_ps_rows(
            context.bout_id, pair, fight.ps_fight_a, fight.ps_fight_b, fight.quality_of_win_a, fight.quality_of_win_b
        ),
        'ps_margin': {
            (context.bout_id, pair.fighter_a_id): fight.ps_fight_a - 0.5,
            (context.bout_id, pair.fighter_b_id): fight.ps_fight_b - 0.5,
        },
        'ps_fight': {
            (context.bout_id, pair.fighter_a_id): fight.ps_fight_a,
            (context.bout_id, pair.fighter_b_id): fight.ps_fight_b,
        },
    }


def _compute_totals_level_ps_for_bout(
    context: _BoutContext, pair: _BoutPair, totals: dict[Any, FighterRoundStats]
) -> dict[str, Any] | None:
    a_stats = totals.get(pair.fighter_a_id)
    b_stats = totals.get(pair.fighter_b_id)
    if a_stats is None or b_stats is None:
        return None

    pseudo_meta = _round_meta(context, pair, round_num=1)
    pseudo_round = compute_ps_round(a_stats, b_stats, pseudo_meta)
    fight = aggregate_fight_ps([pseudo_round], [pseudo_meta])

    return {
        'fight_rows': _fight_ps_rows(
            context.bout_id, pair, fight.ps_fight_a, fight.ps_fight_b, fight.quality_of_win_a, fight.quality_of_win_b
        ),
        'ps_margin': {
            (context.bout_id, pair.fighter_a_id): fight.ps_fight_a - 0.5,
            (context.bout_id, pair.fighter_b_id): fight.ps_fight_b - 0.5,
        },
        'ps_fight': {
            (context.bout_id, pair.fighter_a_id): fight.ps_fight_a,
            (context.bout_id, pair.fighter_b_id): fight.ps_fight_b,
        },
    }


def _round_meta(context: _BoutContext, pair: _BoutPair, round_num: int) -> RoundMeta:
    is_finish_round = context.finish_round == round_num and context.finish_time_seconds is not None
    winner_is_a = pair.outcome_for_a == BoutOutcome.WIN if is_finish_round else None
    return RoundMeta(
        round_num=round_num,
        round_seconds=300,
        is_finish_round=is_finish_round,
        finish_elapsed_seconds=context.finish_time_seconds if is_finish_round else None,
        winner_is_a=winner_is_a,
    )


def _round_ps_rows(bout_id: Any, round_num: int, pair: _BoutPair, result: Any) -> list[dict]:
    return [
        {
            'ps_version': PS_VERSION,
            'bout_id': bout_id,
            'round_num': round_num,
            'fighter_id': pair.fighter_a_id,
            'ps_round': result.ps_a,
            'dmg_index': result.dmg_a,
            'dom_index': result.dom_a,
            'dur_index': result.dur_a,
            'ten_nine_p': result.p10_9_a,
            'ten_eight_p': result.p10_8_a,
            'ten_seven_p': result.p10_7_a,
            'debug_json': {},
        },
        {
            'ps_version': PS_VERSION,
            'bout_id': bout_id,
            'round_num': round_num,
            'fighter_id': pair.fighter_b_id,
            'ps_round': result.ps_b,
            'dmg_index': result.dmg_b,
            'dom_index': result.dom_b,
            'dur_index': result.dur_b,
            'ten_nine_p': result.p10_9_b,
            'ten_eight_p': result.p10_8_b,
            'ten_seven_p': result.p10_7_b,
            'debug_json': {},
        },
    ]


def _fight_ps_rows(bout_id: Any, pair: _BoutPair, ps_a: float, ps_b: float, qow_a: float, qow_b: float) -> list[dict]:
    return [
        {
            'ps_version': PS_VERSION,
            'bout_id': bout_id,
            'fighter_id': pair.fighter_a_id,
            'ps_fight': ps_a,
            'quality_of_win': qow_a,
            'debug_json': {},
        },
        {
            'ps_version': PS_VERSION,
            'bout_id': bout_id,
            'fighter_id': pair.fighter_b_id,
            'ps_fight': ps_b,
            'quality_of_win': qow_b,
            'debug_json': {},
        },
    ]


def _seed_rating_artifacts(
    seeder: NormalizedCsvSeeder,
    conn: Connection,
    contexts: list[_BoutContext],
    bout_pairs: dict[Any, _BoutPair],
    ps_artifacts: _PsArtifacts,
    system_ids: dict[str, Any],
    final_context_date: date,
) -> None:
    runtime = _RatingRuntime(
        elo_system=EloPerformanceRatingSystem(),
        glicko_system=Glicko2PerformanceRatingSystem(),
        dynamic_system=DynamicFactorBradleyTerrySystem(),
        stacked_system=StackedLogitMixtureSystem(),
        ewr_system=ExpectedWinRatePoolSystem(),
        elo_states={},
        glicko_states={},
        dynamic_states={},
        mma_fight_counts=defaultdict(int),
        mma_last_date={},
        delta_rows=[],
        snapshot_map={},
        stacked_samples=[],
    )

    for context in _ordered_contexts(contexts, bout_pairs):
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

        if context.sport_key == 'mma':
            _append_stacking_sample(runtime, context, pair, evidence_a)
            _update_elo_system(
                context,
                pair,
                evidence_a,
                runtime.elo_system,
                runtime.elo_states,
                system_ids,
                runtime.delta_rows,
                runtime.snapshot_map,
            )
            _update_glicko_system(
                context,
                pair,
                evidence_a,
                runtime.glicko_system,
                runtime.glicko_states,
                system_ids,
                runtime.delta_rows,
                runtime.snapshot_map,
            )

        _update_dynamic_system(
            context,
            pair,
            evidence_a,
            runtime.dynamic_system,
            runtime.dynamic_states,
            system_ids,
            runtime.delta_rows,
            runtime.snapshot_map,
        )

        if context.sport_key == 'mma':
            runtime.mma_fight_counts[pair.fighter_a_id] += 1
            runtime.mma_fight_counts[pair.fighter_b_id] += 1
            runtime.mma_last_date[pair.fighter_a_id] = context.processing_date
            runtime.mma_last_date[pair.fighter_b_id] = context.processing_date

    if runtime.delta_rows:
        seeder.bulk_insert(conn, db_schema.fact_rating_delta_table, runtime.delta_rows)
    if runtime.snapshot_map:
        seeder.bulk_insert(conn, db_schema.fact_rating_snapshot_table, list(runtime.snapshot_map.values()))

    _seed_stacked_and_ewr_snapshots(seeder, conn, runtime, system_ids, final_context_date)


def _append_stacking_sample(
    runtime: _RatingRuntime, context: _BoutContext, pair: _BoutPair, evidence_a: BoutEvidence
) -> None:
    if pair.outcome_for_a not in {BoutOutcome.WIN, BoutOutcome.LOSS}:
        return

    elo_a = runtime.elo_states.get(pair.fighter_a_id) or new_elo_state(str(pair.fighter_a_id))
    elo_b = runtime.elo_states.get(pair.fighter_b_id) or new_elo_state(str(pair.fighter_b_id))
    glicko_a = runtime.glicko_states.get(pair.fighter_a_id) or new_glicko2_state(str(pair.fighter_a_id))
    glicko_b = runtime.glicko_states.get(pair.fighter_b_id) or new_glicko2_state(str(pair.fighter_b_id))
    dynamic_a = runtime.dynamic_states.get(pair.fighter_a_id) or new_dynamic_factor_state(str(pair.fighter_a_id))
    dynamic_b = runtime.dynamic_states.get(pair.fighter_b_id) or new_dynamic_factor_state(str(pair.fighter_b_id))

    last_date = runtime.mma_last_date.get(pair.fighter_a_id)
    days_since_last = float((context.processing_date - last_date).days) if last_date else 999.0

    runtime.stacked_samples.append(
        StackingSample(
            p_a=runtime.elo_system.expected_win_probability(elo_a.rating, elo_b.rating),
            p_b=runtime.glicko_system.expected_win_probability(glicko_a, glicko_b),
            p_c=runtime.dynamic_system.expected_win_probability(dynamic_a, dynamic_b, evidence_a),
            p_n=None,
            outcome_a_win=1.0 if pair.outcome_for_a == BoutOutcome.WIN else 0.0,
            tier=evidence_a.tier,
            scheduled_rounds=context.scheduled_rounds,
            is_title_fight=context.is_title_fight,
            days_since_last_mma=days_since_last,
            missing_weight_class=not context.has_division,
            missing_promotion=not context.has_promotion,
            opponent_connectivity=float(runtime.mma_fight_counts.get(pair.fighter_b_id, 0)),
        )
    )


def _update_elo_system(
    context: _BoutContext,
    pair: _BoutPair,
    evidence_a: BoutEvidence,
    elo_system: EloPerformanceRatingSystem,
    elo_states: dict[Any, Any],
    system_ids: dict[str, Any],
    delta_rows: list[dict],
    snapshot_map: dict[tuple[Any, Any, date], dict],
) -> None:
    elo_a = elo_states.get(pair.fighter_a_id) or new_elo_state(str(pair.fighter_a_id))
    elo_b = elo_states.get(pair.fighter_b_id) or new_elo_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = elo_system.update_bout(elo_a, elo_b, evidence_a)
    elo_states[pair.fighter_a_id] = post_a
    elo_states[pair.fighter_b_id] = post_b

    system_id = system_ids[_SYSTEM_A_KEY]
    delta_rows.extend(
        [
            _delta_row(system_id, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(system_id, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    _upsert_snapshot(snapshot_map, system_id, pair.fighter_a_id, context.checkpoint_date, post_a.rating, None, None, {})
    _upsert_snapshot(snapshot_map, system_id, pair.fighter_b_id, context.checkpoint_date, post_b.rating, None, None, {})


def _update_glicko_system(
    context: _BoutContext,
    pair: _BoutPair,
    evidence_a: BoutEvidence,
    glicko_system: Glicko2PerformanceRatingSystem,
    glicko_states: dict[Any, Any],
    system_ids: dict[str, Any],
    delta_rows: list[dict],
    snapshot_map: dict[tuple[Any, Any, date], dict],
) -> None:
    glicko_a = glicko_states.get(pair.fighter_a_id) or new_glicko2_state(str(pair.fighter_a_id))
    glicko_b = glicko_states.get(pair.fighter_b_id) or new_glicko2_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = glicko_system.update_bout(glicko_a, glicko_b, evidence_a)
    glicko_states[pair.fighter_a_id] = post_a
    glicko_states[pair.fighter_b_id] = post_b

    system_id = system_ids[_SYSTEM_B_KEY]
    delta_rows.extend(
        [
            _delta_row(system_id, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(system_id, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    _upsert_snapshot(
        snapshot_map,
        system_id,
        pair.fighter_a_id,
        context.checkpoint_date,
        post_a.rating,
        post_a.rd,
        post_a.volatility,
        {},
    )
    _upsert_snapshot(
        snapshot_map,
        system_id,
        pair.fighter_b_id,
        context.checkpoint_date,
        post_b.rating,
        post_b.rd,
        post_b.volatility,
        {},
    )


def _update_dynamic_system(
    context: _BoutContext,
    pair: _BoutPair,
    evidence_a: BoutEvidence,
    dynamic_system: DynamicFactorBradleyTerrySystem,
    dynamic_states: dict[Any, Any],
    system_ids: dict[str, Any],
    delta_rows: list[dict],
    snapshot_map: dict[tuple[Any, Any, date], dict],
) -> None:
    dynamic_a = dynamic_states.get(pair.fighter_a_id) or new_dynamic_factor_state(str(pair.fighter_a_id))
    dynamic_b = dynamic_states.get(pair.fighter_b_id) or new_dynamic_factor_state(str(pair.fighter_b_id))

    post_a, post_b, delta_a, delta_b = dynamic_system.update_bout(dynamic_a, dynamic_b, evidence_a)
    dynamic_states[pair.fighter_a_id] = post_a
    dynamic_states[pair.fighter_b_id] = post_b

    system_id = system_ids[_SYSTEM_C_KEY]
    delta_rows.extend(
        [
            _delta_row(system_id, context.bout_id, pair.fighter_a_id, delta_a),
            _delta_row(system_id, context.bout_id, pair.fighter_b_id, delta_b),
        ]
    )
    _upsert_snapshot(
        snapshot_map,
        system_id,
        pair.fighter_a_id,
        context.checkpoint_date,
        dynamic_system.implied_mma_rating(post_a),
        dynamic_system.mma_uncertainty(post_a),
        None,
        {'striking': float(post_a.mean[1]), 'grappling': float(post_a.mean[2]), 'durability': float(post_a.mean[3])},
    )
    _upsert_snapshot(
        snapshot_map,
        system_id,
        pair.fighter_b_id,
        context.checkpoint_date,
        dynamic_system.implied_mma_rating(post_b),
        dynamic_system.mma_uncertainty(post_b),
        None,
        {'striking': float(post_b.mean[1]), 'grappling': float(post_b.mean[2]), 'durability': float(post_b.mean[3])},
    )


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


def _seed_stacked_and_ewr_snapshots(
    seeder: NormalizedCsvSeeder, conn: Connection, runtime: _RatingRuntime, system_ids: dict[str, Any], as_of_date: date
) -> None:
    if len(runtime.stacked_samples) >= STACK_TRAIN_MIN_SAMPLES:
        runtime.stacked_system.fit(runtime.stacked_samples)

    active_fighters = sorted(
        runtime.mma_fight_counts, key=lambda fighter_id: runtime.mma_fight_counts[fighter_id], reverse=True
    )
    if not active_fighters:
        return

    reference_pool = active_fighters[: min(REFERENCE_POOL_LIMIT, len(active_fighters))]
    uncertainty_map = {
        fighter_id: min(0.49, runtime.dynamic_system.mma_uncertainty(state) / 800.0)
        for fighter_id, state in runtime.dynamic_states.items()
        if fighter_id in active_fighters
    }
    fighter_id_by_str = {str(fighter_id): fighter_id for fighter_id in active_fighters}
    reference_pool_str = [str(item) for item in reference_pool]

    def probability_fn(fighter_a_id: str, fighter_b_id: str) -> float:
        fighter_a = fighter_id_by_str.get(fighter_a_id)
        fighter_b = fighter_id_by_str.get(fighter_b_id)
        if fighter_a is None or fighter_b is None:
            return 0.5

        elo_a = runtime.elo_states.get(fighter_a) or new_elo_state(str(fighter_a))
        elo_b = runtime.elo_states.get(fighter_b) or new_elo_state(str(fighter_b))
        glicko_a = runtime.glicko_states.get(fighter_a) or new_glicko2_state(str(fighter_a))
        glicko_b = runtime.glicko_states.get(fighter_b) or new_glicko2_state(str(fighter_b))
        dynamic_a = runtime.dynamic_states.get(fighter_a) or new_dynamic_factor_state(str(fighter_a))
        dynamic_b = runtime.dynamic_states.get(fighter_b) or new_dynamic_factor_state(str(fighter_b))

        p_a = runtime.elo_system.expected_win_probability(elo_a.rating, elo_b.rating)
        p_b = runtime.glicko_system.expected_win_probability(glicko_a, glicko_b)
        evidence = BoutEvidence(
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
        )
        p_c = runtime.dynamic_system.expected_win_probability(dynamic_a, dynamic_b, evidence)

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
            opponent_connectivity=float(runtime.mma_fight_counts.get(fighter_b, 0)),
        )
        return runtime.stacked_system.predict_probability(sample)

    d_rows = []
    for fighter_id in active_fighters:
        fighter_str = str(fighter_id)
        ewr_value = runtime.ewr_system.compute_ewr(fighter_str, reference_pool_str, probability_fn)
        d_rows.append(
            {
                'system_id': system_ids[_SYSTEM_D_KEY],
                'fighter_id': fighter_id,
                'as_of_date': as_of_date,
                'rating_mean': ewr_value,
                'rating_rd': None,
                'rating_vol': None,
                'extra_json': {'ref_pool_size': len(reference_pool)},
            }
        )

    e_rows = []
    ranked = runtime.ewr_system.rank(
        [str(item) for item in active_fighters],
        reference_pool_str,
        probability_fn,
        {str(key): value for key, value in uncertainty_map.items()},
    )
    for row in ranked:
        fighter_id = fighter_id_by_str.get(row.fighter_id)
        if fighter_id is None:
            continue
        e_rows.append(
            {
                'system_id': system_ids[_SYSTEM_E_KEY],
                'fighter_id': fighter_id,
                'as_of_date': as_of_date,
                'rating_mean': row.adjusted_score,
                'rating_rd': uncertainty_map.get(fighter_id),
                'rating_vol': None,
                'extra_json': {
                    'ewr': row.ewr,
                    'adjusted_score': row.adjusted_score,
                    'ref_pool_size': len(reference_pool),
                },
            }
        )

    seeder.bulk_insert(conn, db_schema.fact_rating_snapshot_table, d_rows)
    seeder.bulk_insert(conn, db_schema.fact_rating_snapshot_table, e_rows)
