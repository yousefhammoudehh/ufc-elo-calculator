from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Connection, select

from elo_calculator.domain.shared.enumerations import (
    BoutResultEnum,
    DecisionTypeEnum,
    MethodGroupEnum,
    inverse_bout_result,
    is_finish_method_group,
    normalize_bout_result,
    normalize_ufcstats_method_group,
)
from elo_calculator.infrastructure.database import schema as db_schema
from seeder_data.normalized_seed.helpers import clean_text, parse_datetime_value, parse_int
from seeder_data.normalized_seed.normalizers import derive_ufc_outcomes, normalize_decision_for_outcome

if TYPE_CHECKING:
    from seeder_data.normalized_seed.core import NormalizedCsvSeeder


def seed_result_ontology(seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    rows = []
    outcomes = (
        BoutResultEnum.WIN,
        BoutResultEnum.LOSS,
        BoutResultEnum.DRAW,
        BoutResultEnum.NO_CONTEST,
        BoutResultEnum.UNKNOWN,
    )
    methods = (
        MethodGroupEnum.KO,
        MethodGroupEnum.TKO,
        MethodGroupEnum.SUB,
        MethodGroupEnum.DEC,
        MethodGroupEnum.DQ,
        MethodGroupEnum.OVERTURNED,
        MethodGroupEnum.OTHER,
        MethodGroupEnum.UNKNOWN,
    )
    decision_types = (
        DecisionTypeEnum.UD,
        DecisionTypeEnum.SD,
        DecisionTypeEnum.MD,
        DecisionTypeEnum.TD,
        DecisionTypeEnum.DRAW,
        DecisionTypeEnum.NA,
        DecisionTypeEnum.UNKNOWN,
    )

    for outcome in outcomes:
        for method in methods:
            scoped_decisions = (
                decision_types if method == MethodGroupEnum.DEC else (DecisionTypeEnum.NA, DecisionTypeEnum.UNKNOWN)
            )
            for decision in scoped_decisions:
                rows.append(
                    {
                        'outcome_key': outcome,
                        'method_group': method,
                        'decision_type': decision,
                        'is_finish': is_finish_method_group(method),
                    }
                )

    seeder.bulk_insert(conn, db_schema.dim_result_ontology_table, rows)
    result_rows = conn.execute(
        select(
            db_schema.dim_result_ontology_table.c.result_id,
            db_schema.dim_result_ontology_table.c.outcome_key,
            db_schema.dim_result_ontology_table.c.method_group,
            db_schema.dim_result_ontology_table.c.decision_type,
        )
    ).all()
    for row in result_rows:
        outcome = row.outcome_key.value if isinstance(row.outcome_key, BoutResultEnum) else str(row.outcome_key)
        method = row.method_group.value if isinstance(row.method_group, MethodGroupEnum) else str(row.method_group)
        decision = (
            row.decision_type.value
            if isinstance(row.decision_type, DecisionTypeEnum)
            else str(row.decision_type or DecisionTypeEnum.NA.value)
        )
        seeder.result_id_by_key[(outcome, method, decision)] = row.result_id


def result_id_for(
    seeder: NormalizedCsvSeeder, outcome: BoutResultEnum, method_group: MethodGroupEnum, decision_type: DecisionTypeEnum
) -> object:
    decision = normalize_decision_for_outcome(outcome, method_group, decision_type)
    key = (outcome.value, method_group.value, decision.value)
    fallback = (BoutResultEnum.UNKNOWN.value, MethodGroupEnum.UNKNOWN.value, DecisionTypeEnum.UNKNOWN.value)
    return seeder.result_id_by_key.get(key, seeder.result_id_by_key[fallback])


def seed_participants(seeder: NormalizedCsvSeeder, conn: Connection) -> None:  # noqa: PLR0912, PLR0915
    records_by_key: dict[tuple[object, object], dict] = {}
    ufc_record_by_id: dict[str, dict] = {}

    for row in seeder.frames['ufcstats_fighters.csv'].to_dict('records'):
        source_id = clean_text(row.get('ufcstats_fighter_id'))
        if source_id:
            ufc_record_by_id[source_id] = row

    for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False):
        fight_source_id = clean_text(row.ufcstats_fight_id)
        bout_id = seeder.bout_id_by_ufcstats.get(fight_source_id or '')
        if bout_id is None:
            continue

        red_source_id = clean_text(row.red_fighter_id)
        blue_source_id = clean_text(row.blue_fighter_id)
        winner_source_id = clean_text(row.winner_fighter_id)
        red_fighter_id = seeder.fighter_id_by_ufcstats.get(red_source_id or '')
        blue_fighter_id = seeder.fighter_id_by_ufcstats.get(blue_source_id or '')
        if red_fighter_id is None or blue_fighter_id is None:
            continue

        method_group, decision_type = normalize_ufcstats_method_group(
            clean_text(row.method), clean_text(row.method_detail)
        )
        red_outcome, blue_outcome = derive_ufc_outcomes(
            winner_source_id=winner_source_id,
            red_source_id=red_source_id,
            blue_source_id=blue_source_id,
            method_group=method_group,
        )

        red_key = (bout_id, red_fighter_id)
        blue_key = (bout_id, blue_fighter_id)
        red_prefight = clean_text(ufc_record_by_id.get(red_source_id or '', {}).get('record'))
        blue_prefight = clean_text(ufc_record_by_id.get(blue_source_id or '', {}).get('record'))
        red_row = {
            'bout_id': bout_id,
            'fighter_id': red_fighter_id,
            'corner': 'red',
            'outcome_key': red_outcome,
            'result_id': result_id_for(seeder, red_outcome, method_group, decision_type),
            'opponent_name': seeder.fighter_name_by_ufcstats.get(blue_source_id or ''),
            'opponent_source_slug': blue_source_id,
            'prefight_record': red_prefight,
            'opponent_prefight_record': blue_prefight,
            'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            'source_file': 'ufcstats_fights.csv',
        }
        blue_row = {
            'bout_id': bout_id,
            'fighter_id': blue_fighter_id,
            'corner': 'blue',
            'outcome_key': blue_outcome,
            'result_id': result_id_for(seeder, blue_outcome, method_group, decision_type),
            'opponent_name': seeder.fighter_name_by_ufcstats.get(red_source_id or ''),
            'opponent_source_slug': red_source_id,
            'prefight_record': blue_prefight,
            'opponent_prefight_record': red_prefight,
            'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            'source_file': 'ufcstats_fights.csv',
        }
        records_by_key[red_key] = red_row
        records_by_key[blue_key] = blue_row

    for row in seeder.frames['tapology_bout_fighters.csv'].itertuples(index=False):
        tapology_bout_id = parse_int(clean_text(row.tapology_bout_id))
        if tapology_bout_id is None:
            continue
        bout_id = seeder.bout_id_by_tapology.get(tapology_bout_id)
        if bout_id is None:
            continue
        fighter_source_id = clean_text(row.tapology_fighter_id)
        fighter_id = seeder.fighter_id_by_tapology.get(fighter_source_id or '')
        if fighter_id is None:
            continue

        outcome = normalize_bout_result(clean_text(row.result))
        method_group, decision_type = seeder.bout_method_by_bout_id.get(
            bout_id, (MethodGroupEnum.UNKNOWN, DecisionTypeEnum.UNKNOWN)
        )
        if method_group == MethodGroupEnum.OVERTURNED:
            outcome = BoutResultEnum.NO_CONTEST

        participant_key = (bout_id, fighter_id)
        tapology_row = {
            'bout_id': bout_id,
            'fighter_id': fighter_id,
            'corner': records_by_key.get(participant_key, {}).get('corner', 'unknown'),
            'outcome_key': outcome,
            'result_id': result_id_for(seeder, outcome, method_group, decision_type),
            'opponent_name': clean_text(row.opponent_name),
            'opponent_source_slug': clean_text(row.opponent_tapology_slug),
            'prefight_record': clean_text(row.fighter_record_before_raw),
            'opponent_prefight_record': clean_text(row.opponent_record_before_raw),
            'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            'source_file': clean_text(row.source_file) or 'tapology_bout_fighters.csv',
        }
        if participant_key in records_by_key:
            records_by_key[participant_key].update(
                {
                    'outcome_key': tapology_row['outcome_key'] or records_by_key[participant_key]['outcome_key'],
                    'result_id': tapology_row['result_id'] or records_by_key[participant_key]['result_id'],
                    'prefight_record': tapology_row['prefight_record']
                    or records_by_key[participant_key].get('prefight_record'),
                    'opponent_prefight_record': tapology_row['opponent_prefight_record']
                    or records_by_key[participant_key].get('opponent_prefight_record'),
                    'opponent_name': tapology_row['opponent_name']
                    or records_by_key[participant_key].get('opponent_name'),
                    'opponent_source_slug': tapology_row['opponent_source_slug']
                    or records_by_key[participant_key].get('opponent_source_slug'),
                    'fetched_at': tapology_row['fetched_at'] or records_by_key[participant_key].get('fetched_at'),
                }
            )
        else:
            records_by_key[participant_key] = tapology_row

        opponent_source_id = clean_text(row.opponent_tapology_slug)
        opponent_fighter_id = seeder.fighter_id_by_tapology.get(opponent_source_id or '')
        if opponent_fighter_id is None:
            continue

        opponent_key = (bout_id, opponent_fighter_id)
        if opponent_key in records_by_key:
            continue

        opponent_outcome = inverse_bout_result(outcome)
        inferred_opponent_row = {
            'bout_id': bout_id,
            'fighter_id': opponent_fighter_id,
            'corner': 'unknown',
            'outcome_key': opponent_outcome,
            'result_id': result_id_for(seeder, opponent_outcome, method_group, decision_type),
            'opponent_name': seeder.fighter_name_by_tapology.get(fighter_source_id or ''),
            'opponent_source_slug': fighter_source_id,
            'prefight_record': clean_text(row.opponent_record_before_raw),
            'opponent_prefight_record': clean_text(row.fighter_record_before_raw),
            'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            'source_file': clean_text(row.source_file) or 'tapology_bout_fighters.csv',
        }
        records_by_key[opponent_key] = inferred_opponent_row

    seeder.bulk_insert(conn, db_schema.fact_bout_participant_table, list(records_by_key.values()))
