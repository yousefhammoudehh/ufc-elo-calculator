from __future__ import annotations

# ruff: noqa: PLR2004
from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Connection, func, select

from elo_calculator.infrastructure.database import schema as db_schema
from seeder_data.normalized_seed.helpers import (
    clean_text,
    parse_datetime_value,
    parse_int,
    parse_mmss_to_seconds,
    parse_round_value,
)

if TYPE_CHECKING:
    from seeder_data.normalized_seed.core import NormalizedCsvSeeder


def seed_round_and_stats(seeder: NormalizedCsvSeeder, conn: Connection) -> None:  # noqa: PLR0912, PLR0915
    round_rows: dict[tuple[object, int], dict] = {}
    round_stats_rows: list[dict] = []
    sig_target_rows: list[dict] = []
    sig_position_rows: list[dict] = []
    fight_totals_rows: list[dict] = []

    bout_rows = conn.execute(
        select(
            db_schema.fact_bout_table.c.bout_id,
            db_schema.fact_bout_table.c.scheduled_rounds,
            db_schema.fact_bout_table.c.finish_round,
            db_schema.fact_bout_table.c.finish_time_seconds,
        )
    ).all()
    for row in bout_rows:
        if row.scheduled_rounds is None or int(row.scheduled_rounds) <= 0:
            continue
        scheduled_rounds = int(row.scheduled_rounds)
        finish_round = int(row.finish_round) if row.finish_round is not None else None
        finish_seconds = int(row.finish_time_seconds) if row.finish_time_seconds is not None else None
        for round_num in range(1, scheduled_rounds + 1):
            completed = True
            if finish_round is not None and round_num > finish_round:
                completed = False
            if (
                finish_round is not None
                and round_num == finish_round
                and finish_seconds is not None
                and finish_seconds < 300
            ):
                completed = False
            round_rows[(row.bout_id, round_num)] = {
                'bout_id': row.bout_id,
                'round_num': round_num,
                'round_seconds': 300,
                'completed': completed,
            }

    for row in seeder.frames['ufcstats_fight_rounds.csv'].itertuples(index=False):
        fight_source_id = clean_text(row.ufcstats_fight_id)
        fighter_source_id = clean_text(row.ufcstats_fighter_id)
        round_num = parse_round_value(clean_text(row.round))
        bout_id = seeder.bout_id_by_ufcstats.get(fight_source_id or '')
        fighter_id = seeder.fighter_id_by_ufcstats.get(fighter_source_id or '')
        if bout_id is None or fighter_id is None or round_num is None:
            continue

        round_rows[(bout_id, round_num)] = {
            'bout_id': bout_id,
            'round_num': round_num,
            'round_seconds': 300,
            'completed': round_num != 0,
        }
        round_stats_rows.append(
            {
                'bout_id': bout_id,
                'round_num': round_num,
                'fighter_id': fighter_id,
                'kd': parse_int(clean_text(row.kd)),
                'sig_landed': parse_int(clean_text(row.sig_str_land)),
                'sig_attempted': parse_int(clean_text(row.sig_str_att)),
                'total_landed': parse_int(clean_text(row.total_str_land)),
                'total_attempted': parse_int(clean_text(row.total_str_att)),
                'td_landed': parse_int(clean_text(row.td_land)),
                'td_attempted': parse_int(clean_text(row.td_att)),
                'sig_raw': clean_text(row.sig_str_raw),
                'total_raw': clean_text(row.total_str_raw),
                'td_raw': clean_text(row.td_raw),
                'sub_attempts': parse_int(clean_text(row.sub_att)),
                'rev': parse_int(clean_text(row.rev)),
                'ctrl_seconds': parse_mmss_to_seconds(clean_text(row.ctrl)),
                'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            }
        )

    for row in seeder.frames['ufcstats_sig_strikes_by_target.csv'].itertuples(index=False):
        fight_source_id = clean_text(row.ufcstats_fight_id)
        fighter_source_id = clean_text(row.ufcstats_fighter_id)
        round_num = parse_round_value(clean_text(row.round))
        bout_id = seeder.bout_id_by_ufcstats.get(fight_source_id or '')
        fighter_id = seeder.fighter_id_by_ufcstats.get(fighter_source_id or '')
        if bout_id is None or fighter_id is None or round_num is None:
            continue

        round_rows[(bout_id, round_num)] = {
            'bout_id': bout_id,
            'round_num': round_num,
            'round_seconds': 300 if round_num != 0 else 0,
            'completed': round_num != 0,
        }
        sig_target_rows.append(
            {
                'bout_id': bout_id,
                'round_num': round_num,
                'fighter_id': fighter_id,
                'head_landed': parse_int(clean_text(row.head_land)),
                'head_attempted': parse_int(clean_text(row.head_att)),
                'head_raw': clean_text(row.head_raw),
                'body_landed': parse_int(clean_text(row.body_land)),
                'body_attempted': parse_int(clean_text(row.body_att)),
                'body_raw': clean_text(row.body_raw),
                'leg_landed': parse_int(clean_text(row.leg_land)),
                'leg_attempted': parse_int(clean_text(row.leg_att)),
                'leg_raw': clean_text(row.leg_raw),
                'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            }
        )

    for row in seeder.frames['ufcstats_sig_strikes_by_position.csv'].itertuples(index=False):
        fight_source_id = clean_text(row.ufcstats_fight_id)
        fighter_source_id = clean_text(row.ufcstats_fighter_id)
        round_num = parse_round_value(clean_text(row.round))
        bout_id = seeder.bout_id_by_ufcstats.get(fight_source_id or '')
        fighter_id = seeder.fighter_id_by_ufcstats.get(fighter_source_id or '')
        if bout_id is None or fighter_id is None or round_num is None:
            continue

        round_rows[(bout_id, round_num)] = {
            'bout_id': bout_id,
            'round_num': round_num,
            'round_seconds': 300 if round_num != 0 else 0,
            'completed': round_num != 0,
        }
        sig_position_rows.append(
            {
                'bout_id': bout_id,
                'round_num': round_num,
                'fighter_id': fighter_id,
                'distance_landed': parse_int(clean_text(row.distance_land)),
                'distance_attempted': parse_int(clean_text(row.distance_att)),
                'distance_raw': clean_text(row.distance_raw),
                'clinch_landed': parse_int(clean_text(row.clinch_land)),
                'clinch_attempted': parse_int(clean_text(row.clinch_att)),
                'clinch_raw': clean_text(row.clinch_raw),
                'ground_landed': parse_int(clean_text(row.ground_land)),
                'ground_attempted': parse_int(clean_text(row.ground_att)),
                'ground_raw': clean_text(row.ground_raw),
                'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            }
        )

    for row in seeder.frames['ufcstats_fight_totals.csv'].itertuples(index=False):
        fight_source_id = clean_text(row.ufcstats_fight_id)
        fighter_source_id = clean_text(row.ufcstats_fighter_id)
        bout_id = seeder.bout_id_by_ufcstats.get(fight_source_id or '')
        fighter_id = seeder.fighter_id_by_ufcstats.get(fighter_source_id or '')
        if bout_id is None or fighter_id is None:
            continue
        fight_totals_rows.append(
            {
                'bout_id': bout_id,
                'fighter_id': fighter_id,
                'kd': parse_int(clean_text(row.kd)),
                'sig_landed': parse_int(clean_text(row.sig_str_land)),
                'sig_attempted': parse_int(clean_text(row.sig_str_att)),
                'total_landed': parse_int(clean_text(row.total_str_land)),
                'total_attempted': parse_int(clean_text(row.total_str_att)),
                'td_landed': parse_int(clean_text(row.td_land)),
                'td_attempted': parse_int(clean_text(row.td_att)),
                'sig_raw': clean_text(row.sig_str_raw),
                'total_raw': clean_text(row.total_str_raw),
                'td_raw': clean_text(row.td_raw),
                'sub_attempts': parse_int(clean_text(row.sub_att)),
                'rev': parse_int(clean_text(row.rev)),
                'ctrl_seconds': parse_mmss_to_seconds(clean_text(row.ctrl)),
                'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
            }
        )

    round_rows_list = sorted(round_rows.values(), key=lambda row: (row['bout_id'], row['round_num']))
    seeder.bulk_insert(conn, db_schema.fact_round_table, round_rows_list)
    seeder.bulk_insert(conn, db_schema.fact_round_stats_table, round_stats_rows)
    seeder.bulk_insert(conn, db_schema.fact_round_sig_by_target_table, sig_target_rows)
    seeder.bulk_insert(conn, db_schema.fact_round_sig_by_position_table, sig_position_rows)
    seeder.bulk_insert(conn, db_schema.fact_fight_totals_table, fight_totals_rows)


def seed_watermark(_seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    latest_event_date = conn.execute(select(func.max(db_schema.fact_event_table.c.event_date))).scalar_one()
    latest_bout_id = conn.execute(
        select(db_schema.fact_bout_table.c.bout_id)
        .order_by(db_schema.fact_bout_table.c.created_at.desc(), db_schema.fact_bout_table.c.bout_id.desc())
        .limit(1)
    ).scalar_one_or_none()
    conn.execute(
        db_schema.pipeline_watermark_table.insert(),
        [
            {
                'pipeline_key': 'normalized_csv_seed',
                'last_processed_event_date': latest_event_date or date(1900, 1, 1),
                'last_processed_bout_id': latest_bout_id,
            }
        ],
    )
