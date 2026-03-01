from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import Connection, select

from elo_calculator.domain.shared.enumerations import (
    DecisionTypeEnum,
    FighterGenderEnum,
    MethodGroupEnum,
    SourceSystemEnum,
    normalize_sport_type,
    normalize_tapology_method_group,
    normalize_ufcstats_method_group,
    parse_bool_text,
)
from elo_calculator.infrastructure.database import schema as db_schema
from seeder_data.normalized_seed.helpers import (
    clean_text,
    display_name_from_slug,
    inches_to_cm,
    normalize_datetime_for_compare,
    parse_date_value,
    parse_datetime_value,
    parse_decimal,
    parse_gender,
    parse_int,
    parse_mmss_to_seconds,
    parse_round_and_time_from_details,
    parse_tapology_slug_from_url,
    parse_uuid_value,
)
from seeder_data.normalized_seed.normalizers import infer_ruleset, parse_weight_class

if TYPE_CHECKING:
    from seeder_data.normalized_seed.core import NormalizedCsvSeeder


def seed_fighters(seeder: NormalizedCsvSeeder, conn: Connection) -> None:  # noqa: PLR0912, PLR0915
    all_tapology_profiles: dict[str, dict] = {}
    tapology_profiles: dict[str, dict] = {}
    ufc_profiles: dict[str, dict] = {}
    sex_by_ufc_id: dict[str, FighterGenderEnum] = {}
    eligible_tapology_ids: set[str] = set()
    required_tapology_ids: set[str] = set()
    required_ufcstats_ids: set[str] = set()

    for row in seeder.frames['tapology_bout_fighters.csv'].itertuples(index=False):
        fighter_source_id = clean_text(row.tapology_fighter_id)
        if fighter_source_id:
            eligible_tapology_ids.add(fighter_source_id)

    for row in seeder.frames['tapology_fighters.csv'].itertuples(index=False):
        source_id = clean_text(row.tapology_fighter_id)
        if source_id is None:
            continue
        all_tapology_profiles[source_id] = row._asdict()

    for source_id in sorted(eligible_tapology_ids):
        row_dict = all_tapology_profiles.get(source_id)
        if row_dict is None:
            continue
        tapology_profiles[source_id] = row_dict
        required_tapology_ids.add(source_id)
        seeder.fighter_name_by_tapology[source_id] = clean_text(row_dict.get('name')) or display_name_from_slug(
            source_id
        )

    for row in seeder.frames['ufcstats_fighters.csv'].itertuples(index=False):
        source_id = clean_text(row.ufcstats_fighter_id)
        if source_id is None:
            continue
        ufc_profiles[source_id] = row._asdict()
        seeder.fighter_name_by_ufcstats[source_id] = clean_text(row.name) or display_name_from_slug(source_id)

    for row in seeder.frames['fighter_id_map.csv'].itertuples(index=False):
        tapology_id = clean_text(row.tapology_fighter_id)
        ufcstats_id = clean_text(row.ufcstats_fighter_id)
        if tapology_id and tapology_id in required_tapology_ids and ufcstats_id:
            required_ufcstats_ids.add(ufcstats_id)

    for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False):
        red_id = clean_text(row.red_fighter_id)
        blue_id = clean_text(row.blue_fighter_id)
        gender = parse_gender(clean_text(row.gender))

        if red_id and red_id in required_ufcstats_ids and gender != FighterGenderEnum.UNKNOWN:
            sex_by_ufc_id[red_id] = gender
        if blue_id and blue_id in required_ufcstats_ids and gender != FighterGenderEnum.UNKNOWN:
            sex_by_ufc_id[blue_id] = gender

    tapology_to_key: dict[str, str] = {}
    ufcstats_to_key: dict[str, str] = {}
    canonical_links: dict[str, dict[str, str | None]] = {}

    for source_id in sorted(required_tapology_ids):
        key = f'tap:{source_id}'
        tapology_to_key[source_id] = key
        canonical_links[key] = {'tapology_id': source_id, 'ufcstats_id': None}

    for row in seeder.frames['fighter_id_map.csv'].itertuples(index=False):
        tapology_id = clean_text(row.tapology_fighter_id)
        ufcstats_id = clean_text(row.ufcstats_fighter_id)
        if tapology_id is None or tapology_id not in required_tapology_ids or ufcstats_id is None:
            continue

        if tapology_id in tapology_to_key:
            key = tapology_to_key[tapology_id]
        else:
            continue

        ufcstats_to_key[ufcstats_id] = key
        canonical_links[key]['ufcstats_id'] = ufcstats_id

    fighter_rows: list[dict] = []
    ordered_keys = sorted(canonical_links)
    for key in ordered_keys:
        tapology_id = canonical_links[key]['tapology_id']
        ufcstats_id = canonical_links[key]['ufcstats_id']
        tapology_row = tapology_profiles.get(tapology_id) if tapology_id else None
        ufcstats_row = ufc_profiles.get(ufcstats_id) if ufcstats_id else None

        display_name = (
            (clean_text(tapology_row.get('name')) if tapology_row else None)
            or (clean_text(ufcstats_row.get('name')) if ufcstats_row else None)
            or display_name_from_slug(tapology_id or ufcstats_id)
        )

        tapology_height_in = parse_decimal(clean_text(tapology_row.get('height_in')) if tapology_row else None)
        ufcstats_height_in = parse_decimal(clean_text(ufcstats_row.get('height_in')) if ufcstats_row else None)
        height_in = tapology_height_in or ufcstats_height_in

        tapology_reach_in = parse_decimal(clean_text(tapology_row.get('reach_in')) if tapology_row else None)
        ufcstats_reach_in = parse_decimal(clean_text(ufcstats_row.get('reach_in')) if ufcstats_row else None)
        reach_in = tapology_reach_in or ufcstats_reach_in

        tapology_fetched_at = parse_datetime_value(clean_text(tapology_row.get('fetched_at')) if tapology_row else None)
        ufcstats_fetched_at = parse_datetime_value(clean_text(ufcstats_row.get('fetched_at')) if ufcstats_row else None)
        fetched_at = tapology_fetched_at or ufcstats_fetched_at
        tapology_fetched_at_cmp = normalize_datetime_for_compare(tapology_fetched_at)
        ufcstats_fetched_at_cmp = normalize_datetime_for_compare(ufcstats_fetched_at)
        if tapology_fetched_at_cmp and ufcstats_fetched_at_cmp and tapology_fetched_at_cmp < ufcstats_fetched_at_cmp:
            fetched_at = ufcstats_fetched_at

        sex = sex_by_ufc_id.get(ufcstats_id or '', FighterGenderEnum.UNKNOWN)

        fighter_rows.append(
            {
                'display_name': display_name,
                'nickname': clean_text(tapology_row.get('nickname')) if tapology_row else None,
                'dob_raw': clean_text(tapology_row.get('dob_raw')) if tapology_row else None,
                'birth_date': parse_date_value(
                    (clean_text(tapology_row.get('dob')) if tapology_row else None)
                    or (clean_text(ufcstats_row.get('dob')) if ufcstats_row else None)
                ),
                'birth_place': clean_text(tapology_row.get('birth_place')) if tapology_row else None,
                'country_code': (clean_text(tapology_row.get('country')) if tapology_row else None) or 'UNK',
                'fighting_out_of': clean_text(tapology_row.get('fighting_out_of')) if tapology_row else None,
                'affiliation_gym': clean_text(tapology_row.get('affiliation_gym')) if tapology_row else None,
                'foundation_style': clean_text(tapology_row.get('foundation_style')) if tapology_row else None,
                'profile_image_url': clean_text(tapology_row.get('profile_image_url')) if tapology_row else None,
                'record_raw': clean_text(ufcstats_row.get('record')) if ufcstats_row else None,
                'tapology_slug': tapology_id,
                'height_in': height_in,
                'height_cm': inches_to_cm(height_in),
                'reach_in': reach_in,
                'reach_cm': inches_to_cm(reach_in),
                'stance': (
                    (clean_text(tapology_row.get('stance')) if tapology_row else None)
                    or (clean_text(ufcstats_row.get('stance')) if ufcstats_row else None)
                ),
                'sex': sex,
                'source_fetched_at': fetched_at,
                'source_file': clean_text(tapology_row.get('source_file')) if tapology_row else None,
                'source_meta_json': {
                    'has_tapology_profile': tapology_row is not None,
                    'has_ufcstats_profile': ufcstats_row is not None,
                },
            }
        )

    fighter_id_by_key: dict[str, object] = {}
    for index in range(0, len(ordered_keys), 2000):
        key_chunk = ordered_keys[index : index + 2000]
        row_chunk = fighter_rows[index : index + 2000]
        result = conn.execute(
            db_schema.dim_fighter_table.insert().returning(db_schema.dim_fighter_table.c.fighter_id), row_chunk
        )
        inserted_ids = list(result.scalars().all())
        for current_key, fighter_id in zip(key_chunk, inserted_ids, strict=True):
            fighter_id_by_key[current_key] = fighter_id

    bridge_rows_by_key: dict[tuple[str, str], dict] = {}
    for source_id, key in tapology_to_key.items():
        fighter_id = fighter_id_by_key[key]
        tapology_row = tapology_profiles.get(source_id)
        bridge_rows_by_key[(SourceSystemEnum.TAPOLOGY.value, source_id)] = {
            'fighter_id': fighter_id,
            'source_key': SourceSystemEnum.TAPOLOGY,
            'source_fighter_id': source_id,
            'source_slug': source_id,
            'fetched_at': parse_datetime_value(clean_text(tapology_row.get('fetched_at')) if tapology_row else None),
            'source_file': clean_text(tapology_row.get('source_file')) if tapology_row else None,
            'source_metadata_json': {'from_profile_table': tapology_row is not None},
        }
        seeder.fighter_id_by_tapology[source_id] = fighter_id

    for source_id, key in ufcstats_to_key.items():
        fighter_id = fighter_id_by_key[key]
        ufcstats_row = ufc_profiles.get(source_id)
        bridge_rows_by_key[(SourceSystemEnum.UFCSTATS.value, source_id)] = {
            'fighter_id': fighter_id,
            'source_key': SourceSystemEnum.UFCSTATS,
            'source_fighter_id': source_id,
            'source_slug': source_id,
            'fetched_at': parse_datetime_value(clean_text(ufcstats_row.get('fetched_at')) if ufcstats_row else None),
            'source_file': None,
            'source_metadata_json': {'from_profile_table': ufcstats_row is not None},
        }
        seeder.fighter_id_by_ufcstats[source_id] = fighter_id

    seeder.bulk_insert(conn, db_schema.bridge_fighter_source_table, list(bridge_rows_by_key.values()))


def build_ufc_to_tapology_event_map(
    seeder: NormalizedCsvSeeder, allowed_ufc_event_ids: set[str]
) -> dict[str, dict[str, Any]]:
    bout_to_tapology_event_slug: dict[str, str] = {}
    for row in seeder.frames['tapology_bouts.csv'].itertuples(index=False):
        tapology_bout_ref = clean_text(row.tapology_bout_id)
        tapology_event_slug = clean_text(getattr(row, 'event_tapology_slug', None)) or parse_tapology_slug_from_url(
            clean_text(row.event_tapology_url)
        )
        if tapology_bout_ref is not None and tapology_event_slug is not None:
            bout_to_tapology_event_slug[tapology_bout_ref] = tapology_event_slug

    fight_to_ufc_event: dict[str, str] = {}
    for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False):
        ufcstats_fight_id = clean_text(row.ufcstats_fight_id)
        ufcstats_event_id = clean_text(row.ufcstats_event_id)
        if ufcstats_fight_id and ufcstats_event_id:
            fight_to_ufc_event[ufcstats_fight_id] = ufcstats_event_id

    pair_counts: dict[tuple[str, str], int] = {}
    total_counts_by_ufc_event: dict[str, int] = {}
    for row in seeder.frames['bout_id_map.csv'].itertuples(index=False):
        ufcstats_fight_id = clean_text(row.ufcstats_fight_id)
        tapology_bout_ref = clean_text(row.tapology_bout_id)
        if ufcstats_fight_id is None or tapology_bout_ref is None:
            continue
        ufcstats_event_id = fight_to_ufc_event.get(ufcstats_fight_id)
        tapology_event_slug = bout_to_tapology_event_slug.get(tapology_bout_ref)
        if ufcstats_event_id is None or tapology_event_slug is None or ufcstats_event_id not in allowed_ufc_event_ids:
            continue
        pair_key = (ufcstats_event_id, tapology_event_slug)
        pair_counts[pair_key] = pair_counts.get(pair_key, 0) + 1
        total_counts_by_ufc_event[ufcstats_event_id] = total_counts_by_ufc_event.get(ufcstats_event_id, 0) + 1

    ranked_pairs = sorted(
        (
            (count, ufcstats_event_id, tapology_event_slug)
            for (ufcstats_event_id, tapology_event_slug), count in pair_counts.items()
        ),
        key=lambda item: (-item[0], item[1], item[2]),
    )

    mapping: dict[str, dict[str, Any]] = {}
    used_ufc_events: set[str] = set()
    used_tapology_events: set[str] = set()
    for mapped_count, ufcstats_event_id, tapology_event_slug in ranked_pairs:
        if ufcstats_event_id in used_ufc_events or tapology_event_slug in used_tapology_events:
            continue
        total = total_counts_by_ufc_event.get(ufcstats_event_id, mapped_count)
        confidence = Decimal(mapped_count) / Decimal(total)
        mapping[ufcstats_event_id] = {
            'tapology_event_slug': tapology_event_slug,
            'mapped_bout_count': mapped_count,
            'confidence': confidence.quantize(Decimal('0.0001')),
        }
        used_ufc_events.add(ufcstats_event_id)
        used_tapology_events.add(tapology_event_slug)
    return mapping


def seed_events(seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    ufcstats_events: dict[str, dict] = {}
    tapology_events: dict[str, dict] = {}

    analysis_cutoff_date = date(2025, 12, 31)

    def merge_record(target: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
        for field_name, value in candidate.items():
            if value is None:
                continue
            if target.get(field_name) is None:
                target[field_name] = value
        return target

    for row in seeder.frames['ufcstats_events_2025.csv'].itertuples(index=False):
        ufcstats_event_id = clean_text(row.event_id)
        if ufcstats_event_id is None:
            continue

        event_date = parse_date_value(clean_text(row.date)) or date(1900, 1, 1)
        if event_date > analysis_cutoff_date:
            continue

        candidate = {
            'event_date': event_date,
            'event_name': clean_text(row.name) or f'UFCStats Event {ufcstats_event_id}',
            'ufcstats_event_id': ufcstats_event_id,
            'ufcstats_event_url': clean_text(row.url),
            'ufcstats_event_uuid': parse_uuid_value(clean_text(row.uuid)),
            'num_fights': parse_int(clean_text(row.num_fights)),
            'location': clean_text(row.location),
            'fetched_at': None,
            'source_file': 'ufcstats_events_2025.csv',
        }
        ufcstats_events[ufcstats_event_id] = merge_record(ufcstats_events.get(ufcstats_event_id, {}), candidate)

    def register_tapology_event(row_data: dict[str, Any], source_file: str) -> None:
        tapology_event_slug = clean_text(row_data.get('event_tapology_slug')) or parse_tapology_slug_from_url(
            clean_text(row_data.get('event_tapology_url'))
        )
        if tapology_event_slug is None:
            tapology_event_slug = clean_text(row_data.get('event_tapology_id'))
        if tapology_event_slug is None:
            return
        candidate = {
            'tapology_event_slug': tapology_event_slug,
            'event_date': parse_date_value(clean_text(row_data.get('event_date')))
            or parse_date_value(clean_text(row_data.get('event_date_raw')))
            or date(1900, 1, 1),
            'event_date_raw': clean_text(row_data.get('event_date_raw')),
            'event_name': clean_text(row_data.get('event_name')) or f'Tapology Event {tapology_event_slug}',
            'tapology_event_url': clean_text(row_data.get('event_tapology_url')),
            'promotion_tapology_slug': parse_tapology_slug_from_url(clean_text(row_data.get('promotion_tapology_url'))),
            'promotion_name': clean_text(row_data.get('promotion_name')),
            'source_file': source_file,
        }
        tapology_events[tapology_event_slug] = merge_record(tapology_events.get(tapology_event_slug, {}), candidate)

    for row in seeder.frames['tapology_events.csv'].to_dict('records'):
        register_tapology_event(row, 'tapology_events.csv')
    for row in seeder.frames['tapology_bouts.csv'].to_dict('records'):
        register_tapology_event(row, 'tapology_bouts.csv')

    seeder.bulk_insert(conn, db_schema.fact_event_ufcstats_table, list(ufcstats_events.values()))
    seeder.bulk_insert(conn, db_schema.fact_event_tapology_table, list(tapology_events.values()))

    ufcstats_source_rows = conn.execute(
        select(
            db_schema.fact_event_ufcstats_table.c.ufcstats_event_fact_id,
            db_schema.fact_event_ufcstats_table.c.ufcstats_event_id,
        )
    ).all()
    ufcstats_fact_id_by_source: dict[str, object] = {}
    for row in ufcstats_source_rows:
        ufcstats_fact_id_by_source[str(row.ufcstats_event_id)] = row.ufcstats_event_fact_id

    tapology_source_rows = conn.execute(
        select(
            db_schema.fact_event_tapology_table.c.tapology_event_fact_id,
            db_schema.fact_event_tapology_table.c.tapology_event_slug,
        )
    ).all()
    tapology_fact_id_by_source: dict[str, object] = {}
    for row in tapology_source_rows:
        tapology_fact_id_by_source[str(row.tapology_event_slug)] = row.tapology_event_fact_id

    checkpoint_rows = [
        {
            'event_date': event['event_date'],
            'event_name': event['event_name'],
            'ufcstats_event_id': event['ufcstats_event_id'],
            'ufcstats_event_url': event.get('ufcstats_event_url'),
            'ufcstats_event_uuid': event.get('ufcstats_event_uuid'),
            'num_fights': event.get('num_fights'),
            'location': event.get('location'),
            'fetched_at': event.get('fetched_at'),
            'source_file': event.get('source_file'),
        }
        for event in ufcstats_events.values()
    ]
    seeder.bulk_insert(conn, db_schema.fact_event_table, checkpoint_rows)

    event_results = conn.execute(
        select(db_schema.fact_event_table.c.event_id, db_schema.fact_event_table.c.ufcstats_event_id)
    ).all()
    for row in event_results:
        if row.ufcstats_event_id is not None:
            seeder.event_id_by_ufcstats[str(row.ufcstats_event_id)] = row.event_id

    event_bridge_map = build_ufc_to_tapology_event_map(seeder, set(ufcstats_events))
    bridge_rows = []
    for ufcstats_event_id, mapping in event_bridge_map.items():
        checkpoint_event_id = seeder.event_id_by_ufcstats.get(ufcstats_event_id)
        if checkpoint_event_id is None:
            continue
        tapology_event_slug = str(mapping['tapology_event_slug'])
        seeder.event_id_by_tapology[tapology_event_slug] = checkpoint_event_id
        bridge_rows.append(
            {
                'ufcstats_event_id': ufcstats_event_id,
                'tapology_event_slug': tapology_event_slug,
                'ufcstats_event_fact_id': ufcstats_fact_id_by_source.get(ufcstats_event_id),
                'tapology_event_fact_id': tapology_fact_id_by_source.get(tapology_event_slug),
                'checkpoint_event_id': checkpoint_event_id,
                'mapped_bout_count': int(mapping['mapped_bout_count']),
                'confidence': mapping['confidence'],
                'source_file': 'bout_id_map.csv',
            }
        )
    seeder.bulk_insert(conn, db_schema.bridge_event_source_table, bridge_rows)


def build_ufc_checkpoint_schedule(conn: Connection) -> list[tuple[date, object]]:
    rows = conn.execute(
        select(db_schema.fact_event_table.c.event_id, db_schema.fact_event_table.c.event_date)
        .where(db_schema.fact_event_table.c.ufcstats_event_id.is_not(None))
        .order_by(db_schema.fact_event_table.c.event_date, db_schema.fact_event_table.c.event_id)
    ).all()
    return [(row.event_date, row.event_id) for row in rows]


def resolve_next_checkpoint_event_id(
    checkpoint_schedule: list[tuple[date, object]], bout_date: date | None
) -> object | None:
    if not checkpoint_schedule:
        return None
    if bout_date is None:
        return checkpoint_schedule[-1][1]
    for checkpoint_date, checkpoint_event_id in checkpoint_schedule:
        if bout_date <= checkpoint_date:
            return checkpoint_event_id
    return checkpoint_schedule[-1][1]


def seed_bouts(seeder: NormalizedCsvSeeder, conn: Connection) -> None:  # noqa: PLR0912, PLR0915
    records_by_key: dict[str, dict] = {}
    tapology_bout_by_ufcstats: dict[str, str] = {}
    checkpoint_schedule = build_ufc_checkpoint_schedule(conn)

    for row in seeder.frames['bout_id_map.csv'].itertuples(index=False):
        ufcstats_fight_id = clean_text(row.ufcstats_fight_id)
        tapology_bout_ref = clean_text(row.tapology_bout_id)
        if ufcstats_fight_id and tapology_bout_ref is not None:
            tapology_bout_by_ufcstats[ufcstats_fight_id] = tapology_bout_ref

    ruleset_round_seconds: dict[object, int] = {}
    ruleset_rows = conn.execute(
        select(db_schema.dim_ruleset_table.c.ruleset_id, db_schema.dim_ruleset_table.c.round_seconds)
    ).all()
    for row in ruleset_rows:
        ruleset_round_seconds[row.ruleset_id] = int(row.round_seconds)

    for row in seeder.frames['tapology_bouts.csv'].itertuples(index=False):
        tapology_bout_ref = clean_text(row.tapology_bout_id)
        if tapology_bout_ref is None:
            continue
        key = f'tap:{tapology_bout_ref}'

        event_date = parse_date_value(clean_text(row.event_date)) or parse_date_value(clean_text(row.event_date_raw))
        event_id = resolve_next_checkpoint_event_id(checkpoint_schedule, event_date)
        if event_id is None:
            continue

        sport_key = normalize_sport_type(clean_text(row.sport)).value
        is_title_fight = parse_bool_text(clean_text(row.is_title_fight)) or False
        is_amateur = parse_bool_text(clean_text(row.is_amateur)) or False
        scheduled_rounds = 5 if sport_key == 'mma' and is_title_fight else None
        ruleset = infer_ruleset(sport_key, scheduled_rounds, is_title_fight)
        ruleset_id = seeder.ruleset_id_by_key.get(ruleset.key)

        parsed_weight = parse_weight_class(
            raw_weight_class=clean_text(row.weight_class),
            raw_gender=None,
            raw_weight_lbs=clean_text(row.weight_lbs),
            sport_key=sport_key,
        )
        division_id = seeder.resolve_division_id(parsed_weight.sport_key, parsed_weight.division_key, parsed_weight.sex)
        mma_division_id = (
            seeder.resolve_division_id('mma', parsed_weight.mma_division_key, parsed_weight.sex)
            if parsed_weight.mma_division_key
            else None
        )
        weight_limit_id = seeder.resolve_weight_limit_id(
            parsed_weight.weight_limit_unit, parsed_weight.weight_limit_value
        )
        catch_delta_lbs = None
        if (
            parsed_weight.is_catchweight
            and parsed_weight.weight_limit_lbs is not None
            and parsed_weight.limit_lbs is not None
        ):
            catch_delta_lbs = (parsed_weight.weight_limit_lbs - parsed_weight.limit_lbs).quantize(Decimal('0.01'))
        method_group, decision_type = normalize_tapology_method_group(
            clean_text(row.method), clean_text(row.method_details)
        )
        finish_round, finish_time_seconds = parse_round_and_time_from_details(clean_text(row.method_details))
        total_seconds = None
        if finish_round is not None and finish_time_seconds is not None:
            round_seconds = ruleset_round_seconds.get(ruleset_id, 300)
            total_seconds = ((finish_round - 1) * round_seconds) + finish_time_seconds

        records_by_key[key] = {
            'event_id': event_id,
            'sport_id': seeder.sport_id_by_key.get(sport_key),
            'ruleset_id': ruleset_id,
            'division_id': division_id,
            'mma_division_id': mma_division_id,
            'weight_limit_id': weight_limit_id,
            'weight_class_raw': clean_text(row.weight_class),
            'weight_lbs': parse_decimal(clean_text(row.weight_lbs)) or parsed_weight.weight_limit_lbs,
            'catch_weight_lbs': parsed_weight.weight_limit_lbs if parsed_weight.is_catchweight else None,
            'catch_delta_lbs': catch_delta_lbs,
            'is_catchweight': parsed_weight.is_catchweight,
            'is_openweight': parsed_weight.is_openweight,
            'is_amateur': is_amateur,
            'is_title_fight': is_title_fight,
            'scheduled_rounds': scheduled_rounds,
            'tapology_bout_ref': tapology_bout_ref,
            'ufcstats_fight_id': None,
            'winner_fighter_id': None,
            'referee': None,
            'tapology_method_raw': clean_text(row.method),
            'tapology_method_details_raw': clean_text(row.method_details),
            'ufcstats_method_raw': None,
            'method_group': method_group,
            'decision_type': decision_type,
            'finish_round': finish_round,
            'finish_time_seconds': finish_time_seconds,
            'finish_time_total_seconds': total_seconds,
            'fetched_at': None,
            'source_file': 'tapology_bouts.csv',
            'source_meta_json': {
                'tapology_event_name': clean_text(row.event_name),
                'tapology_event_date': event_date.isoformat() if event_date is not None else None,
                'tapology_event_slug': clean_text(getattr(row, 'event_tapology_slug', None))
                or parse_tapology_slug_from_url(clean_text(row.event_tapology_url)),
                'promotion_name': clean_text(row.promotion_name),
                'promotion_tapology_slug': parse_tapology_slug_from_url(clean_text(row.promotion_tapology_url)),
            },
        }

    for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False):
        ufcstats_fight_id = clean_text(row.ufcstats_fight_id)
        if ufcstats_fight_id is None:
            continue

        mapped_tapology_bout_ref = tapology_bout_by_ufcstats.get(ufcstats_fight_id)
        key = (
            f'tap:{mapped_tapology_bout_ref}'
            if mapped_tapology_bout_ref and f'tap:{mapped_tapology_bout_ref}' in records_by_key
            else None
        )
        if key is None:
            key = f'ufc:{ufcstats_fight_id}'

        record = records_by_key.get(key, {})
        event_id = seeder.event_id_by_ufcstats.get(clean_text(row.ufcstats_event_id) or '')
        if event_id is None:
            continue

        scheduled_rounds = parse_int(clean_text(row.scheduled_rounds))
        is_title_fight = parse_bool_text(clean_text(row.is_title_bout)) or False
        ruleset = infer_ruleset('mma', scheduled_rounds, is_title_fight)
        ruleset_id = seeder.ruleset_id_by_key.get(ruleset.key)
        parsed_weight = parse_weight_class(
            raw_weight_class=clean_text(row.weight_class),
            raw_gender=clean_text(row.gender),
            raw_weight_lbs=None,
            sport_key='mma',
        )
        division_id = seeder.resolve_division_id(parsed_weight.sport_key, parsed_weight.division_key, parsed_weight.sex)
        mma_division_id = (
            seeder.resolve_division_id('mma', parsed_weight.mma_division_key, parsed_weight.sex)
            if parsed_weight.mma_division_key
            else None
        )
        weight_limit_id = seeder.resolve_weight_limit_id(
            parsed_weight.weight_limit_unit, parsed_weight.weight_limit_value
        )
        catch_delta_lbs = None
        if (
            parsed_weight.is_catchweight
            and parsed_weight.weight_limit_lbs is not None
            and parsed_weight.limit_lbs is not None
        ):
            catch_delta_lbs = (parsed_weight.weight_limit_lbs - parsed_weight.limit_lbs).quantize(Decimal('0.01'))
        method_group, decision_type = normalize_ufcstats_method_group(
            clean_text(row.method), clean_text(row.method_detail)
        )
        finish_round = parse_int(clean_text(row.round))
        finish_time_seconds = parse_mmss_to_seconds(clean_text(row.time))
        total_seconds = None
        if finish_round is not None and finish_time_seconds is not None:
            round_seconds = ruleset_round_seconds.get(ruleset_id, 300)
            total_seconds = ((finish_round - 1) * round_seconds) + finish_time_seconds

        record.update(
            {
                'event_id': event_id,
                'sport_id': seeder.sport_id_by_key['mma'],
                'ruleset_id': ruleset_id,
                'division_id': division_id or record.get('division_id'),
                'mma_division_id': mma_division_id or record.get('mma_division_id'),
                'weight_limit_id': weight_limit_id or record.get('weight_limit_id'),
                'weight_class_raw': clean_text(row.weight_class) or record.get('weight_class_raw'),
                'weight_lbs': parsed_weight.weight_limit_lbs or record.get('weight_lbs'),
                'catch_weight_lbs': parsed_weight.weight_limit_lbs
                if parsed_weight.is_catchweight
                else record.get('catch_weight_lbs'),
                'catch_delta_lbs': catch_delta_lbs if catch_delta_lbs is not None else record.get('catch_delta_lbs'),
                'is_catchweight': parsed_weight.is_catchweight or record.get('is_catchweight', False),
                'is_openweight': parsed_weight.is_openweight or record.get('is_openweight', False),
                'is_title_fight': is_title_fight,
                'scheduled_rounds': scheduled_rounds,
                'tapology_bout_ref': mapped_tapology_bout_ref or record.get('tapology_bout_ref'),
                'ufcstats_fight_id': ufcstats_fight_id,
                'winner_fighter_id': seeder.fighter_id_by_ufcstats.get(clean_text(row.winner_fighter_id) or ''),
                'referee': clean_text(row.referee),
                'ufcstats_method_raw': clean_text(row.method),
                'method_group': method_group if method_group != MethodGroupEnum.UNKNOWN else record.get('method_group'),
                'decision_type': (
                    decision_type
                    if decision_type != DecisionTypeEnum.UNKNOWN
                    else record.get('decision_type', DecisionTypeEnum.NA)
                ),
                'finish_round': finish_round if finish_round is not None else record.get('finish_round'),
                'finish_time_seconds': finish_time_seconds
                if finish_time_seconds is not None
                else record.get('finish_time_seconds'),
                'finish_time_total_seconds': total_seconds
                if total_seconds is not None
                else record.get('finish_time_total_seconds'),
                'fetched_at': parse_datetime_value(clean_text(row.fetched_at)),
                'source_file': record.get('source_file') or 'ufcstats_fights.csv',
            }
        )
        if 'source_meta_json' not in record:
            record['source_meta_json'] = {}
        record['source_meta_json']['ufcstats_fight_url'] = clean_text(row.ufcstats_fight_url)
        records_by_key[key] = record

    rows_to_insert = []
    sport_key_by_id = {sport_id: sport_key for sport_key, sport_id in seeder.sport_id_by_key.items()}
    for _key, record in records_by_key.items():
        if record.get('event_id') is None:
            continue
        if record.get('sport_id') is None:
            record['sport_id'] = seeder.sport_id_by_key['mma']
        sport_key = sport_key_by_id.get(record['sport_id'], 'mma')
        if record.get('division_id') is None:
            record['division_id'] = seeder.resolve_division_id(
                sport_key, f'{sport_key.upper()}_UNKNOWN', FighterGenderEnum.UNKNOWN
            )
        if sport_key == 'mma' and record.get('mma_division_id') is None:
            record['mma_division_id'] = seeder.resolve_division_id('mma', 'MMA_UNKNOWN', FighterGenderEnum.UNKNOWN)
        if record.get('weight_lbs') is None and record.get('catch_weight_lbs') is not None:
            record['weight_lbs'] = record['catch_weight_lbs']
        rows_to_insert.append(record)

    expected_ufc_fight_ids = {
        clean_text(row.ufcstats_fight_id)
        for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False)
        if clean_text(row.ufcstats_fight_id) is not None
        and clean_text(row.ufcstats_event_id) in seeder.event_id_by_ufcstats
    }
    mapped_ufc_fight_ids = {
        str(record['ufcstats_fight_id'])
        for record in rows_to_insert
        if record.get('ufcstats_fight_id') is not None and record.get('tapology_bout_ref') is not None
    }
    missing_bout_crosswalk = sorted(expected_ufc_fight_ids - mapped_ufc_fight_ids)
    if missing_bout_crosswalk:
        sample = ', '.join(missing_bout_crosswalk[:10])
        raise ValueError(
            f'Missing tapology bout crosswalk for {len(missing_bout_crosswalk)} UFCStats fights. Sample: {sample}'
        )

    seeder.bulk_insert(conn, db_schema.fact_bout_table, rows_to_insert)

    bout_rows = conn.execute(
        select(
            db_schema.fact_bout_table.c.bout_id,
            db_schema.fact_bout_table.c.tapology_bout_ref,
            db_schema.fact_bout_table.c.ufcstats_fight_id,
            db_schema.fact_bout_table.c.method_group,
            db_schema.fact_bout_table.c.decision_type,
        )
    ).all()

    for row in bout_rows:
        if row.tapology_bout_ref is not None:
            seeder.bout_id_by_tapology[str(row.tapology_bout_ref)] = row.bout_id
        if row.ufcstats_fight_id is not None:
            seeder.bout_id_by_ufcstats[str(row.ufcstats_fight_id)] = row.bout_id
        method_group = row.method_group if isinstance(row.method_group, MethodGroupEnum) else MethodGroupEnum.UNKNOWN
        decision_type = (
            row.decision_type if isinstance(row.decision_type, DecisionTypeEnum) else DecisionTypeEnum.UNKNOWN
        )
        seeder.bout_method_by_bout_id[row.bout_id] = (method_group, decision_type)
