from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Connection, select

from elo_calculator.domain.shared.enumerations import (
    FighterGenderEnum,
    WeightUnitEnum,
    normalize_sport_type,
    parse_bool_text,
)
from elo_calculator.infrastructure.database import schema as db_schema
from seeder_data.normalized_seed.constants import MMA_DIVISION_BOUNDS_LBS, MMA_DIVISION_LIMITS_LBS
from seeder_data.normalized_seed.helpers import (
    clean_text,
    parse_decimal,
    parse_int,
    parse_tapology_slug_from_url,
    parse_uuid_value,
)
from seeder_data.normalized_seed.normalizers import infer_ruleset, parse_weight_class

if TYPE_CHECKING:
    from seeder_data.normalized_seed.core import NormalizedCsvSeeder


def seed_sports(seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    translation_rows = seeder.frames['sport_translation.csv']
    translation_by_sport: dict[str, Decimal] = {}
    for row in translation_rows.itertuples(index=False):
        sport_key = normalize_sport_type(clean_text(row.sport)).value
        translation = parse_decimal(clean_text(row.translation_to_mma))
        if translation is not None:
            translation_by_sport[sport_key] = translation

    sports: set[str] = {'mma'}
    for row in seeder.frames['tapology_bouts.csv'].itertuples(index=False):
        sports.add(normalize_sport_type(clean_text(row.sport)).value)
    for row in seeder.frames['promotions_with_sports.csv'].itertuples(index=False):
        sports.add(normalize_sport_type(clean_text(row.sport_type)).value)

    rows_to_insert = []
    for sport_key in sorted(sports):
        rows_to_insert.append(
            {
                'sport_key': sport_key,
                'is_mma': sport_key == 'mma',
                'translation_to_mma': translation_by_sport.get(sport_key),
            }
        )
    seeder.bulk_insert(conn, db_schema.dim_sport_table, rows_to_insert)

    results = conn.execute(select(db_schema.dim_sport_table.c.sport_id, db_schema.dim_sport_table.c.sport_key)).all()
    seeder.sport_id_by_key = {row.sport_key: row.sport_id for row in results}


def seed_promotions(seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    promotions: dict[str, dict] = {}

    def resolve_key(raw_tapology_slug: str | None, raw_name: str | None) -> str | None:
        if raw_tapology_slug is not None:
            return f'slug:{raw_tapology_slug}'
        name = clean_text(raw_name)
        if name is None:
            return None
        return f'name:{name.lower()}'

    def merge_record(target: dict, candidate: dict) -> dict:
        for field_name, value in candidate.items():
            if value is None:
                continue
            current = target.get(field_name)
            if current is None or field_name in {'strength'}:
                if field_name in {'strength'} and current is not None:
                    target[field_name] = max(current, value)
                else:
                    target[field_name] = value
        return target

    def promotion_uuid_from_source(source_uuid: str | None, key: str) -> str:
        # Stable fallback keeps promotion IDs deterministic even when legacy IDs are malformed.
        return source_uuid or str(uuid5(NAMESPACE_URL, f'promotion:{key}'))

    allowed_tapology_slugs: set[str] = set()

    for row in seeder.frames['promotions_with_sports.csv'].itertuples(index=False):
        tapology_slug = clean_text(row.tapology_slug)
        tapology_id = parse_int(clean_text(row.tapology_promotion_id))
        key = resolve_key(tapology_slug, clean_text(row.name))
        if key is None:
            continue

        sport_key = normalize_sport_type(clean_text(row.sport_type)).value
        strength = parse_decimal(clean_text(row.baseline_strength))
        sport_id = seeder.sport_id_by_key.get(sport_key)
        if strength is None or sport_id is None:
            continue

        if tapology_slug is not None:
            allowed_tapology_slugs.add(tapology_slug)

        source_uuid = parse_uuid_value(clean_text(row.id))
        candidate = {
            'promotion_id': promotion_uuid_from_source(source_uuid, key),
            'promotion_name': clean_text(row.name) or 'Unknown Promotion',
            'tapology_promotion_id': tapology_id,
            'tapology_promotion_url': (
                f'https://www.tapology.com/fightcenter/promotions/{clean_text(row.tapology_slug)}'
                if clean_text(row.tapology_slug)
                else None
            ),
            'tapology_slug': tapology_slug,
            'strength': strength,
            'sport_id': sport_id,
        }
        promotions[key] = merge_record(promotions.get(key, {}), candidate)

    for file_name in ('tapology_events.csv', 'tapology_bouts.csv'):
        frame = seeder.frames[file_name]
        for row in frame.itertuples(index=False):
            promotion_slug = parse_tapology_slug_from_url(clean_text(row.promotion_tapology_url))
            if promotion_slug is None or promotion_slug not in allowed_tapology_slugs:
                continue
            key = resolve_key(promotion_slug, clean_text(row.promotion_name))
            if key is None:
                continue
            candidate = {
                'promotion_name': clean_text(row.promotion_name) or 'Unknown Promotion',
                'tapology_promotion_id': parse_int(clean_text(row.promotion_tapology_id)),
                'tapology_promotion_url': clean_text(row.promotion_tapology_url),
                'tapology_slug': promotion_slug,
            }
            promotions[key] = merge_record(promotions.get(key, {}), candidate)

    seeder.bulk_insert(conn, db_schema.dim_promotion_table, list(promotions.values()))

    results = conn.execute(
        select(
            db_schema.dim_promotion_table.c.promotion_id,
            db_schema.dim_promotion_table.c.tapology_slug,
            db_schema.dim_promotion_table.c.promotion_name,
        )
    ).all()

    for row in results:
        if row.tapology_slug:
            seeder.promotion_id_by_tapology_slug[str(row.tapology_slug)] = row.promotion_id
        if row.promotion_name:
            seeder.promotion_id_by_name[row.promotion_name.strip().lower()] = row.promotion_id


def seed_weight_taxonomy(seeder: NormalizedCsvSeeder, conn: Connection) -> None:  # noqa: PLR0915
    division_rows_by_key: dict[tuple[str, str, FighterGenderEnum], dict] = {}
    weight_limit_rows_by_key: dict[tuple[WeightUnitEnum, Decimal], dict] = {}
    bridge_stage_by_key: dict[tuple[str, str, object | None, str], dict] = {}
    mma_display_by_code = {
        'ATOM': 'Atomweight',
        'STRAW': 'Strawweight',
        'FLY': 'Flyweight',
        'BW': 'Bantamweight',
        'FW': 'Featherweight',
        'LW': 'Lightweight',
        'WW': 'Welterweight',
        'MW': 'Middleweight',
        'LHW': 'Light Heavyweight',
        'HW': 'Heavyweight',
        'SHW': 'Super Heavyweight',
    }

    def add_division_row(sport_key: str, division_key: str, sex: FighterGenderEnum, fields: dict[str, object]) -> None:
        key = (sport_key, division_key, sex)
        candidate = {
            'sport_id': seeder.sport_id_by_key.get(sport_key),
            'division_key': division_key,
            'sex': sex,
            **fields,
        }
        existing = division_rows_by_key.get(key)
        if existing is None:
            division_rows_by_key[key] = candidate
            return
        for field_name, value in candidate.items():
            if value is not None and existing.get(field_name) is None:
                existing[field_name] = value

    def add_weight_limit_row(unit: WeightUnitEnum, value: Decimal, weight_lbs: Decimal) -> None:
        normalized_value = value.quantize(Decimal('0.01'))
        normalized_lbs = weight_lbs.quantize(Decimal('0.01'))
        key = (unit, normalized_value)
        weight_limit_rows_by_key.setdefault(
            key, {'unit': unit, 'value': normalized_value, 'weight_lbs': normalized_lbs}
        )

    def register_raw_weight_class(
        source_key: str,
        sport_key: str,
        raw_weight_class: str | None,
        raw_gender: str | None,
        raw_weight_lbs: str | None,
        promotion_id: object | None,
    ) -> None:
        parsed = parse_weight_class(
            raw_weight_class=raw_weight_class, raw_gender=raw_gender, raw_weight_lbs=raw_weight_lbs, sport_key=sport_key
        )

        add_division_row(
            sport_key=parsed.sport_key,
            division_key=parsed.division_key,
            sex=parsed.sex,
            fields={
                'display_name': parsed.display_name,
                'limit_lbs': parsed.limit_lbs,
                'lbs_min': parsed.lbs_min,
                'lbs_max': parsed.lbs_max,
                'is_openweight': parsed.is_openweight,
                'is_canonical_mma': parsed.is_canonical_mma,
            },
        )

        if parsed.weight_limit_unit and parsed.weight_limit_value is not None and parsed.weight_limit_lbs is not None:
            add_weight_limit_row(parsed.weight_limit_unit, parsed.weight_limit_value, parsed.weight_limit_lbs)

        stage_key = (source_key, parsed.sport_key, promotion_id, parsed.raw_weight_class)
        candidate = {
            'source_key': source_key,
            'raw_weight_class': parsed.raw_weight_class,
            'sport_key': parsed.sport_key,
            'promotion_id': promotion_id,
            'division_key': parsed.division_key,
            'sex': parsed.sex,
            'weight_limit_unit': parsed.weight_limit_unit,
            'weight_limit_value': parsed.weight_limit_value,
            'is_catchweight': parsed.is_catchweight,
            'is_openweight': parsed.is_openweight,
            'parse_confidence': parsed.parse_confidence,
            'notes': parsed.notes,
        }
        existing = bridge_stage_by_key.get(stage_key)
        if existing is None or parsed.parse_confidence >= existing['parse_confidence']:
            bridge_stage_by_key[stage_key] = candidate

    for sport_key in sorted(seeder.sport_id_by_key):
        add_division_row(
            sport_key=sport_key,
            division_key=f'{sport_key.upper()}_UNKNOWN',
            sex=FighterGenderEnum.UNKNOWN,
            fields={
                'display_name': 'Unknown',
                'limit_lbs': None,
                'lbs_min': None,
                'lbs_max': None,
                'is_openweight': False,
                'is_canonical_mma': False,
            },
        )

    add_division_row(
        sport_key='mma',
        division_key='MMA_OPEN',
        sex=FighterGenderEnum.UNKNOWN,
        fields={
            'display_name': 'Open Weight',
            'limit_lbs': None,
            'lbs_min': None,
            'lbs_max': None,
            'is_openweight': True,
            'is_canonical_mma': True,
        },
    )

    add_division_row(
        sport_key='mma',
        division_key='MMA_UNKNOWN',
        sex=FighterGenderEnum.UNKNOWN,
        fields={
            'display_name': 'Unknown',
            'limit_lbs': None,
            'lbs_min': None,
            'lbs_max': None,
            'is_openweight': False,
            'is_canonical_mma': False,
        },
    )

    for mma_code, bounds in MMA_DIVISION_BOUNDS_LBS.items():
        add_division_row(
            sport_key='mma',
            division_key=f'MMA_{mma_code}',
            sex=FighterGenderEnum.UNKNOWN,
            fields={
                'display_name': mma_display_by_code.get(mma_code, mma_code),
                'limit_lbs': MMA_DIVISION_LIMITS_LBS.get(mma_code),
                'lbs_min': bounds[0],
                'lbs_max': bounds[1],
                'is_openweight': False,
                'is_canonical_mma': True,
            },
        )

    for row in seeder.frames['tapology_bouts.csv'].itertuples(index=False):
        sport_key = normalize_sport_type(clean_text(row.sport)).value
        promotion_slug = parse_tapology_slug_from_url(clean_text(row.promotion_tapology_url))
        promotion_id = seeder.promotion_id_by_tapology_slug.get(promotion_slug or '')
        register_raw_weight_class(
            source_key='tapology',
            sport_key=sport_key,
            raw_weight_class=clean_text(row.weight_class),
            raw_gender=None,
            raw_weight_lbs=clean_text(row.weight_lbs),
            promotion_id=promotion_id,
        )

    for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False):
        register_raw_weight_class(
            source_key='ufcstats',
            sport_key='mma',
            raw_weight_class=clean_text(row.weight_class),
            raw_gender=clean_text(row.gender),
            raw_weight_lbs=None,
            promotion_id=None,
        )

    seeder.bulk_insert(conn, db_schema.dim_division_table, list(division_rows_by_key.values()))
    seeder.bulk_insert(conn, db_schema.dim_weight_limit_table, list(weight_limit_rows_by_key.values()))

    division_results = conn.execute(
        select(
            db_schema.dim_division_table.c.division_id,
            db_schema.dim_division_table.c.sport_id,
            db_schema.dim_division_table.c.division_key,
            db_schema.dim_division_table.c.sex,
        )
    ).all()
    sport_key_by_id = {sport_id: sport_key for sport_key, sport_id in seeder.sport_id_by_key.items()}
    seeder.division_id_by_key = {}
    for row in division_results:
        sport_key = sport_key_by_id.get(row.sport_id)
        if sport_key is None:
            continue
        sex = row.sex if isinstance(row.sex, FighterGenderEnum) else FighterGenderEnum(str(row.sex))
        seeder.division_id_by_key[(sport_key, row.division_key, sex)] = row.division_id

    weight_limit_results = conn.execute(
        select(
            db_schema.dim_weight_limit_table.c.weight_limit_id,
            db_schema.dim_weight_limit_table.c.unit,
            db_schema.dim_weight_limit_table.c.value,
        )
    ).all()
    seeder.weight_limit_id_by_key = {}
    for row in weight_limit_results:
        unit = row.unit if isinstance(row.unit, WeightUnitEnum) else WeightUnitEnum(str(row.unit))
        seeder.weight_limit_id_by_key[(unit, row.value.quantize(Decimal('0.01')))] = row.weight_limit_id

    bridge_rows: list[dict] = []
    for staged in bridge_stage_by_key.values():
        division_id = seeder.resolve_division_id(staged['sport_key'], staged['division_key'], staged['sex'])
        weight_limit_id = seeder.resolve_weight_limit_id(staged['weight_limit_unit'], staged['weight_limit_value'])
        sport_id = seeder.sport_id_by_key.get(staged['sport_key'])
        if sport_id is None:
            continue
        bridge_rows.append(
            {
                'source_key': staged['source_key'],
                'raw_weight_class': staged['raw_weight_class'],
                'sport_id': sport_id,
                'promotion_id': staged['promotion_id'],
                'division_id': division_id,
                'weight_limit_id': weight_limit_id,
                'is_catchweight': staged['is_catchweight'],
                'is_openweight': staged['is_openweight'],
                'parse_confidence': staged['parse_confidence'],
                'notes': staged['notes'],
            }
        )
    seeder.bulk_insert(conn, db_schema.bridge_weight_class_source_table, bridge_rows)


def seed_rulesets(seeder: NormalizedCsvSeeder, conn: Connection) -> None:
    rulesets = {}

    for row in seeder.frames['ufcstats_fights.csv'].itertuples(index=False):
        scheduled_rounds = parse_int(clean_text(row.scheduled_rounds))
        is_title_fight = parse_bool_text(clean_text(row.is_title_bout)) or False
        normalized = infer_ruleset('mma', scheduled_rounds, is_title_fight)
        rulesets[normalized.key] = normalized

    for row in seeder.frames['tapology_bouts.csv'].itertuples(index=False):
        sport_key = normalize_sport_type(clean_text(row.sport)).value
        is_title_fight = parse_bool_text(clean_text(row.is_title_fight)) or False
        normalized = infer_ruleset(sport_key, None, is_title_fight)
        rulesets[normalized.key] = normalized

    rows_to_insert = []
    for key in sorted(rulesets):
        row = rulesets[key]
        rows_to_insert.append(
            {
                'ruleset_key': row.key,
                'sport_id': seeder.sport_id_by_key.get(row.sport_key),
                'rounds_scheduled': row.rounds_scheduled,
                'round_seconds': row.round_seconds,
                'judging_standard': row.judging_standard,
            }
        )
    seeder.bulk_insert(conn, db_schema.dim_ruleset_table, rows_to_insert)

    results = conn.execute(
        select(db_schema.dim_ruleset_table.c.ruleset_id, db_schema.dim_ruleset_table.c.ruleset_key)
    ).all()
    seeder.ruleset_id_by_key = {row.ruleset_key: row.ruleset_id for row in results}
