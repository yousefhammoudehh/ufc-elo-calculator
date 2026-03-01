from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Connection, text

from elo_calculator.domain.shared.enumerations import (
    DecisionTypeEnum,
    FighterGenderEnum,
    MethodGroupEnum,
    WeightUnitEnum,
)
from elo_calculator.infrastructure.database import schema as db_schema
from seeder_data.normalized_seed.helpers import resolve_ufcstats_id
from seeder_data.normalized_seed.step_dimensions import (
    seed_promotions,
    seed_rulesets,
    seed_sports,
    seed_weight_taxonomy,
)
from seeder_data.normalized_seed.step_entities import seed_bouts, seed_events, seed_fighters
from seeder_data.normalized_seed.step_ranking import seed_ranking_artifacts
from seeder_data.normalized_seed.step_results import seed_participants, seed_result_ontology
from seeder_data.normalized_seed.step_stats import seed_round_and_stats, seed_watermark


class NormalizedCsvSeeder:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.frames: dict[str, pd.DataFrame] = {}
        self.sport_id_by_key: dict[str, Any] = {}
        self.promotion_id_by_tapology_slug: dict[str, Any] = {}
        self.promotion_id_by_name: dict[str, Any] = {}
        self.division_id_by_key: dict[tuple[str, str, FighterGenderEnum], Any] = {}
        self.weight_limit_id_by_key: dict[tuple[WeightUnitEnum, Decimal], Any] = {}
        self.ruleset_id_by_key: dict[str, Any] = {}
        self.result_id_by_key: dict[tuple[str, str, str], Any] = {}
        self.fighter_id_by_tapology: dict[str, Any] = {}
        self.fighter_id_by_ufcstats: dict[str, Any] = {}
        self.fighter_name_by_tapology: dict[str, str] = {}
        self.fighter_name_by_ufcstats: dict[str, str] = {}
        self.event_id_by_tapology: dict[str, Any] = {}
        self.event_id_by_ufcstats: dict[str, Any] = {}
        self.bout_id_by_tapology: dict[str, Any] = {}
        self.bout_id_by_ufcstats: dict[str, Any] = {}
        self.bout_method_by_bout_id: dict[Any, tuple[MethodGroupEnum, DecisionTypeEnum]] = {}

    def load(self) -> None:
        file_names = (
            'tapology_fighters.csv',
            'tapology_events.csv',
            'tapology_bouts.csv',
            'tapology_bout_fighters.csv',
            'ufcstats_fighters.csv',
            'ufcstats_events_2025.csv',
            'ufcstats_fights.csv',
            'ufcstats_fight_totals.csv',
            'ufcstats_fight_rounds.csv',
            'ufcstats_sig_strikes_by_target.csv',
            'ufcstats_sig_strikes_by_position.csv',
            'fighter_id_map.csv',
            'bout_id_map.csv',
            'promotions_with_sports.csv',
            'sport_translation.csv',
        )
        for file_name in file_names:
            full_path = self.data_dir / file_name
            self.frames[file_name] = pd.read_csv(full_path, dtype=str, keep_default_na=False, na_filter=False)
        self._normalize_ufcstats_ids()

    def _normalize_ufcstats_ids(self) -> None:
        def apply_with_url(frame_name: str, id_col: str, url_col: str) -> None:
            frame = self.frames[frame_name]
            frame[id_col] = [
                resolve_ufcstats_id(raw_id, raw_url) or ''
                for raw_id, raw_url in zip(frame[id_col], frame[url_col], strict=False)
            ]

        def apply_without_url(frame_name: str, id_col: str) -> None:
            frame = self.frames[frame_name]
            frame[id_col] = [resolve_ufcstats_id(raw_id, None) or '' for raw_id in frame[id_col]]

        apply_with_url('ufcstats_fighters.csv', 'ufcstats_fighter_id', 'ufcstats_url')
        apply_with_url('ufcstats_events_2025.csv', 'event_id', 'url')
        apply_with_url('ufcstats_fights.csv', 'ufcstats_fight_id', 'ufcstats_fight_url')

        apply_without_url('ufcstats_fights.csv', 'ufcstats_event_id')
        apply_without_url('ufcstats_fights.csv', 'red_fighter_id')
        apply_without_url('ufcstats_fights.csv', 'blue_fighter_id')
        apply_without_url('ufcstats_fights.csv', 'winner_fighter_id')

        apply_without_url('ufcstats_fight_totals.csv', 'ufcstats_fight_id')
        apply_without_url('ufcstats_fight_totals.csv', 'ufcstats_fighter_id')
        apply_without_url('ufcstats_fight_rounds.csv', 'ufcstats_fight_id')
        apply_without_url('ufcstats_fight_rounds.csv', 'ufcstats_fighter_id')
        apply_without_url('ufcstats_sig_strikes_by_target.csv', 'ufcstats_fight_id')
        apply_without_url('ufcstats_sig_strikes_by_target.csv', 'ufcstats_fighter_id')
        apply_without_url('ufcstats_sig_strikes_by_position.csv', 'ufcstats_fight_id')
        apply_without_url('ufcstats_sig_strikes_by_position.csv', 'ufcstats_fighter_id')

        apply_without_url('fighter_id_map.csv', 'ufcstats_fighter_id')
        apply_without_url('bout_id_map.csv', 'ufcstats_fight_id')

    def truncate_existing(self, conn: Connection) -> None:
        names = ', '.join(table.name for table in db_schema.ALL_TABLES)
        conn.execute(text(f'TRUNCATE TABLE {names} RESTART IDENTITY CASCADE'))

    def bulk_insert(self, conn: Connection, table: Any, rows: list[dict], chunk_size: int = 5000) -> None:
        if not rows:
            return
        for index in range(0, len(rows), chunk_size):
            batch = rows[index : index + chunk_size]
            keyset = {key for row in batch for key in row}
            normalized_batch = [{key: row.get(key) for key in keyset} for row in batch]
            conn.execute(table.insert(), normalized_batch)

    def resolve_division_id(self, sport_key: str, division_key: str | None, sex: FighterGenderEnum) -> Any:
        if division_key is None:
            return None
        for candidate_sex in (sex, FighterGenderEnum.UNKNOWN):
            found = self.division_id_by_key.get((sport_key, division_key, candidate_sex))
            if found is not None:
                return found
        return None

    def resolve_weight_limit_id(self, unit: WeightUnitEnum | None, value: Decimal | None) -> Any:
        if unit is None or value is None:
            return None
        return self.weight_limit_id_by_key.get((unit, value.quantize(Decimal('0.01'))))

    def run(self, conn: Connection) -> None:
        self.load()
        seed_sports(self, conn)
        seed_promotions(self, conn)
        seed_weight_taxonomy(self, conn)
        seed_rulesets(self, conn)
        seed_fighters(self, conn)
        seed_events(self, conn)
        seed_bouts(self, conn)
        seed_result_ontology(self, conn)
        seed_participants(self, conn)
        seed_round_and_stats(self, conn)
        seed_ranking_artifacts(self, conn)
        seed_watermark(self, conn)
