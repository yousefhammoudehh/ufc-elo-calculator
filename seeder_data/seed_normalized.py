from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import create_engine

from elo_calculator.infrastructure.database.engine import metadata
from seeder_data.normalized_seed.core import NormalizedCsvSeeder
from seeder_data.normalized_seed.helpers import get_sync_db_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


DATA_DIR = Path(__file__).resolve().parent / 'data'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Seed normalized canonical tables from CSV data exports.')
    parser.add_argument('--data-dir', type=Path, default=DATA_DIR, help='Directory that contains source CSV files.')
    parser.add_argument(
        '--no-truncate', action='store_true', help='Do not truncate existing canonical tables before loading.'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seeder = NormalizedCsvSeeder(data_dir=args.data_dir)
    engine = create_engine(get_sync_db_url(), future=True)

    with engine.begin() as conn:
        metadata.create_all(conn)
        if not args.no_truncate:
            seeder.truncate_existing(conn)
        seeder.run(conn)

    logger.info('Normalized seeding complete from {}', args.data_dir)


if __name__ == '__main__':
    main()
