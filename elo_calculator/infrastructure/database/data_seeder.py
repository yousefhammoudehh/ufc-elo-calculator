import csv
import os
from datetime import date
from typing import Any, cast

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from elo_calculator.configs.log import get_logger
from elo_calculator.infrastructure.database.engine import engine
from elo_calculator.infrastructure.database.schema import events, promotions


def _seeder_data_dir() -> str:
    """Resolve absolute path to the seeder_data directory.

    Priority order:
    1. SEEDER_DATA_DIR env var (explicit override)
    2. /app/seeder_data (default in container via volume mount)
    3. CWD/seeder_data (useful for local runs)
    """
    # 1) Env override
    env_dir = os.getenv('SEEDER_DATA_DIR')
    if env_dir and os.path.isdir(env_dir):
        return env_dir

    # 2) Default container path
    default_container_path = '/app/seeder_data'
    if os.path.isdir(default_container_path):
        return default_container_path

    # 3) Fallback to CWD
    fallback_dir = os.path.join(os.getcwd(), 'seeder_data')
    if os.path.isdir(fallback_dir):
        return fallback_dir
    # None of the expected directories exist; log a warning to aid debugging
    logger.warning(
        'No valid seeder_data directory found. Checked: env=%s, container=%s, fallback=%s',
        env_dir,
        default_container_path,
        fallback_dir,
    )
    return fallback_dir


def _read_csv(path: str) -> list[dict[str, str]]:
    try:
        with open(path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return cast(list[dict[str, str]], list(reader))
    except FileNotFoundError:
        # Gracefully skip if file not present to avoid startup crash, but log a warning
        logger.warning('Seeder CSV not found at %s; skipping.', path)
        return []


def _load_events_csv() -> list[dict[str, Any]]:
    rows = _read_csv(os.path.join(_seeder_data_dir(), 'events.csv'))
    norm: list[dict[str, Any]] = []
    for r in rows:
        try:
            # CSV headers (new format): id,event_date,event_link,events_stats_link,fighters_seeded,fights_seeded
            # Map to DB columns: event_id,event_date,event_link,event_stats_link,fighters_seeded,fights_seeded
            csv_id = r.get('id') or r.get('event_id')
            event_stats_link = r.get('events_stats_link') or r.get('event_stats_link')

            def _to_bool(val: str | None) -> bool | None:
                if val is None or val == '':
                    return None
                v = val.strip().lower()
                if v in {'true', '1', 'yes', 'y'}:
                    return True
                if v in {'false', '0', 'no', 'n'}:
                    return False
                # Unknown token: return None so DB default can apply
                return None

            norm.append(
                {
                    'event_id': csv_id,  # keep given UUIDs as strings
                    'event_link': r.get('event_link'),
                    'event_date': date.fromisoformat(r['event_date']),
                    'event_stats_link': event_stats_link,
                    'fighters_seeded': _to_bool(r.get('fighters_seeded')),
                    'fights_seeded': _to_bool(r.get('fights_seeded')),
                }
            )
        except Exception as e:
            logger.warning('Failed to parse events.csv row: %s. Error: %s', r, e, exc_info=True)
            continue
    return norm


def _load_promotions_csv() -> list[dict[str, Any]]:
    rows = _read_csv(os.path.join(_seeder_data_dir(), 'promotions.csv'))
    norm: list[dict[str, Any]] = []
    for r in rows:
        try:
            # CSV headers: promotions,links,strength
            # Note: CSV header 'promotions' maps to DB column 'name'; other headers: links,strength
            strength_val = r.get('strength')
            norm.append(
                {'name': r['promotions'], 'link': r['links'], 'strength': float(strength_val) if strength_val else None}
            )
        except Exception as e:
            logger.warning('Failed to parse promotions.csv row: %s. Error: %s', r, e, exc_info=True)
            continue
    return norm


async def seed_events(con: AsyncConnection) -> None:
    # Collect existing IDs to avoid duplicates
    result = await con.execute(select(events.c.event_id))
    existing = {str(row.event_id) for row in result.fetchall()}

    payload = [row for row in _load_events_csv() if str(row['event_id']) not in existing]
    if payload:
        await con.execute(insert(events).values(payload))


async def seed_promotions(con: AsyncConnection) -> None:
    # Use name to deduplicate (links may not be unique across time)
    result = await con.execute(select(promotions.c.name))
    existing_names = {row.name for row in result.fetchall()}

    payload = [row for row in _load_promotions_csv() if row['name'] not in existing_names]
    if payload:
        await con.execute(insert(promotions).values(payload))


async def seed_data() -> None:
    # Run all seeders within a single transaction for consistency
    async with engine.begin() as con:
        await seed_promotions(con)
        await seed_events(con)


# Initialize logger last (after functions are defined to avoid circular import surprises)
logger = get_logger()
