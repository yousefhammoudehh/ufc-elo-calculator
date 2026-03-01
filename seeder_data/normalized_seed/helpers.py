from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus, urlparse
from uuid import UUID

from elo_calculator.configs import env
from elo_calculator.domain.shared.enumerations import FighterGenderEnum

MMSS_RE = re.compile(r'^\s*(\d+):(\d{2})\s*$')
ROUND_RE = re.compile(r'R(\d+)', re.IGNORECASE)
TIME_RE = re.compile(r'(\d+):(\d{2})')
PREFIGHT_RECORD_RE = re.compile(r'\d+')
TAPOLOGY_FIGHTER_ID_RE = re.compile(r'/fighters/([^/?#]+)')
UFCSTATS_ID_RE = re.compile(r'/(?:fighter|event|fight)-details/([0-9a-fA-F]{16})(?:[/?#]|$)')
UFCSTATS_HEX_ID_RE = re.compile(r'^[0-9a-fA-F]{16}$')


def get_sync_db_url() -> str:
    user = quote_plus(env.DB_USERNAME)
    password = quote_plus(env.DB_PASSWORD)
    return f'postgresql+psycopg2://{user}:{password}@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}'


def clean_text(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    stripped = str(raw_value).strip()
    return stripped or None


def parse_decimal(raw_value: str | None) -> Decimal | None:
    value = clean_text(raw_value)
    if value is None:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_int(raw_value: str | None) -> int | None:
    value = clean_text(raw_value)
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def parse_date_value(raw_value: str | None) -> date | None:
    value = clean_text(raw_value)
    if value is None:
        return None

    for candidate in (value, re.sub(r'\s+', ' ', value)):
        for fmt in ('%Y-%m-%d', '%Y-%b %d'):
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue
    return None


def parse_datetime_value(raw_value: str | None) -> datetime | None:
    value = clean_text(raw_value)
    if value is None:
        return None
    formatted = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(formatted)
    except ValueError:
        return None


def parse_uuid_value(raw_value: str | None) -> str | None:
    value = clean_text(raw_value)
    if value is None:
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


def normalize_datetime_for_compare(raw_value: datetime | None) -> datetime | None:
    if raw_value is None:
        return None
    if raw_value.tzinfo is None:
        return raw_value.replace(tzinfo=UTC)
    return raw_value.astimezone(UTC)


def inches_to_cm(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return (value * Decimal('2.54')).quantize(Decimal('0.01'))


def parse_mmss_to_seconds(raw_value: str | None) -> int | None:
    value = clean_text(raw_value)
    if value is None:
        return None

    matched = MMSS_RE.match(value)
    if matched is None:
        return None
    minutes = int(matched.group(1))
    seconds = int(matched.group(2))
    return (minutes * 60) + seconds


def parse_round_and_time_from_details(details: str | None) -> tuple[int | None, int | None]:
    details_value = clean_text(details)
    if details_value is None:
        return None, None

    round_match = ROUND_RE.search(details_value)
    time_match = TIME_RE.search(details_value)
    parsed_round = int(round_match.group(1)) if round_match else None
    parsed_time = parse_mmss_to_seconds(time_match.group(0)) if time_match else None
    return parsed_round, parsed_time


def parse_round_value(raw_value: str | None) -> int | None:
    value = clean_text(raw_value)
    if value is None:
        return None
    if value.lower() == 'totals':
        return 0
    return parse_int(value)


def parse_prefight_record_total(raw_value: str | None) -> int | None:
    value = clean_text(raw_value)
    if value is None:
        return None

    lowered = value.lower()
    if lowered in {'n/a', 'na', 'none', '--'}:
        return None

    # Record strings are stored as W-L[-D], with optional notes like "(1 NC)".
    core_record = lowered.split('(', maxsplit=1)[0].strip()
    numbers = [int(token) for token in PREFIGHT_RECORD_RE.findall(core_record)]
    if len(numbers) < 2:  # noqa: PLR2004
        return None
    return sum(numbers)


def parse_tapology_fighter_id_from_url(raw_url: str | None) -> str | None:
    value = clean_text(raw_url)
    if value is None:
        return None
    matched = TAPOLOGY_FIGHTER_ID_RE.search(value)
    if matched is None:
        return None
    return clean_text(matched.group(1))


def parse_tapology_slug_from_url(raw_url: str | None) -> str | None:
    value = clean_text(raw_url)
    if value is None:
        return None
    parsed = urlparse(value)
    path = parsed.path.strip('/')
    if not path:
        return None
    segment = path.split('/')[-1]
    return clean_text(segment)


def normalize_ufcstats_id(raw_id: str | None) -> str | None:
    value = clean_text(raw_id)
    if value is None:
        return None
    if UFCSTATS_HEX_ID_RE.fullmatch(value) is None:
        return None
    return value.lower()


def parse_ufcstats_id_from_url(raw_url: str | None) -> str | None:
    value = clean_text(raw_url)
    if value is None:
        return None
    matched = UFCSTATS_ID_RE.search(value)
    if matched is None:
        return None
    return normalize_ufcstats_id(matched.group(1))


def resolve_ufcstats_id(raw_id: str | None, raw_url: str | None) -> str | None:
    from_url = parse_ufcstats_id_from_url(raw_url)
    if from_url is not None:
        return from_url
    return normalize_ufcstats_id(raw_id)


def display_name_from_slug(slug: str | None) -> str:
    if slug is None:
        return 'Unknown Fighter'
    stripped = re.sub(r'^\d+-', '', slug).strip('-')
    if not stripped:
        return 'Unknown Fighter'
    return stripped.replace('-', ' ').title()


def parse_gender(raw_value: str | None) -> FighterGenderEnum:
    value = clean_text(raw_value)
    if value is None:
        return FighterGenderEnum.UNKNOWN
    uppercase_value = value.upper()
    if uppercase_value == 'M':
        return FighterGenderEnum.MALE
    if uppercase_value == 'F':
        return FighterGenderEnum.FEMALE
    return FighterGenderEnum.UNKNOWN
