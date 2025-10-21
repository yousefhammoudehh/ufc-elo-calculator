from datetime import datetime
from typing import Optional


def parse_date(date_str: str, date_format: str = '%Y-%m-%d') -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, date_format)
    except ValueError:
        return None


def parse_iso_date(date_str: str) -> Optional[datetime]:
    try:
        return parse_date(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception:
        return None
