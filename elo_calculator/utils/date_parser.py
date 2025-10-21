import re
from datetime import date, datetime, time, timedelta

ISO_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
ISO_DATE_FORMAT = '%Y-%m-%d'


def date_to_iso_str(d: date) -> str:
    return d.strftime(ISO_DATE_FORMAT)


def datetime_to_iso_str(dt: datetime) -> str:
    # Normalize microseconds to 6 digits and append Z for UTC assumption
    return dt.strftime(ISO_DATETIME_FORMAT)


def time_to_str(t: time) -> str:
    return t.strftime('%H:%M:%S')


def timedelta_to_iso_str(td: timedelta) -> str:
    # Represent as ISO 8601 duration (approx, days/hours/minutes/seconds)
    total_seconds = int(td.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = 'P'
    if days:
        parts += f'{days}D'
    if hours or minutes or seconds:
        parts += 'T'
    if hours:
        parts += f'{hours}H'
    if minutes:
        parts += f'{minutes}M'
    if seconds or (not hours and not minutes and not days):
        parts += f'{seconds}S'
    return parts


def str_to_timedelta(val: str) -> timedelta:
    # Accept format HH:MM:SS or ISO duration like PT1H30M
    try:
        if val.startswith('P'):
            # very small parser for PnDTnHnMnS
            days = hours = minutes = seconds = 0
            date_part, time_part = val[1:], ''
            if 'T' in date_part:
                date_part, time_part = date_part.split('T', 1)
            d_match = re.search(r'(\d+)D', date_part)
            if d_match:
                days = int(d_match.group(1))
            h_match = re.search(r'(\d+)H', time_part)
            if h_match:
                hours = int(h_match.group(1))
            m_match = re.search(r'(\d+)M', time_part)
            if m_match:
                minutes = int(m_match.group(1))
            s_match = re.search(r'(\d+)S', time_part)
            if s_match:
                seconds = int(s_match.group(1))
            return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        h, m, s = val.split(':')
        return timedelta(hours=int(h), minutes=int(m), seconds=int(s))
    except Exception:
        raise ValueError(f'Invalid timedelta value: {val}') from None


def parse_date(date_str: str, date_format: str = ISO_DATE_FORMAT) -> datetime | None:
    try:
        return datetime.strptime(date_str, date_format)
    except ValueError:
        return None


def parse_iso_date(date_str: str) -> datetime | None:
    try:
        return datetime.strptime(date_str, ISO_DATETIME_FORMAT)
    except ValueError:
        return None


def iso_str_to_datetime(val: str) -> datetime:
    return datetime.fromisoformat(val.replace('Z', '+00:00')) if val.endswith('Z') else datetime.fromisoformat(val)


def str_to_date(val: str) -> date:
    return date.fromisoformat(val)
