import hashlib
from typing import Any

import orjson


def convert_to_int(value: Any, default_value: int = 0) -> int:
    try:
        return int(value)
    except ValueError:
        return default_value


def hash_str(data: str) -> str | None:
    if not data:
        return None
    return hashlib.md5(data.encode()).hexdigest()  # noqa: S324


def hash_dict(data: dict[str, Any] | None) -> str | None:
    if not data:
        return None

    hashed_data = {k: v if isinstance(v, str) else str(v) for k, v in data.items()}

    return hashlib.md5(orjson.dumps(hashed_data, option=orjson.OPT_SORT_KEYS)).hexdigest()  # noqa: S324
