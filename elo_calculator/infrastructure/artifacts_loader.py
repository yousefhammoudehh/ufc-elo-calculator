import json
import pathlib
from typing import Any

CONFIG_DIR = pathlib.Path(__file__).resolve().parent.parent / 'configs'


def load_artifacts() -> dict[str, Any] | None:
    """Load adjustment artifacts if present.

    Looks for elo_adjust.json under configs/. Returns None if missing.
    """
    path = CONFIG_DIR / 'elo_adjust.json'
    if not path.exists():
        return None
    try:
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None
