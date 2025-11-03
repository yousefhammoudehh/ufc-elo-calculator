from __future__ import annotations

from pydantic import BaseModel


class CacheFlushResponse(BaseModel):
    status: str


class CacheInvalidateRequest(BaseModel):
    prefixes: list[str]
    batch_size: int | None = 200


class CacheInvalidateResponse(BaseModel):
    deleted_prefixes: list[str]


class EntryEloReseedRequest(BaseModel):
    default_strength: float | None = None
    dry_run: bool | None = False


class EntryEloReseedResponse(BaseModel):
    total_fighters: int
    updated: int
    defaulted_to_1500: int
    dry_run: bool
    params: dict[str, float]
    sample: list[dict[str, object]]
