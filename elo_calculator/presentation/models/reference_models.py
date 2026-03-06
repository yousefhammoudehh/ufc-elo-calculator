"""Pydantic models for reference-data endpoints (divisions, systems)."""

from pydantic import BaseModel


class DivisionResponse(BaseModel):
    division_id: str
    division_key: str
    display_name: str | None = None
    sex: str
    limit_lbs: float | None = None
    is_canonical_mma: bool


class DivisionListResponse(BaseModel):
    data: list[DivisionResponse]


class RatingSystemResponse(BaseModel):
    system_id: str
    system_key: str
    description: str | None = None


class SystemListResponse(BaseModel):
    data: list[RatingSystemResponse]
