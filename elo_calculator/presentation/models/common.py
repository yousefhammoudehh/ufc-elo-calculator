"""Shared pydantic models used across all endpoints."""

from pydantic import BaseModel


class PaginationMeta(BaseModel):
    """Standard pagination metadata included in every list response."""

    total: int
    limit: int
    offset: int
    has_next: bool
    has_previous: bool
