"""Reference-data domain entities: divisions and rating systems."""

from dataclasses import dataclass

from elo_calculator.domain.entities.base_entity import BaseEntity


@dataclass(slots=True)
class Division(BaseEntity):
    """MMA weight-class division."""

    division_id: str = ''
    division_key: str = ''
    display_name: str | None = None
    sex: str = 'U'
    limit_lbs: float | None = None
    is_canonical_mma: bool = False


@dataclass(slots=True)
class RatingSystem(BaseEntity):
    """One of the six rating systems."""

    system_id: str = ''
    system_key: str = ''
    description: str | None = None
