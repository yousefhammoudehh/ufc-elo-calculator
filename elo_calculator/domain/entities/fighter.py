from dataclasses import dataclass

from elo_calculator.domain.entities.base_entity import BaseEntityBase


@dataclass
class Fighter(BaseEntityBase):
    """Fighter entity representing a UFC fighter with ELO ratings."""

    fighter_id: str
    name: str
    entry_elo: int | None = None
    current_elo: int | None = None
    peak_elo: int | None = None
