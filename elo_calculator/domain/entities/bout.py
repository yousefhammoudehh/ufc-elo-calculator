from dataclasses import dataclass
from uuid import UUID

from elo_calculator.domain.entities.base_entity import BaseEntityBase


@dataclass
class Bout(BaseEntityBase):
    """Bout entity representing a UFC fight with core metadata."""

    bout_id: str
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    weight_class_code: int | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None
