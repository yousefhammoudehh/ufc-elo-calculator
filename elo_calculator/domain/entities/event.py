from dataclasses import dataclass
from datetime import date
from uuid import UUID

from elo_calculator.domain.entities.base_entity import BaseEntityBase


@dataclass
class Event(BaseEntityBase):
    """Event entity representing a UFC event."""

    event_id: UUID
    event_date: date
    event_link: str | None = None
