"""Event domain entity — maps to ``fact_event``."""

from dataclasses import dataclass

from elo_calculator.domain.entities.base_entity import BaseEntity


@dataclass(slots=True)
class Event(BaseEntity):
    """Canonical event (card)."""

    event_id: str = ''
    event_date: str = ''
    event_name: str = ''
    promotion_id: str | None = None
    promotion_name: str | None = None
    location: str | None = None
    num_fights: int | None = None
