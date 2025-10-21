from dataclasses import dataclass
from uuid import UUID

from elo_calculator.domain.entities.base_entity import BaseEntityBase
from elo_calculator.domain.shared.enumerations import FightOutcome


@dataclass
class PreUfcBout(BaseEntityBase):
    """Pre-UFC bout entity representing a fighter's record before joining the UFC."""

    bout_id: UUID
    fighter_id: str | None = None
    promotion_id: UUID | None = None
    result: FightOutcome | None = None
