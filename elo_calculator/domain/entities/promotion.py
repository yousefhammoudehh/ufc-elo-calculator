from dataclasses import dataclass
from uuid import UUID

from elo_calculator.domain.entities.base_entity import BaseEntityBase


@dataclass
class Promotion(BaseEntityBase):
    """Promotion entity representing a fighting organization (UFC, Bellator, etc.)."""

    promotion_id: UUID
    name: str
    link: str | None = None
    strength: float | None = None
