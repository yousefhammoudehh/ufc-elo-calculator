from dataclasses import dataclass

from elo_calculator.domain.entities.base_entity import BaseEntityBase


@dataclass
class JudgeScore(BaseEntityBase):
    """Judge score entity representing judges' scorecards for a fighter in a bout."""

    bout_id: str
    fighter_id: str
    judge1_score: int | None = None
    judge2_score: int | None = None
    judge3_score: int | None = None
