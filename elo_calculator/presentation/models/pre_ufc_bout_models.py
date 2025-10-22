from uuid import UUID

from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.presentation.models.shared import DataModel


class PreUfcBoutCreateRequest(DataModel):
    fighter_id: str | None = None
    promotion_id: UUID | None = None
    result: FightOutcome | None = None


class PreUfcBoutUpdateRequest(DataModel):
    fighter_id: str | None = None
    promotion_id: UUID | None = None
    result: FightOutcome | None = None


class PreUfcBoutResponse(DataModel):
    bout_id: UUID
    fighter_id: str | None = None
    promotion_id: UUID | None = None
    result: FightOutcome | None = None
