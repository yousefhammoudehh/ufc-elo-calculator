from uuid import UUID

from elo_calculator.presentation.models.shared import DataModel


class PromotionCreateRequest(DataModel):
    name: str
    link: str | None = None
    strength: float | None = None


class PromotionUpdateRequest(DataModel):
    name: str | None = None
    link: str | None = None
    strength: float | None = None


class PromotionResponse(DataModel):
    promotion_id: UUID
    name: str
    link: str | None = None
    strength: float | None = None
