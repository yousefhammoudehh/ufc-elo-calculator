from elo_calculator.presentation.models.shared import DataModel


class FighterCreateRequest(DataModel):
    fighter_id: str
    name: str
    entry_elo: int | None = None
    current_elo: int | None = None
    peak_elo: int | None = None


class FighterUpdateRequest(DataModel):
    name: str | None = None
    entry_elo: int | None = None
    current_elo: int | None = None
    peak_elo: int | None = None


class FighterResponse(DataModel):
    fighter_id: str
    name: str
    entry_elo: int | None = None
    current_elo: int | None = None
    peak_elo: int | None = None
