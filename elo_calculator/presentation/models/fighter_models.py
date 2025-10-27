from elo_calculator.presentation.models.shared import DataModel


class FighterCreateRequest(DataModel):
    fighter_id: str
    name: str
    entry_elo: float | None = None
    current_elo: float | None = None
    peak_elo: float | None = None
    tapology_link: str | None = None
    stats_link: str | None = None


class FighterUpdateRequest(DataModel):
    name: str | None = None
    entry_elo: float | None = None
    current_elo: float | None = None
    peak_elo: float | None = None
    tapology_link: str | None = None
    stats_link: str | None = None


class FighterResponse(DataModel):
    fighter_id: str
    name: str
    entry_elo: float | None = None
    current_elo: float | None = None
    peak_elo: float | None = None
    tapology_link: str | None = None
    stats_link: str | None = None
