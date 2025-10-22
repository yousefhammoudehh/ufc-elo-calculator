from uuid import UUID

from elo_calculator.presentation.models.shared import DataModel


class BoutCreateRequest(DataModel):
    bout_id: str
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None


class BoutUpdateRequest(DataModel):
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None


class BoutResponse(DataModel):
    bout_id: str
    event_id: UUID | None = None
    is_title_fight: bool | None = None
    method: str | None = None
    round_num: int | None = None
    time_sec: int | None = None
    time_format: str | None = None
