from datetime import date
from uuid import UUID

from elo_calculator.presentation.models.shared import DataModel


class EventCreateRequest(DataModel):
    event_date: date
    event_link: str | None = None


class EventUpdateRequest(DataModel):
    event_date: date | None = None
    event_link: str | None = None


class EventResponse(DataModel):
    event_id: UUID
    event_date: date
    event_link: str | None = None
