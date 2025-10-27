from uuid import UUID

from elo_calculator.presentation.models.event_models import EventResponse
from elo_calculator.presentation.models.fighter_models import FighterResponse
from elo_calculator.presentation.models.shared import DataModel


class EventIngestionRequest(DataModel):
    event_ids: list[UUID]


class EventIngestionLinksRequest(DataModel):
    event_links: list[str]


class FirstUnseededEventsRequest(DataModel):
    limit: int


class FirstUnseededEventsResponse(DataModel):
    events_seeded: list[EventResponse]
    events_seeded_count: int
    first_event: EventResponse | None = None
    last_event: EventResponse | None = None
    fighters_seeded: list[FighterResponse]
    fighters_seeded_count: int


class EventsIngestionResponse(DataModel):
    events_seeded: list[EventResponse]
    events_seeded_count: int
    fighters_seeded: list[FighterResponse]
    fighters_seeded_count: int
