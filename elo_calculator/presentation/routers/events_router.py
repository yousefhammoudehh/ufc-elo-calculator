"""Events router — ``/api/v1/events``."""

from fastapi import APIRouter, HTTPException, Query

from elo_calculator.application.events import EventService
from elo_calculator.presentation.models.common import PaginationMeta
from elo_calculator.presentation.models.event_models import (
    BoutCard,
    EventDetailResponse,
    EventListResponse,
    EventSummary,
    ParticipantCard,
)

events_router = APIRouter(prefix='/api/v1/events', tags=['events'])
_service = EventService()


@events_router.get('', response_model=EventListResponse)
async def list_events(limit: int = Query(25, ge=1, le=200), offset: int = Query(0, ge=0)) -> EventListResponse:
    """Paginated list of events, most recent first."""
    events, total = await _service.list_events(limit=limit, offset=offset)
    return EventListResponse(
        data=[
            EventSummary(
                event_id=e.event_id,
                event_date=e.event_date,
                event_name=e.event_name,
                promotion_name=e.promotion_name,
                location=e.location,
                num_fights=e.num_fights,
            )
            for e in events
        ],
        pagination=PaginationMeta(
            total=total, limit=limit, offset=offset, has_next=offset + limit < total, has_previous=offset > 0
        ),
    )


@events_router.get('/{event_id}', response_model=EventDetailResponse)
async def get_event(event_id: str) -> EventDetailResponse:
    """Event detail with full fight card."""
    event, bouts = await _service.get_event_detail(event_id=event_id)
    if event is None:
        raise HTTPException(status_code=404, detail='Event not found')
    return EventDetailResponse(
        event_id=event.event_id,
        event_date=event.event_date,
        event_name=event.event_name,
        promotion_name=event.promotion_name,
        location=event.location,
        bouts=[
            BoutCard(
                bout_id=b.bout_id,
                division_key=b.division_key,
                weight_class_raw=b.weight_class_raw,
                is_title_fight=b.is_title_fight,
                method_group=b.method_group,
                decision_type=b.decision_type,
                finish_round=b.finish_round,
                finish_time_seconds=b.finish_time_seconds,
                participants=[
                    ParticipantCard(
                        fighter_id=p.fighter_id, display_name=p.display_name, corner=p.corner, outcome_key=p.outcome_key
                    )
                    for p in b.participants
                ],
            )
            for b in bouts
        ],
    )
