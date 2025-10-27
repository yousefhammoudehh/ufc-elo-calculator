from uuid import UUID

from fastapi import APIRouter, Depends, Query

from elo_calculator.application.services.event_service import EventService
from elo_calculator.domain.entities import Event
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.event_models import EventCreateRequest, EventResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_not_found, get_ok

router = APIRouter(prefix='/events', tags=['events'])


@router.get('/', response_model=MainResponse[list[EventResponse]])
async def list_events(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=1000),
    service: EventService = Depends(get_service(EventService)),
) -> MainResponse[list[EventResponse]]:
    events = await service.get_all(page=page, limit=limit)
    return get_ok(EventResponse.from_entity_list(events))


@router.post('/', response_model=MainResponse[EventResponse])
async def create_event(
    request: EventCreateRequest, service: EventService = Depends(get_service(EventService))
) -> MainResponse[EventResponse]:
    entity = Event.from_dict(request.model_dump())
    created = await service.create(entity)
    return get_ok(EventResponse.from_entity(created))


@router.get('/by-link', response_model=MainResponse[EventResponse])
async def get_event_by_link(
    event_link: str = Query(..., description='Canonical event link URL'),
    service: EventService = Depends(get_service(EventService)),
) -> MainResponse[EventResponse]:
    event = await service.get_by_link(event_link)
    if event is None:
        return get_not_found(message=f'Event link not found: {event_link}')
    return get_ok(EventResponse.from_entity(event))


@router.get('/{event_id}', response_model=MainResponse[EventResponse])
async def get_event(
    event_id: UUID, service: EventService = Depends(get_service(EventService))
) -> MainResponse[EventResponse]:
    event = await service.get_by_event_id(event_id)
    if event is None:
        return get_not_found(message=f'Event id:{event_id} not found')
    return get_ok(EventResponse.from_entity(event))
