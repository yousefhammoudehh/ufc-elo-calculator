from uuid import UUID

from fastapi import APIRouter, Depends

from elo_calculator.application.services.event_service import EventService
from elo_calculator.domain.entities import Event
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.event_models import EventCreateRequest, EventResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_ok

router = APIRouter(prefix='/events', tags=['events'])


@router.get('/', response_model=MainResponse[list[EventResponse]])
async def list_events(service: EventService = Depends(get_service(EventService))) -> MainResponse[list[EventResponse]]:
    events = await service.list()
    return get_ok[MainResponse[list[EventResponse]]](EventResponse.from_entity_list(events))


@router.post('/', response_model=MainResponse[EventResponse])
async def create_event(
    request: EventCreateRequest, service: EventService = Depends(get_service(EventService))
) -> MainResponse[EventResponse]:
    entity = Event.from_dict(request.model_dump())
    created = await service.create(entity)
    return get_ok[MainResponse[EventResponse]](EventResponse.from_entity(created))


@router.get('/{event_id}', response_model=MainResponse[EventResponse])
async def get_event(
    event_id: UUID, service: EventService = Depends(get_service(EventService))
) -> MainResponse[EventResponse]:
    event = await service.get_by_event_id(event_id)
    return get_ok[MainResponse[EventResponse]](EventResponse.from_entity(event))
