from fastapi import APIRouter, Depends

from elo_calculator.application.services.bout_participant_service import BoutParticipantService
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.bout_participant_models import BoutParticipantResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_not_found, get_ok

router = APIRouter(prefix='/bout-participants', tags=['bout-participants'])


@router.get('/by-bout/{bout_id}', response_model=MainResponse[list[BoutParticipantResponse]])
async def list_by_bout(
    bout_id: str, service: BoutParticipantService = Depends(get_service(BoutParticipantService))
) -> MainResponse[list[BoutParticipantResponse]]:
    participants = await service.list_by_bout(bout_id)
    return get_ok(BoutParticipantResponse.from_entity_list(participants))


@router.get('/by-fighter/{fighter_id}', response_model=MainResponse[list[BoutParticipantResponse]])
async def list_by_fighter(
    fighter_id: str, service: BoutParticipantService = Depends(get_service(BoutParticipantService))
) -> MainResponse[list[BoutParticipantResponse]]:
    participants = await service.list_by_fighter(fighter_id)
    return get_ok(BoutParticipantResponse.from_entity_list(participants))


@router.get('/{bout_id}/{fighter_id}', response_model=MainResponse[BoutParticipantResponse])
async def get_bout_participant(
    bout_id: str, fighter_id: str, service: BoutParticipantService = Depends(get_service(BoutParticipantService))
) -> MainResponse[BoutParticipantResponse]:
    participant = await service.get(bout_id, fighter_id)
    if participant is None:
        return get_not_found(message=f'BoutParticipant bout_id:{bout_id} fighter_id:{fighter_id} not found')
    return get_ok(BoutParticipantResponse.from_entity(participant))


@router.get('/record/{fighter_id}', response_model=MainResponse[dict[str, int]])
async def fighter_record(
    fighter_id: str, service: BoutParticipantService = Depends(get_service(BoutParticipantService))
) -> MainResponse[dict[str, int]]:
    record = await service.record(fighter_id)
    return get_ok(record)
