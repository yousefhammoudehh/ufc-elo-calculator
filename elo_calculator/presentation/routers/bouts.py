from fastapi import APIRouter, Depends

from elo_calculator.application.services.bout_service import BoutService
from elo_calculator.domain.entities import Bout
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.bout_models import BoutCreateRequest, BoutDetailsResponse, BoutResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_not_found, get_ok

router = APIRouter(prefix='/bouts', tags=['bouts'])


@router.get('/', response_model=MainResponse[list[BoutResponse]])
async def list_bouts(service: BoutService = Depends(get_service(BoutService))) -> MainResponse[list[BoutResponse]]:
    bouts = await service.get_all()
    return get_ok(BoutResponse.from_entity_list(bouts))


@router.get('/{bout_id}', response_model=MainResponse[BoutResponse])
async def get_bout(
    bout_id: str, service: BoutService = Depends(get_service(BoutService))
) -> MainResponse[BoutResponse]:
    bout = await service.get_by_bout_id(bout_id)
    if bout is None:
        return get_not_found(message=f'Bout id:{bout_id} not found')
    return get_ok(BoutResponse.from_entity(bout))


@router.post('/', response_model=MainResponse[BoutResponse])
async def create_bout(
    request: BoutCreateRequest, service: BoutService = Depends(get_service(BoutService))
) -> MainResponse[BoutResponse]:
    entity = Bout.from_dict(request.model_dump())
    created = await service.create(entity)
    return get_ok(BoutResponse.from_entity(created))


@router.get('/{bout_id}/details', response_model=MainResponse[BoutDetailsResponse])
async def get_bout_details(
    bout_id: str, service: BoutService = Depends(get_service(BoutService))
) -> MainResponse[BoutDetailsResponse]:
    details = await service.get_details_by_bout_id(bout_id)
    if details is None:
        return get_not_found(message=f'Bout details not found for id:{bout_id}')
    return get_ok(details)
