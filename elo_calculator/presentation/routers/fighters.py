from fastapi import APIRouter, Depends

from elo_calculator.application.services.fighter_service import FighterService
from elo_calculator.domain.entities import Fighter
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.fighter_models import FighterCreateRequest, FighterResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_not_found, get_ok

router = APIRouter(prefix='/fighters', tags=['fighters'])


@router.get('/', response_model=MainResponse[list[FighterResponse]])
async def list_fighters(
    service: FighterService = Depends(get_service(FighterService)),
) -> MainResponse[list[FighterResponse]]:
    fighters = await service.get_all()
    return get_ok(FighterResponse.from_entity_list(fighters))


@router.get('/{fighter_id}', response_model=MainResponse[FighterResponse])
async def get_fighter(
    fighter_id: str, service: FighterService = Depends(get_service(FighterService))
) -> MainResponse[FighterResponse]:
    fighter = await service.get_by_fighter_id(fighter_id)
    if fighter is None:
        return get_not_found(message=f'Fighter id:{fighter_id} not found')
    return get_ok(FighterResponse.from_entity(fighter))


@router.post('/', response_model=MainResponse[FighterResponse])
async def create_fighter(
    request: FighterCreateRequest, service: FighterService = Depends(get_service(FighterService))
) -> MainResponse[FighterResponse]:
    entity = Fighter.from_dict(request.model_dump())
    created = await service.create(entity)
    return get_ok(FighterResponse.from_entity(created))
