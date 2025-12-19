from uuid import UUID

from fastapi import APIRouter, Depends

from elo_calculator.application.pre_ufc_bout_service import PreUfcBoutService
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.pre_ufc_bout_models import PreUfcBoutResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_ok

pre_ufc_bouts_router = APIRouter(prefix='/pre-ufc-bouts', tags=['Pre-UFC Bouts'])


@pre_ufc_bouts_router.get('/by-fighter/{fighter_id}', response_model=MainResponse[list[PreUfcBoutResponse]])
async def list_by_fighter(
    fighter_id: str, service: PreUfcBoutService = Depends(get_service(PreUfcBoutService))
) -> MainResponse[list[PreUfcBoutResponse]]:
    bouts = await service.list_by_fighter(fighter_id)
    return get_ok(PreUfcBoutResponse.from_entity_list(bouts))


@pre_ufc_bouts_router.get('/by-promotion/{promotion_id}', response_model=MainResponse[list[PreUfcBoutResponse]])
async def list_by_promotion(
    promotion_id: UUID, service: PreUfcBoutService = Depends(get_service(PreUfcBoutService))
) -> MainResponse[list[PreUfcBoutResponse]]:
    bouts = await service.list_by_promotion(promotion_id)
    return get_ok(PreUfcBoutResponse.from_entity_list(bouts))


@pre_ufc_bouts_router.get(
    '/by-fighter-and-promotion/{fighter_id}/{promotion_id}', response_model=MainResponse[list[PreUfcBoutResponse]]
)
async def list_by_fighter_and_promotion(
    fighter_id: str, promotion_id: UUID, service: PreUfcBoutService = Depends(get_service(PreUfcBoutService))
) -> MainResponse[list[PreUfcBoutResponse]]:
    bouts = await service.list_by_fighter_and_promotion(fighter_id, promotion_id)
    return get_ok(PreUfcBoutResponse.from_entity_list(bouts))


@pre_ufc_bouts_router.get('/record/{fighter_id}', response_model=MainResponse[dict[str, int]])
async def fighter_record(
    fighter_id: str, service: PreUfcBoutService = Depends(get_service(PreUfcBoutService))
) -> MainResponse[dict[str, int]]:
    record = await service.record(fighter_id)
    return get_ok(record)
