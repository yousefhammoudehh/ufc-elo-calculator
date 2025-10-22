from fastapi import APIRouter, Depends

from elo_calculator.application.services.promotion_service import PromotionService
from elo_calculator.domain.entities import Promotion
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.promotion_models import PromotionCreateRequest, PromotionResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_ok

router = APIRouter(prefix='/promotions', tags=['promotions'])


@router.get('/', response_model=MainResponse[list[PromotionResponse]])
async def list_promotions(
    service: PromotionService = Depends(get_service(PromotionService)),
) -> MainResponse[list[PromotionResponse]]:
    promotions = await service.get_all()
    return get_ok(PromotionResponse.from_entity_list(promotions))


@router.post('/', response_model=MainResponse[PromotionResponse])
async def create_promotion(
    request: PromotionCreateRequest, service: PromotionService = Depends(get_service(PromotionService))
) -> MainResponse[PromotionResponse]:
    entity = Promotion.from_dict(request.model_dump())
    created = await service.create(entity)
    return get_ok(PromotionResponse.from_entity(created))
