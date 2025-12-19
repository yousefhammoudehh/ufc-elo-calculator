from fastapi import APIRouter, Depends, Query

from elo_calculator.application.promotion_service import PromotionService
from elo_calculator.domain.entities import Promotion
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.promotion_models import PromotionCreateRequest, PromotionResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_not_found, get_ok

promotions_router = APIRouter(prefix='/promotions', tags=['Promotions'])


@promotions_router.get('/', response_model=MainResponse[list[PromotionResponse]])
async def list_promotions(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=1000),
    service: PromotionService = Depends(get_service(PromotionService)),
) -> MainResponse[list[PromotionResponse]]:
    promotions = await service.get_all(page=page, limit=limit)
    return get_ok(PromotionResponse.from_entity_list(promotions))


@promotions_router.post('/', response_model=MainResponse[PromotionResponse])
async def create_promotion(
    request: PromotionCreateRequest, service: PromotionService = Depends(get_service(PromotionService))
) -> MainResponse[PromotionResponse]:
    entity = Promotion.from_dict(request.model_dump())
    created = await service.create(entity)
    return get_ok(PromotionResponse.from_entity(created))


@promotions_router.get('/by-link', response_model=MainResponse[PromotionResponse])
async def get_promotion_by_link(
    promotion_link: str = Query(..., description='Canonical promotion link URL'),
    service: PromotionService = Depends(get_service(PromotionService)),
) -> MainResponse[PromotionResponse]:
    promo = await service.get_by_link(promotion_link)
    if promo is None:
        return get_not_found(message=f'Promotion link not found: {promotion_link}')
    return get_ok(PromotionResponse.from_entity(promo))
