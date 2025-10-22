from fastapi import APIRouter, Depends

from elo_calculator.application.services.judge_score_service import JudgeScoreService
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.judge_score_models import JudgeScoreResponse
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_ok

router = APIRouter(prefix='/judge-scores', tags=['judge-scores'])


@router.get('/by-bout/{bout_id}', response_model=MainResponse[list[JudgeScoreResponse]])
async def list_by_bout(
    bout_id: str, service: JudgeScoreService = Depends(get_service(JudgeScoreService))
) -> MainResponse[list[JudgeScoreResponse]]:
    scores = await service.list_by_bout(bout_id)
    return get_ok[MainResponse[list[JudgeScoreResponse]]](JudgeScoreResponse.from_entity_list(scores))


@router.get('/by-fighter/{fighter_id}', response_model=MainResponse[list[JudgeScoreResponse]])
async def list_by_fighter(
    fighter_id: str, service: JudgeScoreService = Depends(get_service(JudgeScoreService))
) -> MainResponse[list[JudgeScoreResponse]]:
    scores = await service.list_by_fighter(fighter_id)
    return get_ok[MainResponse[list[JudgeScoreResponse]]](JudgeScoreResponse.from_entity_list(scores))


@router.get('/{bout_id}/{fighter_id}', response_model=MainResponse[JudgeScoreResponse])
async def get_judge_score(
    bout_id: str, fighter_id: str, service: JudgeScoreService = Depends(get_service(JudgeScoreService))
) -> MainResponse[JudgeScoreResponse]:
    score = await service.get(bout_id, fighter_id)
    return get_ok[MainResponse[JudgeScoreResponse]](JudgeScoreResponse.from_entity(score))


@router.get('/total/{bout_id}/{fighter_id}', response_model=MainResponse[dict[str, int | None]])
async def total_score(
    bout_id: str, fighter_id: str, service: JudgeScoreService = Depends(get_service(JudgeScoreService))
) -> MainResponse[dict[str, int | None]]:
    total = await service.total(bout_id, fighter_id)
    return get_ok[MainResponse[dict[str, int | None]]]({'total': total})
