"""Bouts router — ``/api/v1/bouts``."""

from fastapi import APIRouter, HTTPException

from elo_calculator.application.bouts import BoutService
from elo_calculator.presentation.models.bout_models import (
    BoutDetailResponse,
    BoutParticipantResponse,
    FightStatsResponse,
    PerformanceScoreResponse,
    RatingChangeResponse,
)

bouts_router = APIRouter(prefix='/api/v1/bouts', tags=['bouts'])
_service = BoutService()


@bouts_router.get('/{bout_id}', response_model=BoutDetailResponse)
async def get_bout(bout_id: str) -> BoutDetailResponse:
    """Full bout detail with stats, rating changes and performance scores."""
    bout = await _service.get_bout_detail(bout_id=bout_id)
    if bout is None:
        raise HTTPException(status_code=404, detail='Bout not found')
    return BoutDetailResponse(
        bout_id=bout.bout_id,
        event_id=bout.event_id,
        event_date=bout.event_date,
        event_name=bout.event_name,
        sport_key=bout.sport_key,
        division_key=bout.division_key,
        weight_class_raw=bout.weight_class_raw,
        is_title_fight=bout.is_title_fight,
        method_group=bout.method_group,
        decision_type=bout.decision_type,
        finish_round=bout.finish_round,
        finish_time_seconds=bout.finish_time_seconds,
        scheduled_rounds=bout.scheduled_rounds,
        referee=bout.referee,
        participants=[
            BoutParticipantResponse(
                fighter_id=p.fighter_id, display_name=p.display_name, corner=p.corner, outcome_key=p.outcome_key
            )
            for p in bout.participants
        ],
        fight_stats=[
            FightStatsResponse(
                fighter_id=s.fighter_id,
                kd=s.kd,
                sig_landed=s.sig_landed,
                sig_attempted=s.sig_attempted,
                total_landed=s.total_landed,
                total_attempted=s.total_attempted,
                td_landed=s.td_landed,
                td_attempted=s.td_attempted,
                sub_attempts=s.sub_attempts,
                ctrl_seconds=s.ctrl_seconds,
            )
            for s in bout.fight_stats
        ],
        rating_changes=[
            RatingChangeResponse(
                fighter_id=d.fighter_id,
                system_key=d.system_key,
                pre_rating=d.pre_rating,
                post_rating=d.post_rating,
                delta_rating=d.delta_rating,
                expected_win_prob=d.expected_win_prob,
            )
            for d in bout.rating_changes
        ],
        performance_scores=[
            PerformanceScoreResponse(fighter_id=ps.fighter_id, ps_fight=ps.ps_fight, quality_of_win=ps.quality_of_win)
            for ps in bout.performance_scores
        ],
    )
