"""Fighters router — ``/api/v1/fighters``."""

from fastapi import APIRouter, HTTPException, Query

from elo_calculator.application.fighters import FighterService
from elo_calculator.presentation.models.common import PaginationMeta
from elo_calculator.presentation.models.fighter_models import (
    BoutParticipantSummary,
    FighterBoutListResponse,
    FighterBoutSummary,
    FighterListResponse,
    FighterProfileResponse,
    FighterRating,
    FighterSummary,
    FighterTimeseriesResponse,
    TimeseriesPoint,
)

fighters_router = APIRouter(prefix='/api/v1/fighters', tags=['fighters'])
_service = FighterService()


@fighters_router.get('', response_model=FighterListResponse)
async def list_fighters(
    q: str = Query('', description='Search display_name (ILIKE)'),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> FighterListResponse:
    """Search or list fighters."""
    fighters, total = await _service.search_fighters(query=q, limit=limit, offset=offset)
    return FighterListResponse(
        data=[
            FighterSummary(
                fighter_id=f.fighter_id,
                display_name=f.display_name,
                nickname=f.nickname,
                country_code=f.country_code,
                sex=f.sex,
            )
            for f in fighters
        ],
        pagination=PaginationMeta(
            total=total, limit=limit, offset=offset, has_next=offset + limit < total, has_previous=offset > 0
        ),
    )


@fighters_router.get('/{fighter_id}', response_model=FighterProfileResponse)
async def get_fighter(fighter_id: str) -> FighterProfileResponse:
    """Fighter profile with current ratings across all systems."""
    fighter, ratings = await _service.get_fighter_profile(fighter_id=fighter_id)
    if fighter is None:
        raise HTTPException(status_code=404, detail='Fighter not found')
    return FighterProfileResponse(
        fighter_id=fighter.fighter_id,
        display_name=fighter.display_name,
        nickname=fighter.nickname,
        birth_date=fighter.birth_date,
        birth_place=fighter.birth_place,
        country_code=fighter.country_code,
        fighting_out_of=fighter.fighting_out_of,
        affiliation_gym=fighter.affiliation_gym,
        foundation_style=fighter.foundation_style,
        profile_image_url=fighter.profile_image_url,
        height_cm=fighter.height_cm,
        reach_cm=fighter.reach_cm,
        stance=fighter.stance,
        sex=fighter.sex,
        ratings=[
            FighterRating(system_key=rp.system_key, rating_mean=rp.rating_mean, rd=rp.rd, peak_rating=rp.peak_rating)
            for rp in ratings
        ],
    )


@fighters_router.get('/{fighter_id}/bouts', response_model=FighterBoutListResponse)
async def get_fighter_bouts(
    fighter_id: str, limit: int = Query(25, ge=1, le=200), offset: int = Query(0, ge=0)
) -> FighterBoutListResponse:
    """Paginated bout history for a fighter."""
    bouts, total = await _service.get_fighter_bouts(fighter_id=fighter_id, limit=limit, offset=offset)
    return FighterBoutListResponse(
        fighter_id=fighter_id,
        data=[
            FighterBoutSummary(
                bout_id=b.bout_id,
                event_id=b.event_id,
                event_date=b.event_date,
                event_name=b.event_name,
                division_key=b.division_key,
                weight_class_raw=b.weight_class_raw,
                is_title_fight=b.is_title_fight,
                method_group=b.method_group,
                decision_type=b.decision_type,
                finish_round=b.finish_round,
                finish_time_seconds=b.finish_time_seconds,
                participants=[
                    BoutParticipantSummary(
                        fighter_id=p.fighter_id, display_name=p.display_name, corner=p.corner, outcome_key=p.outcome_key
                    )
                    for p in b.participants
                ],
            )
            for b in bouts
        ],
        pagination=PaginationMeta(
            total=total, limit=limit, offset=offset, has_next=offset + limit < total, has_previous=offset > 0
        ),
    )


@fighters_router.get('/{fighter_id}/timeseries', response_model=FighterTimeseriesResponse)
async def get_fighter_timeseries(
    fighter_id: str,
    system: str = Query('unified_composite_elo', description='Rating system key'),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> FighterTimeseriesResponse:
    """Rating timeseries for a fighter under one system (paginated)."""
    points, total = await _service.get_fighter_timeseries(
        fighter_id=fighter_id, system_key=system, limit=limit, offset=offset
    )
    return FighterTimeseriesResponse(
        fighter_id=fighter_id,
        system_key=system,
        data=[TimeseriesPoint(date=pt.date, rating_mean=pt.rating_mean, rd=pt.rd) for pt in points],
        pagination=PaginationMeta(
            total=total, limit=limit, offset=offset, has_next=offset + limit < total, has_previous=offset > 0
        ),
    )
