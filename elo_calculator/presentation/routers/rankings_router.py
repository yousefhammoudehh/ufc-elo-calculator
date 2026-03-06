"""Rankings router — ``/api/v1/rankings``."""

from fastapi import APIRouter, HTTPException, Query

from elo_calculator.application.leaderboard import LeaderboardService
from elo_calculator.presentation.models.common import PaginationMeta
from elo_calculator.presentation.models.ranking_models import RankingEntry, RankingListResponse

rankings_router = APIRouter(prefix='/api/v1/rankings', tags=['rankings'])
_service = LeaderboardService()


@rankings_router.get('', response_model=RankingListResponse)
async def get_rankings(
    system: str = Query(..., description='Rating system key, e.g. elo_ps'),
    division: str = Query(..., description='Division key, e.g. MMA_LW'),
    sex: str = Query('M', description='Sex filter: M, F, or U'),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> RankingListResponse:
    """Current rankings for a given system + division."""
    rankings, total = await _service.get_rankings(
        system_key=system, division_key=division, sex=sex, limit=limit, offset=offset
    )
    if not rankings and total == 0:
        raise HTTPException(
            status_code=404, detail=f'No rankings found for system={system}, division={division}, sex={sex}'
        )

    as_of = rankings[0].as_of_date if rankings else ''
    return RankingListResponse(
        system_key=system,
        division_key=division,
        sex=sex,
        as_of_date=as_of,
        data=[
            RankingEntry(
                rank=r.rank,
                fighter_id=r.fighter_id,
                display_name=r.display_name,
                rating_mean=r.rating_mean,
                rd=r.rd,
                last_fight_date=r.last_fight_date,
            )
            for r in rankings
        ],
        pagination=PaginationMeta(
            total=total, limit=limit, offset=offset, has_next=offset + limit < total, has_previous=offset > 0
        ),
    )
