import contextlib
import hashlib
import json
from typing import Any as _Any
from typing import cast as _cast

from fastapi import APIRouter, Depends, Query, Request, Response

from elo_calculator.application.analytics_service import AnalyticsService
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.analytics_models import (
    EloChangeItem,
    EloMoverItem,
    FighterEloHistoryResponse,
    RandomBoutItem,
    RankingSnapshot,
    TopFighterResponse,
    YearlyEloGainItem,
)
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_bad_request, get_not_found, get_ok

analytics_router = APIRouter(prefix='/analytics', tags=['Analytics'])


@analytics_router.get('/top-elo', response_model=MainResponse[list[TopFighterResponse]])
async def top_elo(
    limit: int = Query(default=20, ge=1, le=100), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[TopFighterResponse]]:
    fighters = await _cast(_Any, service).top_fighters_by_elo(limit=limit)
    return get_ok(TopFighterResponse.from_entity_list(fighters))


@analytics_router.get('/top-peak-elo', response_model=MainResponse[list[TopFighterResponse]])
async def top_peak_elo(
    limit: int = Query(default=20, ge=1, le=100), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[TopFighterResponse]]:
    fighters = await _cast(_Any, service).top_fighters_by_peak_elo(limit=limit)
    return get_ok(TopFighterResponse.from_entity_list(fighters))


@analytics_router.get('/fighter-elo/{fighter_id}', response_model=MainResponse[FighterEloHistoryResponse])
async def fighter_elo(
    fighter_id: str, service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[FighterEloHistoryResponse]:
    fighter, points = await _cast(_Any, service).fighter_elo_history(fighter_id)
    if not fighter:
        return get_not_found(message=f'Fighter id:{fighter_id} not found')
    return get_ok(FighterEloHistoryResponse.from_service(fighter, points))


@analytics_router.get('/top-elo-gains', response_model=MainResponse[list[EloChangeItem]])
async def top_elo_gains(
    limit: int = Query(default=20, ge=1, le=200), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[EloChangeItem]]:
    items = await _cast(_Any, service).top_elo_gains(limit=limit)
    return get_ok(EloChangeItem.from_service(items))


@analytics_router.get('/lowest-elo-gains', response_model=MainResponse[list[EloChangeItem]])
async def lowest_elo_gains(
    limit: int = Query(default=20, ge=1, le=200), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[EloChangeItem]]:
    items = await _cast(_Any, service).lowest_elo_gains(limit=limit)
    return get_ok(EloChangeItem.from_service(items))


@analytics_router.get('/top-elo-losses', response_model=MainResponse[list[EloChangeItem]])
async def top_elo_losses(
    limit: int = Query(default=20, ge=1, le=200), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[EloChangeItem]]:
    items = await _cast(_Any, service).top_elo_losses(limit=limit)
    return get_ok(EloChangeItem.from_service(items))


@analytics_router.get('/elo-movers', response_model=MainResponse[list[EloMoverItem]])
async def elo_movers(
    direction: str = Query(default='gains', pattern='^(gains|losses|net)$'),
    window_days: int | None = Query(default=None, ge=1, le=3650),
    window_range: str | None = Query(default=None, pattern=r'^\d+d$'),
    limit: int = Query(default=10, ge=1, le=200),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[EloMoverItem]]:
    if window_range:
        with contextlib.suppress(Exception):
            window_days = int(window_range.rstrip('d'))
    data = await service.elo_movers(direction=direction, window_days=window_days, limit=limit)
    items = [EloMoverItem(**item) for item in data]
    return get_ok(items)


@analytics_router.get('/top-elo-gain', response_model=MainResponse[list[dict[str, _Any]]])
async def top_elo_gain(
    limit: int = Query(default=20, ge=1, le=200), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[dict[str, _Any]]]:
    items = await _cast(_Any, service).top_fighters_by_elo_gain(limit=limit)
    return get_ok(items)


@analytics_router.get('/top-peak-elo-gain', response_model=MainResponse[list[dict[str, _Any]]])
async def top_peak_elo_gain(
    limit: int = Query(default=20, ge=1, le=200), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[dict[str, _Any]]]:
    items = await _cast(_Any, service).top_fighters_by_peak_elo_gain(limit=limit)
    return get_ok(items)


@analytics_router.get('/random-bouts', response_model=MainResponse[list[RandomBoutItem]])
async def random_bouts(
    limit: int = Query(default=10, ge=1, le=100), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[list[RandomBoutItem]]:
    items = await _cast(_Any, service).random_bouts(limit=limit)
    return get_ok(RandomBoutItem.from_service(items))


@analytics_router.get('/rankings-history', response_model=MainResponse[list[RankingSnapshot]])
async def rankings_history(
    start_year: int | None = Query(default=None, ge=1900),
    end_year: int | None = Query(default=None, ge=1900),
    top: int = Query(default=15, ge=1, le=50),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[RankingSnapshot]]:
    items = await _cast(_Any, service).rankings_history(
        interval='year', start_year=start_year, end_year=end_year, top=top
    )
    return get_ok(RankingSnapshot.from_service(items))


@analytics_router.get('/yearly-elo-gains', response_model=MainResponse[list[YearlyEloGainItem]])
async def yearly_elo_gains(
    year: int = Query(..., ge=1900),
    limit: int = Query(default=10, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
    page_size: int | None = Query(default=None, ge=1, le=20000),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[YearlyEloGainItem]]:
    items = await _cast(_Any, service).yearly_elo_gains(year=year, limit=limit, offset=offset, page_size=page_size)
    return get_ok([YearlyEloGainItem(**x) for x in items])


@analytics_router.get('/ranking-years', response_model=MainResponse[list[int]])
async def ranking_years(service: AnalyticsService = Depends(get_service(AnalyticsService))) -> MainResponse[list[int]]:
    yrs = await service.ranking_years()
    return get_ok(_cast(_Any, yrs))


@analytics_router.get('/rankings-year', response_model=MainResponse[RankingSnapshot])
async def rankings_year(
    year: int = Query(..., ge=1900),
    top: int | None = Query(default=None, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    page_size: int | None = Query(default=None, ge=1, le=1000),
    division: int | None = Query(default=None),
    request: Request = None,  # type: ignore[assignment]
    response: Response = None,  # type: ignore[assignment]
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[RankingSnapshot]:
    snap = await service.rankings_year(year=year, top=top, offset=offset, page_size=page_size, division=division)
    # ETag support
    body_dict = {'label': snap.get('label'), 'date': snap.get('date'), 'entries': snap.get('entries')}
    ser = json.dumps(body_dict, sort_keys=True, default=str).encode('utf-8')
    etag = hashlib.sha256(ser).hexdigest()
    inm = request.headers.get('if-none-match') if request else None
    if inm and inm == etag:
        if response:
            response.status_code = 304
        return get_ok(
            RankingSnapshot.from_service([{'label': snap.get('label'), 'date': snap.get('date'), 'entries': []}])[0]
        )
    if response:
        response.headers['ETag'] = etag
    model = RankingSnapshot.from_service([snap])[0]
    return get_ok(model)


@analytics_router.get('/h2h', response_model=MainResponse[dict[str, _Any]])
async def h2h(  # noqa: PLR0913
    fighter1: str,
    fighter2: str,
    mode1: str = Query(default='current', pattern='^(current|peak|year)$'),
    mode2: str = Query(default='current', pattern='^(current|peak|year)$'),
    year1: int | None = Query(default=None, ge=1900),
    year2: int | None = Query(default=None, ge=1900),
    adjust: str = Query(default='base', pattern='^(base|nudge|window|meta|best)$'),
    ewma_hl: int | None = Query(default=None, ge=1),
    five_round: bool | None = Query(default=None),
    title: bool | None = Query(default=None),
    explain: bool = Query(default=False),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    try:
        res = await _cast(_Any, service).h2h_probability(
            fighter1,
            fighter2,
            mode1=mode1,
            mode2=mode2,
            year1=year1,
            year2=year2,
            adjust=adjust,
            explain=explain,
            ewma_hl=ewma_hl,
            five_round=five_round,
            title=title,
        )
    except ValueError as exc:
        detail = str(exc) or 'Invalid fighter id(s)'
        return get_bad_request(message='Invalid fighter id(s)', errors=[{'code': 'INVALID_FIGHTER', 'message': detail}])
    return get_ok(res)


# Removed H2H calibration endpoint per product decision; probabilities remain available via /h2h


@analytics_router.get('/hazard', response_model=MainResponse[dict[str, _Any]])
async def hazard(
    fighter_id: str,
    five_round: str = Query(default='auto', pattern='^(auto|true|false)$'),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await _cast(_Any, service).hazard(fighter_id=fighter_id, five_round=five_round)
    return get_ok(data)


@analytics_router.get('/division', response_model=MainResponse[dict[str, _Any]])
async def division_roster(
    code: int = Query(...),
    top: int = Query(default=10, ge=1, le=1000),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await _cast(_Any, service).division_roster(code=code, top=top)
    return get_ok(data)


@analytics_router.get('/form-top', response_model=MainResponse[list[dict[str, _Any]]])
async def form_top(
    window: str = Query(default='fights', pattern='^(fights|days)$'),
    n: int = Query(default=6, ge=1),
    half_life_days: int = Query(default=180, ge=1, le=2000),
    top: int = Query(default=3, ge=1, le=50),
    min_recent_fights: int = Query(default=0, ge=0, le=50),
    recent_days: int | None = Query(default=None, ge=1, le=5000),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[dict[str, _Any]]]:
    data = await _cast(_Any, service).form_top(
        window=window,
        n=n,
        half_life_days=half_life_days,
        top=top,
        min_recent_fights=min_recent_fights,
        recent_days=recent_days,
    )
    return get_ok(data)


@analytics_router.get('/fighter-career-stats/{fighter_id}', response_model=MainResponse[dict[str, _Any]])
async def fighter_career_stats(
    fighter_id: str, service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[dict[str, _Any]]:
    data = await service.fighter_career_stats(fighter_id)
    return get_ok(data)


@analytics_router.get('/latest-event-elo', response_model=MainResponse[dict[str, _Any]])
async def latest_event_elo(
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.latest_event_elo()
    return get_ok(data)


@analytics_router.get('/event-elo', response_model=MainResponse[dict[str, _Any]])
async def event_elo(
    event_id: str = Query(...), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[dict[str, _Any]]:
    data = await service.event_elo(event_id=event_id)
    return get_ok(data)


@analytics_router.get('/events', response_model=MainResponse[list[dict[str, _Any]]])
async def events_list(
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[dict[str, _Any]]]:
    items = await service.events_list()
    return get_ok(items)


@analytics_router.get('/top-stats', response_model=MainResponse[list[dict[str, _Any]]])
async def top_stats(
    metric: str = Query(
        ...,
        pattern='^(kd|td|td_attempts|sub_attempts|reversals|control_time_sec|sig_strikes|sig_strikes_thrown|total_strikes|total_strikes_thrown|head_ss|body_ss|leg_ss|distance_ss|clinch_ss|ground_ss)$',
    ),
    limit: int = Query(default=20, ge=1, le=200),
    since_year: int | None = Query(default=None, ge=1900),
    division: int | None = Query(default=None),
    rate: str = Query(default='total', pattern='^(total|per15)$'),
    adjusted: bool = Query(default=False),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[dict[str, _Any]]]:
    items = await service.top_fighter_stats(
        metric=metric, limit=limit, since_year=since_year, division=division, rate=rate, adjusted=adjusted
    )
    return get_ok(items)


@analytics_router.get('/plusminus', response_model=MainResponse[dict[str, _Any]])
async def plus_minus(
    fighter_id: str,
    metric: str = Query(
        default='sig_strikes', pattern='^(kd|td|td_attempts|sub_attempts|control_time_sec|sig_strikes|total_strikes)$'
    ),
    since_year: int | None = Query(default=None, ge=1900),
    opp_window_months: int = Query(default=18, ge=1, le=120),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.plus_minus(
        fighter_id=fighter_id, metric=metric, since_year=since_year, opp_window_months=opp_window_months
    )
    return get_ok(data)


@analytics_router.get('/consistency-versatility', response_model=MainResponse[dict[str, _Any]])
async def consistency_versatility(
    fighter_id: str,
    k: int = Query(default=6, ge=2, le=30),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.consistency_versatility(fighter_id=fighter_id, k=k)
    return get_ok(data)


@analytics_router.get('/divisions', response_model=MainResponse[list[dict[str, _Any]]])
async def divisions(
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[dict[str, _Any]]]:
    items = await service.divisions()
    return get_ok(items)


@analytics_router.get('/division-rankings', response_model=MainResponse[list[dict[str, _Any]]])
async def division_rankings(
    division: int = Query(...),
    metric: str = Query(default='current', pattern='^(current|peak|gains)$'),
    year: int | None = Query(default=None, ge=1900),
    active_only: bool = Query(default=False),
    limit: int = Query(default=20, ge=1, le=200),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[dict[str, _Any]]]:
    items = await service.division_rankings(
        division=division, metric=metric, year=year, active_only=active_only, limit=limit
    )
    return get_ok(items)


@analytics_router.get('/division-parity', response_model=MainResponse[dict[str, _Any]])
async def division_parity(
    division: int = Query(...),
    year: int | None = Query(default=None, ge=1900),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.division_parity(division=division, year=year)
    return get_ok(data)


@analytics_router.get('/division-churn', response_model=MainResponse[dict[str, _Any]])
async def division_churn(
    division: int = Query(...),
    year: int = Query(..., ge=1900),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.division_churn(division=division, year=year)
    return get_ok(data)


@analytics_router.get('/form', response_model=MainResponse[dict[str, _Any]])
async def form_index(
    fighter_id: str,
    window: str = Query(default='fights', pattern='^(fights|days)$'),
    n: int = Query(default=5, ge=1, le=7300),
    half_life_days: int = Query(default=180, ge=1, le=2000),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.form_index(fighter_id=fighter_id, window=window, n=n, half_life_days=half_life_days)
    return get_ok(data)


@analytics_router.get('/momentum', response_model=MainResponse[dict[str, _Any]])
async def momentum(
    fighter_id: str,
    k: int = Query(default=6, ge=2, le=50),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.momentum_slope(fighter_id=fighter_id, k=k)
    return get_ok(data)


@analytics_router.get('/rates', response_model=MainResponse[dict[str, _Any]])
async def rates(
    fighter_id: str, service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[dict[str, _Any]]:
    data = await service.rates_per_minute(fighter_id=fighter_id)
    return get_ok(data)


@analytics_router.get('/event-shock', response_model=MainResponse[dict[str, _Any]])
async def event_shock(
    event_id: str | None = Query(default=None), service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[dict[str, _Any]]:
    data = await service.event_shock(event_id=event_id)
    return get_ok(data)


@analytics_router.get('/latest-event-shock', response_model=MainResponse[dict[str, _Any]])
async def latest_event_shock(
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.event_shock(event_id=None)
    return get_ok(data)


@analytics_router.get('/events-shock-top', response_model=MainResponse[list[dict[str, _Any]]])
async def events_shock_top(
    limit: int = Query(default=5, ge=1, le=50),
    order: str | None = Query(default=None, pattern='^(asc|desc)$'),
    max_events: int | None = Query(default=200, ge=1, le=2000),
    window_days: int | None = Query(default=None, ge=1, le=3650),
    window_range: str | None = Query(default=None, pattern=r'^\d+d$', alias='range'),
    type_: str | None = Query(default=None, pattern='^(shocking|predictable)$', alias='type'),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[list[dict[str, _Any]]]:
    if window_range:
        with contextlib.suppress(Exception):
            window_days = int(window_range.rstrip('d'))
    order_effective = order or 'desc'
    if type_:
        order_effective = 'desc' if type_ == 'shocking' else 'asc'
    data = await service.events_shock_top(
        limit=limit, order=order_effective, max_events=max_events, window_days=window_days
    )
    return get_ok(data)


@analytics_router.get('/sos', response_model=MainResponse[dict[str, _Any]])
async def sos(
    fighter_id: str,
    window: str = Query(default='days', pattern='^(days|fights)$'),
    n: int = Query(default=365, ge=1, le=36500),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.sos(fighter_id=fighter_id, window=window, n=n)
    return get_ok(data)


@analytics_router.get('/quality-wins', response_model=MainResponse[dict[str, _Any]])
async def quality_wins(
    fighter_id: str,
    elo_threshold: float = Query(..., ge=0.0),
    service: AnalyticsService = Depends(get_service(AnalyticsService)),
) -> MainResponse[dict[str, _Any]]:
    data = await service.quality_wins(fighter_id=fighter_id, elo_threshold=elo_threshold)
    return get_ok(data)


@analytics_router.get('/style-profile', response_model=MainResponse[dict[str, _Any]])
async def style_profile(
    fighter_id: str, service: AnalyticsService = Depends(get_service(AnalyticsService))
) -> MainResponse[dict[str, _Any]]:
    data = await service.style_profile(fighter_id=fighter_id)
    return get_ok(data)
