from fastapi import APIRouter, Depends

from elo_calculator.application.services.event_elo_service import EventEloService
from elo_calculator.application.services.event_ingestion_service import EventIngestionService
from elo_calculator.application.services.fight_scrape_service import FightScrapeService
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.elo_models import EventEloSeedListResponse
from elo_calculator.presentation.models.event_models import EventResponse
from elo_calculator.presentation.models.fight_models import FightScrapeListResponse
from elo_calculator.presentation.models.fighter_models import FighterResponse
from elo_calculator.presentation.models.ingestion_models import (
    EventIngestionLinksRequest,
    EventIngestionRequest,
    EventsIngestionResponse,
    FighterLinkRequest,
    FighterLinksRequest,
    FightersIngestionResponse,
    FirstUnseededEventsRequest,
    FirstUnseededEventsResponse,
)
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_bad_request, get_ok

router = APIRouter(prefix='/ingest', tags=['ingestion'])


@router.post('/events', response_model=MainResponse[EventsIngestionResponse])
async def ingest_events(
    request: EventIngestionRequest, service: EventIngestionService = Depends(get_service(EventIngestionService))
) -> MainResponse[EventsIngestionResponse]:
    fighters, events = await service.ingest_events_by_ids(request.event_ids)
    events_list = EventResponse.from_entity_list(sorted(events, key=lambda e: e.event_date))
    fighters_list = FighterResponse.from_entity_list(fighters)
    payload = EventsIngestionResponse(
        events_seeded=events_list,
        events_seeded_count=len(events_list),
        fighters_seeded=fighters_list,
        fighters_seeded_count=len(fighters_list),
    )
    return get_ok(payload)


@router.post('/events-elo', response_model=MainResponse[EventEloSeedListResponse])
async def seed_events_elo(
    request: EventIngestionRequest, service: EventEloService = Depends(get_service(EventEloService))
) -> MainResponse[EventEloSeedListResponse]:
    results = await service.seed_events_by_ids(request.event_ids)
    payload = EventEloSeedListResponse.from_service_results(results)
    return get_ok(payload)


@router.post('/events-by-link', response_model=MainResponse[EventsIngestionResponse])
async def ingest_events_by_link(
    request: EventIngestionLinksRequest, service: EventIngestionService = Depends(get_service(EventIngestionService))
) -> MainResponse[EventsIngestionResponse]:
    fighters, events = await service.ingest_events_by_links(request.event_links)
    events_list = EventResponse.from_entity_list(sorted(events, key=lambda e: e.event_date))
    fighters_list = FighterResponse.from_entity_list(fighters)
    payload = EventsIngestionResponse(
        events_seeded=events_list,
        events_seeded_count=len(events_list),
        fighters_seeded=fighters_list,
        fighters_seeded_count=len(fighters_list),
    )
    return get_ok(payload)


@router.post('/events-fights-elo', response_model=MainResponse[FightScrapeListResponse])
async def ingest_event_fights_and_elo(
    request: EventIngestionRequest, service: FightScrapeService = Depends(get_service(FightScrapeService))
) -> MainResponse[FightScrapeListResponse]:
    # Strict: stop immediately on the first error to keep seeding cumulative and correct
    results = []
    for eid in request.event_ids:
        try:
            res = await service.scrape_event_fights_and_seed(eid, strict=True)
            results.append(res)
        except Exception as exc:
            # Surface as 400 with clear message so the user can fix and retry
            return get_bad_request(message='Failed to seed fights for event', errors=str(exc))
    payload = FightScrapeListResponse.from_service_list(results)
    return get_ok(payload)


@router.post('/fights-first', response_model=MainResponse[FightScrapeListResponse])
async def ingest_first_unseeded_event_fights(
    request: FirstUnseededEventsRequest, service: FightScrapeService = Depends(get_service(FightScrapeService))
) -> MainResponse[FightScrapeListResponse]:
    # Strict chronological seeding: stop on first failure inside an event or across events
    try:
        results = await service.seed_first_unseeded_events_fights(request.limit, strict=True)
        payload = FightScrapeListResponse.from_service_list(results)
        return get_ok(payload)
    except Exception as exc:
        return get_bad_request(message='Failed to seed first unseeded event fights', errors=str(exc))


@router.post('/events-first', response_model=MainResponse[FirstUnseededEventsResponse])
async def ingest_first_unseeded_events(
    request: FirstUnseededEventsRequest, service: EventIngestionService = Depends(get_service(EventIngestionService))
) -> MainResponse[FirstUnseededEventsResponse]:
    fighters, _new_fighters, events = await service.ingest_first_unseeded_events(request.limit)
    events_sorted = sorted(events, key=lambda e: e.event_date)
    first_event = EventResponse.from_entity(events_sorted[0]) if events_sorted else None
    last_event = EventResponse.from_entity(events_sorted[-1]) if events_sorted else None

    events_list = EventResponse.from_entity_list(events_sorted)
    fighters_list = FighterResponse.from_entity_list(fighters)
    # Note: we don't include a separate list of only-new fighters in the response schema

    payload = FirstUnseededEventsResponse(
        events_seeded=events_list,
        events_seeded_count=len(events_list),
        first_event=first_event,
        last_event=last_event,
        fighters_seeded=fighters_list,
        fighters_seeded_count=len(fighters_list),
    )
    return get_ok(payload)


@router.post('/fighters-by-link', response_model=MainResponse[FightersIngestionResponse])
async def ingest_fighters_by_link(
    request: FighterLinksRequest, service: EventIngestionService = Depends(get_service(EventIngestionService))
) -> MainResponse[FightersIngestionResponse]:
    try:
        fighters = await service.ingest_fighters_by_links(request.fighter_links)
        payload = FightersIngestionResponse(
            fighters_seeded=[FighterResponse.from_entity(f) for f in fighters], fighters_seeded_count=len(fighters)
        )
        return get_ok(payload)
    except Exception as exc:
        return get_bad_request(message='Failed to ingest fighters by link', errors=str(exc))


@router.post('/fighter-by-link', response_model=MainResponse[FighterResponse])
async def ingest_fighter_by_link(
    request: FighterLinkRequest, service: EventIngestionService = Depends(get_service(EventIngestionService))
) -> MainResponse[FighterResponse]:
    try:
        fighter = await service.ingest_fighter_by_link(request.tapology_link, stats_link=request.stats_link)
        return get_ok(FighterResponse.from_entity(fighter))
    except Exception as exc:
        return get_bad_request(message='Failed to ingest fighter by link', errors=str(exc))
