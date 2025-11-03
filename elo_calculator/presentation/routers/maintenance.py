from typing import Any

from fastapi import APIRouter, Depends, Query

from elo_calculator.application.services.cache_maintenance_service import CacheMaintenanceService
from elo_calculator.application.services.entry_elo_service import EntryEloService
from elo_calculator.application.services.maintenance_service import MaintenanceService
from elo_calculator.presentation.dependencies import get_service
from elo_calculator.presentation.models.maintenance_models import (
    CacheFlushResponse,
    CacheInvalidateRequest,
    CacheInvalidateResponse,
    EntryEloReseedRequest,
    EntryEloReseedResponse,
)
from elo_calculator.presentation.models.shared import MainResponse
from elo_calculator.presentation.utils.response import get_ok

router = APIRouter(prefix='/maintenance', tags=['maintenance'])


@router.post('/cache/flush', response_model=MainResponse[CacheFlushResponse])
async def flush_cache(
    service: CacheMaintenanceService = Depends(get_service(CacheMaintenanceService)),
) -> MainResponse[CacheFlushResponse]:
    payload = await service.flush_all()
    return get_ok(CacheFlushResponse(**payload))


@router.post('/cache/invalidate', response_model=MainResponse[CacheInvalidateResponse])
async def invalidate_cache(
    request: CacheInvalidateRequest, service: CacheMaintenanceService = Depends(get_service(CacheMaintenanceService))
) -> MainResponse[CacheInvalidateResponse]:
    batch = request.batch_size or 200
    result = await service.invalidate_prefixes(request.prefixes, batch_size=batch)
    return get_ok(CacheInvalidateResponse(**result))


@router.post('/elo/reseed', response_model=MainResponse[EntryEloReseedResponse])
async def reseed_starting_elo(
    request: EntryEloReseedRequest, service: EntryEloService = Depends(get_service(EntryEloService))
) -> MainResponse[EntryEloReseedResponse]:
    """Recalculate each fighter's entry/current/peak ELO from pre-UFC bouts.

    - Uses promotion strengths currently stored in the database.
    - If a fighter has no pre-UFC bouts, defaults to 1500.
    - Set `dry_run=true` to preview without persisting changes.
    - Optional `default_strength` overrides the fallback strength used when a promotion has none.
    """
    result = await service.reseed_all(default_strength=request.default_strength, dry_run=bool(request.dry_run))
    return get_ok(EntryEloReseedResponse(**result))


@router.post('/sync-fighters', response_model=MainResponse[dict[str, Any]])
async def sync_fighters(
    throttle_ms: int = Query(default=250, ge=0, le=2000),
    service: MaintenanceService = Depends(get_service(MaintenanceService)),
) -> MainResponse[dict[str, Any]]:
    summary = await service.sync_fighter_names_and_links(throttle_ms=throttle_ms)
    return get_ok(summary)


@router.post('/seed-event-names', response_model=MainResponse[dict[str, Any]])
async def seed_event_names(
    throttle_ms: int = Query(default=250, ge=0, le=2000),
    service: MaintenanceService = Depends(get_service(MaintenanceService)),
) -> MainResponse[dict[str, Any]]:
    """Scrape UFCStats event pages and store event names (b-content__title-highlight)."""
    summary = await service.seed_event_names(throttle_ms=throttle_ms)
    return get_ok(summary)
