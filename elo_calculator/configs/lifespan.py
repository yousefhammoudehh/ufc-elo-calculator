from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from elo_calculator.application.analytics_service import AnalyticsService
from elo_calculator.configs.log import get_logger
from elo_calculator.infrastructure.database.data_seeder import seed_data

logger = get_logger()

RECENT_YEARS_WARM = 20


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan context.

    Runs data seeding on startup and optionally warms the analytics cache.
    Any exception during seeding or warmup is logged as a warning.
    """
    # Load optional calibration/nudge artifacts for H2H at startup
    try:
        AnalyticsService.init_artifacts()
    except Exception as exc:
        logger.warning(f'Artifact init failed: {exc!r}')

    try:
        await seed_data()
    except Exception as e:
        logger.warning(f'Database seeding failed (this is normal if data already exists): {e}')

    # Warm Redis analytics for last X years
    try:
        svc = AnalyticsService()
        # Trigger large snapshot computation (cached in Redis for 15 min by service)
        await svc.rankings_history(top=1000)
        # Determine latest year and warm year slices and gains
        years = await svc.ranking_years()
        if years:
            recent = years[-RECENT_YEARS_WARM:] if len(years) > RECENT_YEARS_WARM else years
            for y in recent:
                # Warm first 30 entries and first 30 gains
                await svc.rankings_year(year=y, top=30)
                await svc.yearly_elo_gains(year=y, limit=30)
    except Exception as exc:
        # Warming is best-effort; avoid blocking startup
        logger.warning(f'Analytics warmup failed: {exc!r}')

    # Yield control to allow the application to start; no special teardown logic.
    yield
