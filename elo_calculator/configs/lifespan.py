from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from elo_calculator.application.services.analytics_service import AnalyticsService
from elo_calculator.configs.log import get_logger
from elo_calculator.infrastructure.database.data_seeder import seed_data

RECENT_YEARS_WARM = 20


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[Any]:
    # Load optional calibration/nudge artifacts for H2H at startup
    try:
        AnalyticsService.init_artifacts()
    except Exception as exc:  # best-effort
        get_logger().warning('Artifact init failed: %r', exc)
    await seed_data()
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
        get_logger().warning('Analytics warmup failed: %r', exc)
    yield
