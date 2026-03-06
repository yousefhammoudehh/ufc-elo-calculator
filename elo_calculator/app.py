from fastapi import FastAPI

from elo_calculator.configs.lifespan import lifespan
from elo_calculator.presentation.routers.app_router import app_router
from elo_calculator.presentation.routers.bouts_router import bouts_router
from elo_calculator.presentation.routers.events_router import events_router
from elo_calculator.presentation.routers.fighters_router import fighters_router
from elo_calculator.presentation.routers.rankings_router import rankings_router
from elo_calculator.presentation.routers.reference_router import reference_router

app = FastAPI(title='UFC ELO Calculator', lifespan=lifespan)
app.include_router(app_router)
app.include_router(rankings_router)
app.include_router(fighters_router)
app.include_router(events_router)
app.include_router(bouts_router)
app.include_router(reference_router)
