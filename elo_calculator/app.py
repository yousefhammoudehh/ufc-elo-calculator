from fastapi import FastAPI

from elo_calculator.configs.lifespan import lifespan
from elo_calculator.presentation.routers.app_router import app_router

app = FastAPI(title='UFC ELO Calculator', lifespan=lifespan)
app.include_router(app_router)
