
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from elo_calculator.configs.lifespan import lifespan
from elo_calculator.presentation.routers.analytics import analytics_router
from elo_calculator.presentation.routers.bout_participants import bout_participants_router
from elo_calculator.presentation.routers.bouts import bouts_router
from elo_calculator.presentation.routers.events import events_router
from elo_calculator.presentation.routers.fighters import fighters_router
from elo_calculator.presentation.routers.ingestion import ingestion_router
from elo_calculator.presentation.routers.maintenance import maintenance_router
from elo_calculator.presentation.routers.pre_ufc_bouts import pre_ufc_bouts_router
from elo_calculator.presentation.routers.promotions import promotions_router
from elo_calculator.presentation.utils.exception_handlers import register_exception_handlers
from elo_calculator.presentation.utils.openapi import custom_openapi

app = FastAPI(title='UFC ELO Calculator API', lifespan=lifespan)

# Compression for faster payload transfer
app.add_middleware(GZipMiddleware, minimum_size=500)

register_exception_handlers(app)

app.include_router(events_router, prefix='/api')
app.include_router(fighters_router, prefix='/api')
app.include_router(bouts_router, prefix='/api')
app.include_router(bout_participants_router, prefix='/api')
app.include_router(pre_ufc_bouts_router, prefix='/api')
app.include_router(promotions_router, prefix='/api')
app.include_router(ingestion_router, prefix='/api')
app.include_router(analytics_router, prefix='/api')
app.include_router(maintenance_router, prefix='/api')

app.openapi = lambda: custom_openapi(app)  # type: ignore


@app.get('/health')
async def health_root() -> dict[str, str]:
    return {'message': 'UFC ELO Calculator API is healthy'}


@app.get('/')
async def read_root() -> dict[str, str]:
    return {'message': 'Welcome to the UFC ELO Calculator API'}
