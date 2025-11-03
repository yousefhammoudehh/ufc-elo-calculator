from typing import Any

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.staticfiles import StaticFiles

from elo_calculator.configs.lifespan import lifespan
from elo_calculator.configs.log import get_logger
from elo_calculator.presentation.routers import analytics as analytics_router
from elo_calculator.presentation.routers import bout_participants as bout_participants_router
from elo_calculator.presentation.routers import bouts as bouts_router
from elo_calculator.presentation.routers import events as events_router
from elo_calculator.presentation.routers import fighters as fighters_router
from elo_calculator.presentation.routers import ingestion as ingestion_router
from elo_calculator.presentation.routers import maintenance as maintenance_router
from elo_calculator.presentation.routers import pre_ufc_bouts as pre_ufc_bouts_router
from elo_calculator.presentation.routers import promotions as promotions_router
from elo_calculator.presentation.utils.exception_handlers import register_exception_handlers

app = FastAPI(title='UFC ELO Calculator API', lifespan=lifespan)

# Resolve repository base dir to build absolute static paths (avoids cwd issues)
BASE_DIR = Path(__file__).resolve().parent

# Compression for faster payload transfer
app.add_middleware(GZipMiddleware, minimum_size=500)

register_exception_handlers(app)
app.include_router(events_router.router)
app.include_router(fighters_router.router)
app.include_router(bouts_router.router)
app.include_router(bout_participants_router.router)
app.include_router(pre_ufc_bouts_router.router)
app.include_router(promotions_router.router)
app.include_router(ingestion_router.router)
app.include_router(analytics_router.router)
app.include_router(maintenance_router.router)


def custom_openapi() -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(title='UFC ELO Calculator API', version='0.0.1', routes=app.routes)

    for path in openapi_schema['paths'].values():
        for method, method_data in path.items():
            if method == 'get':
                method_data.setdefault('parameters', []).append(
                    {
                        'name': 'params',
                        'in': 'query',
                        'required': False,
                        'schema': {'type': 'object', 'additionalProperties': {'type': 'string'}},
                    }
                )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore


@app.get('/')
async def read_root() -> dict[str, str]:
    return {'Message': 'Welcome to the UFC ELO Calculator API'}


@app.get('/health')
async def health_root() -> dict[str, str]:
    return {'Message': 'UFC ELO Calculator API is healthy'}


# Mount a lightweight static dashboard
app.mount('/viz', StaticFiles(directory=str(BASE_DIR / 'presentation' / 'static' / 'viz'), html=True), name='viz')

# Also expose the nested `viz2` directory at a top-level path /viz2
app.mount('/viz2', StaticFiles(directory=str(BASE_DIR / 'presentation' / 'static' / 'viz2'), html=True), name='viz2')


@app.middleware('http')
async def add_cache_headers(request, call_next):  # type: ignore[no-untyped-def]
    response = await call_next(request)
    try:
        path = str(request.url.path)
        # Cache analytics GET responses briefly to smooth rapid UI interactions
        if request.method == 'GET' and (path.startswith('/analytics') or path.startswith('/viz')):
            # Client-side can revalidate sooner; server has in-memory cache too
            response.headers.setdefault('Cache-Control', 'public, max-age=120')
    except Exception as exc:
        # Log and continue; do not block responses on header errors
        get_logger().warning('Failed to add cache headers: %r', exc)
    return response
