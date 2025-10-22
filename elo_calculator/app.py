from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from elo_calculator.configs.lifespan import lifespan
from elo_calculator.presentation.routers import bout_participants as bout_participants_router
from elo_calculator.presentation.routers import bouts as bouts_router
from elo_calculator.presentation.routers import events as events_router
from elo_calculator.presentation.routers import fighters as fighters_router
from elo_calculator.presentation.routers import judge_scores as judge_scores_router
from elo_calculator.presentation.routers import pre_ufc_bouts as pre_ufc_bouts_router
from elo_calculator.presentation.routers import promotions as promotions_router
from elo_calculator.presentation.utils.exception_handlers import register_exception_handlers

app = FastAPI(title='UFC ELO Calculator API', lifespan=lifespan)

register_exception_handlers(app)
app.include_router(events_router.router)
app.include_router(fighters_router.router)
app.include_router(bouts_router.router)
app.include_router(bout_participants_router.router)
app.include_router(judge_scores_router.router)
app.include_router(pre_ufc_bouts_router.router)
app.include_router(promotions_router.router)


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
