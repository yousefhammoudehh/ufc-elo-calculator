from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from elo_calculator.presentation.utils.exception_handlers import register_exception_handlers

app = FastAPI(title='UFC ELO Calculator API')

register_exception_handlers(app)


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
