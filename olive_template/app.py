from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from olive_template.presentation.middleware.auth_middleware import AuthMiddleware
from olive_template.presentation.routers.clients import clients_router
from olive_template.presentation.utils.exception_handlers import register_exception_handlers

app = FastAPI(title='Olive clients serves')

register_exception_handlers(app)

app.add_middleware(AuthMiddleware)


app.include_router(clients_router)


def custom_openapi() -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(title='Olive clients serves', version='0.0.1', routes=app.routes)
    openapi_schema['components']['securitySchemes'] = {
        'AccessAndIdTokens': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'x-access-token',
            'description': 'Access token for authentication',
        },
        'IdToken': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'x-id-token',
            'description': 'ID token for user identification',
        }
    }
    openapi_schema['security'] = [{'AccessAndIdTokens': []}, {'IdToken': []}]

    for path in openapi_schema['paths'].values():
        for method, method_data in path.items():
            if method == 'get':
                method_data.setdefault('parameters', []).append(
                    {
                        'name': 'params',
                        'in': 'query',
                        'required': False,
                        'schema': {
                            'type': 'object',
                            'additionalProperties': {'type': 'string'},
                        },
                    }
                )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore


@app.get('/')
async def read_root() -> dict[str, str]:
    return {'Message': 'Welcome to olive clients serves'}


@app.get('/heath')
async def health_root() -> dict[str, str]:
    return {'Message': 'Olive clients serves is working!'}
