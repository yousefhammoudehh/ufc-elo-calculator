from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi


def custom_openapi(app: FastAPI) -> dict[str, Any]:
    """Custom OpenAPI schema generator for the UFC ELO Calculator API.

    Extends the default OpenAPI schema to include custom query parameters
    for GET endpoints.
    """
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
