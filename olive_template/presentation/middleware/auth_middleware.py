from typing import Any, Awaitable, Callable

from jwt import DecodeError, decode
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from olive_template.configs.log import get_logger
from olive_template.domain.user.entity import User
from olive_template.presentation.utils.response import get_unauthorized

logger = get_logger()

RequestResponseEndpoint = Callable[[Request], Awaitable[Response]]


async def decode_jwt(token: str) -> Any:
    try:
        decoded_claims = decode(token, options={"verify_signature": False})
        return decoded_claims
    except DecodeError as exc:
        logger.exception(exc)
        return None


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:

        if request.url.path.lower().startswith('/api'):
            access_token = request.headers.get('x-access-token', '')
            id_token = request.headers.get('x-id-token', '')
            if not access_token or not id_token:
                return get_unauthorized('Authorization tokens are required')

            access_token_claims = await decode_jwt(access_token)
            id_token_claims = await decode_jwt(id_token)

            if not access_token_claims or not id_token_claims:
                return get_unauthorized('Authorization tokens are invalid')

            request.state.user = User(auth0_id=access_token_claims['sub'],
                                      name=id_token_claims['name'],
                                      email=id_token_claims['email'],
                                      auth0_org_id=id_token_claims['org_id'],
                                      permissions=access_token_claims['permissions'],
                                      id=id_token_claims.get('MetaData', {}).get('olive_id', None),
                                      id_token=id_token,
                                      access_token=access_token)

        response = await call_next(request)
        return response
