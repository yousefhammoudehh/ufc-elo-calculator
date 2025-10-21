from typing import Any

from fastapi import status
from httpx import HTTPStatusError, ReadTimeout

from olive_template.configs.env import IDENTITY_PROVIDER_URL
from olive_template.domain.client.entity import Client
from olive_template.domain.user.entity import User
from olive_template.infrastructure.shared.exceptions import ExternalServiceError
from olive_template.infrastructure.utils.http_client import get_client


async def create_client(client: Client, user: User) -> Any:
    try:
        headers = {'access_token': user.access_token, 'id_token': user.id_token}
        body = {'name': client.name, 'display_name': client.name, 'branding': {'logo_url': client.logo_url, }}

        with get_client() as http_client:
            response = http_client.post(url=IDENTITY_PROVIDER_URL, json=body, headers=headers)
        response.raise_for_status()
        return response.json()
    except (HTTPStatusError, ReadTimeout) as e:
        code = status.HTTP_424_FAILED_DEPENDENCY
        message = str(e)
        if hasattr(e, 'response'):
            code = e.response.status_code
            message = e.response.text

        raise ExternalServiceError('identity-provider', message, code)
