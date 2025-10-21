from typing import Any, Optional
from uuid import UUID

from olive_template.application.base_service import BaseService
from olive_template.application.shared.exceptions import ApplicationException
from olive_template.domain.client.entity import Client
from olive_template.infrastructure.repositories.unit_of_work import get_uow


class ClientService(BaseService):
    async def create(self, client: Client) -> Client:
        async with get_uow() as uow:
            # TODO: use identity_provider service to create the client in Auth0

            current_clients = await uow.client_repo.get_by_code_or_contact_email(client.code,
                                                                                 client.contact_person_email)
            if current_clients:
                errors = []
                if next((c for c in current_clients if c.code == client.code), None):
                    errors.append({'field': 'code', 'message': 'The code already in use'})
                if next((c for c in current_clients if c.contact_person_email
                         == client.contact_person_email), None):
                    errors.append({'field': 'contact_person_email', 'message': 'The email already in use'})
                raise ApplicationException('The code or the contact person email are already exist', errors)
            client.created_by = self.user.id
            db_client = await uow.client_repo.add(client)
            return db_client

    async def get_all(self, page: int = 1, limit: int = 10, filters: Optional[dict[str, Any]] = None,
                      sort_by: str = 'created_at', order: str = 'desc') -> tuple[list[Client], int]:
        async with get_uow() as uow:
            clients, count = await uow.client_repo.get_paginated_with_filters(page, limit, filters, sort_by, order)
            return clients, count

    async def get_by_id(self, id: UUID) -> Optional[Client]:
        async with get_uow() as uow:
            client = await uow.client_repo.get_by_id(id)
            return client

    async def update(self, id: UUID, data: dict[str, Any]) -> Client:
        async with get_uow() as uow:
            # TODO: use identity_provider service to update the client in Auth0
            # TODO: validate the new code and email if thy are part of the updated data
            data['updated_by'] = self.user.id
            db_client = await uow.client_repo.update(id, data)
            return db_client

    async def delete(self, id: UUID,) -> Client:
        async with get_uow() as uow:
            # TODO: use identity_provider service to delete the client in Auth0
            db_client = await uow.client_repo.delete(id)
            return db_client
