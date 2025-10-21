from fastapi import APIRouter, Depends, Query, Request
from pydantic import UUID4

from olive_template.application.client_service import ClientService
from olive_template.domain.client.entity import Client
from olive_template.presentation.dependencies import get_service
from olive_template.presentation.models.clients import (
    ClientResponse, CreateClientsRequest, MainResponse, PaginatedResponse, UpdateClientsRequest
)
from olive_template.presentation.utils.response import get_not_found, get_ok

clients_router = APIRouter(prefix='/api/clients', tags=['Clients'])


@clients_router.get('')
async def get_all(request: Request, limit: int = Query(10, ge=1), page: int = Query(1, ge=1),
                  sort_by: str = Query('created_at'),
                  order: str = Query('desc'),
                  client_service: ClientService =
                  Depends(get_service(ClientService))) -> PaginatedResponse[ClientResponse]:

    clients, count = await client_service.get_all(page, limit, dict(request.query_params), sort_by, order)
    return get_ok([ClientResponse(**client.to_dict()) for client in clients], {'total_count': count})


@clients_router.post('')
async def create_client(request: CreateClientsRequest,
                        client_service: ClientService =
                        Depends(get_service(ClientService))) -> MainResponse[ClientResponse]:
    client = Client.from_dict(request.model_dump())
    db_client = await client_service.create(client)
    return get_ok(ClientResponse(**db_client.to_dict()))


@clients_router.get('/{id}')
async def get_client_by_id(id: UUID4, client_service: ClientService =
                           Depends(get_service(ClientService))) -> MainResponse[ClientResponse]:
    if db_client := await client_service.get_by_id(id):
        return get_ok(ClientResponse(**db_client.to_dict()))
    return get_not_found()


@clients_router.patch('/{id}')
async def update_client(id: UUID4, request: UpdateClientsRequest, client_service: ClientService =
                        Depends(get_service(ClientService))) -> MainResponse[ClientResponse]:
    if _ := await client_service.get_by_id(id):
        db_client: Client = await client_service.update(id, request.model_dump(exclude_unset=True))
        return get_ok(ClientResponse(**db_client.to_dict()))
    return get_not_found()


@clients_router.delete('/{id}')
async def delete_client(id: UUID4, client_service: ClientService =
                        Depends(get_service(ClientService))) -> MainResponse[ClientResponse]:

    if _ := await client_service.get_by_id(id):
        db_client: Client = await client_service.delete(id)
        return get_ok(ClientResponse(**db_client.to_dict()))
    return get_not_found()
