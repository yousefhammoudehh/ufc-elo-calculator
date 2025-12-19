from datetime import date
from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities.event import Event
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class EventService(BaseService):
    @with_uow
    async def get_all(self, uow: UnitOfWork, page: int = 1, limit: int = 100) -> list[Event]:
        rows, _ = await uow.events.get_paginated(page=page, limit=limit, sort_by='event_date', order='asc')
        return rows

    @with_uow
    async def create(self, uow: UnitOfWork, event: Event) -> Event:
        return await uow.events.add(event)

    @with_uow
    async def get_by_event_id(self, uow: UnitOfWork, event_id: UUID) -> Event | None:
        return await uow.events.get_by_event_id(event_id)

    @with_uow
    async def list_by_date(self, uow: UnitOfWork, event_date: date) -> list[Event]:
        return await uow.events.get_by_date(event_date)

    @with_uow
    async def get_by_link(self, uow: UnitOfWork, event_link: str) -> Event | None:
        return await uow.events.get_by_event_link(event_link)
