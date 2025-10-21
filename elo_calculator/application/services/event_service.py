import uuid

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities.event import Event
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class EventService(BaseService):
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    @with_uow
    async def create_event(self, event: Event) -> Event:
        async with self.uow:
            created_event = await self.uow.events.add(event)
            await self.uow.commit()
            return created_event

    async def get_event(self, event_id: uuid.UUID) -> Event | None:
        async with self.uow:
            return await self.uow.events.get(event_id)

    async def update_event(self, event_id: uuid.UUID, **kwargs) -> Event | None:
        async with self.uow:
            updated_event = await self.uow.events.update(event_id, **kwargs)
            await self.uow.commit()
            return updated_event

    async def delete_event(self, event_id: uuid.UUID) -> bool:
        async with self.uow:
            result = await self.uow.events.delete(event_id)
            await self.uow.commit()
            return result

    async def list_events(self) -> list[Event]:
        async with self.uow:
            return await self.uow.events.list()
