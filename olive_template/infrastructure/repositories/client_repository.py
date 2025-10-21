from typing import List

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncConnection

from olive_template.domain.client.entity import Client
from olive_template.infrastructure.database.schema import clients
from olive_template.infrastructure.repositories.base_repository import BaseRepository


class ClientRepository(BaseRepository[Client]):
    def __init__(self, connection: AsyncConnection):
        super().__init__(connection, Client, clients)

    async def get_by_code_or_contact_email(self, code: str, contact_email: str) -> List[Client]:
        cmd = select(clients).where(or_(clients.c.code == code,
                                        clients.c.contact_person_email == contact_email))

        result = await self.connection.execute(cmd)
        return [Client.from_dict(dict(row._mapping)) for row in result.all()]
