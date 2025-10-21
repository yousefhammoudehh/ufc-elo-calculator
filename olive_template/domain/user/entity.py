from dataclasses import dataclass
from uuid import UUID, uuid4


@dataclass
class User:
    auth0_id: str
    name: str
    email: str
    auth0_org_id: str
    permissions: list[str]
    access_token: str
    id_token: str
    client_id: UUID = uuid4()
    id: UUID = uuid4()
