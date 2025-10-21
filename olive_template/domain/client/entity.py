
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from olive_template.domain.shared.BaseEntity import BaseEntity


@dataclass
class Client(BaseEntity):
    name: str
    code: str
    contact_person_name: str
    contact_person_email: str
    country_id: UUID
    address: str
    phone_number: str
    description: str | None
    logo_url: str | None
    auth0_id: str | None
    metadata: dict[str, Any]
