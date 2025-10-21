from datetime import datetime
from typing import Annotated, Any, Generic, TypeVar

from pydantic import UUID4, AfterValidator, BaseModel, EmailStr, HttpUrl

T = TypeVar('T')


class DataModel(BaseModel):
    pass


class MainResponse(BaseModel, Generic[T]):
    message: str
    data: T | list[T]
    errors: list[dict[str, Any]] | dict[str, Any] | list[str] | str | None


class PaginatedResponse(MainResponse[T]):
    total_count: int


class ClientResponse(DataModel):
    id: UUID4
    name: str
    code: str
    contact_person_email: str
    address: str
    country_id: UUID4
    contact_person_name: str
    phone_number: str
    description: str | None
    logo_url: str
    metadata: dict[str, Any] | None
    auth0_id: str | None
    created_at: datetime
    updated_at: datetime | None
    created_by: UUID4 | None
    updated_by: UUID4 | None


class CreateClientsRequest(DataModel):
    name: str
    code: str
    contact_person_name: str
    contact_person_email: EmailStr
    country_id: UUID4
    address: str
    phone_number: str
    description: str | None = None
    logo_url: Annotated[HttpUrl, AfterValidator(str)]
    metadata: dict[str, Any] | None


class UpdateClientsRequest(DataModel):
    name: str | None = None
    code: str | None = None
    contact_person_email: str | None = None
    address: str | None = None
    country_id: UUID4 | None = None
    contact_person_name: str | None = None
    phone_number: str | None = None
    description: str | None = None
    logo_url: str | None = None
    metadata: dict[str, Any] | None = None
