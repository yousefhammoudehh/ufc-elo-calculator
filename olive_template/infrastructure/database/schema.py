from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, String, Table, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from olive_template.infrastructure.database.engine import metadata

clients = Table(
    'clients',
    metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, server_default=text('uuid_generate_v4()')),
    Column('name', String, nullable=False),
    Column('code', String, nullable=False, unique=True),
    Column('contact_person_name', String, nullable=False),
    Column('contact_person_email', String, nullable=False, unique=True),
    Column('country_id', UUID(as_uuid=True), nullable=False),
    Column('address', String, nullable=False),
    Column('phone_number', String, nullable=False),
    Column('description', String, nullable=True),
    Column('logo_url', String, nullable=True),
    Column('auth0_id', String, nullable=True),
    Column('metadata', JSONB, nullable=True),
    Column('created_by', UUID(as_uuid=True), nullable=True),
    Column('created_at', DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
    Column('updated_by', UUID(as_uuid=True), nullable=True),
    Column('updated_at', DateTime(timezone=True), nullable=True, onupdate=lambda: datetime.now(timezone.utc)),
)
