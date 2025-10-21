"""add_uuid_ossp

Revision ID: d670ccb3cf50
Revises: 
Create Date: 2024-12-03 07:39:01.109840

"""
from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd670ccb3cf50'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')


def downgrade() -> None:
    op.execute('DROP EXTENSION IF EXISTS "uuid-ossp";')
