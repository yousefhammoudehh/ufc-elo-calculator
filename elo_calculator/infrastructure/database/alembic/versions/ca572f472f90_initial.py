"""Initial

Revision ID: ca572f472f90
Revises:
Create Date: 2026-02-27 03:34:01.823035

"""

from collections.abc import Sequence

from alembic import op

from elo_calculator.infrastructure.database import schema as _schema  # noqa: F401
from elo_calculator.infrastructure.database.engine import metadata

# revision identifiers, used by Alembic.
revision: str = 'ca572f472f90'
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')
    bind = op.get_bind()
    metadata.create_all(bind=bind)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    metadata.drop_all(bind=bind)
