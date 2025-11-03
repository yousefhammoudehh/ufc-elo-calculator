"""add event name column

Revision ID: 20251030_add_event_name
Revises: 20251028_make_strength_float
Create Date: 2025-10-30
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251030_add_event_name'
down_revision = '20251028_make_strength_float'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('events', sa.Column('name', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('events', 'name')

