"""Add event ingestion flags and stats link

Revision ID: 20251026_add_event_seed_counts
Revises: 20251024_make_elo_float
Create Date: 2025-10-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251026_add_event_seed_counts'
down_revision = '20251024_make_elo_float'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('events', sa.Column('fighters_seeded', sa.Boolean(), server_default=sa.text('false'), nullable=True))
    op.add_column('events', sa.Column('fights_seeded', sa.Boolean(), server_default=sa.text('false'), nullable=True))
    op.add_column('events', sa.Column('event_stats_link', sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column('events', 'event_stats_link')
    op.drop_column('events', 'fights_seeded')
    op.drop_column('events', 'fighters_seeded')

