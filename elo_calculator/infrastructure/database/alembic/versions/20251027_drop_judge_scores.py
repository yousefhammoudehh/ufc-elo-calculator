"""Drop judge_scores table

Revision ID: 20251027_drop_judge_scores
Revises: 20251026_add_event_seed_counts
Create Date: 2025-10-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251027_drop_judge_scores'
down_revision = '20251026_add_event_seed_counts'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Use raw SQL to avoid failure if the table doesn't exist and to cascade FKs
    op.execute('DROP TABLE IF EXISTS judge_scores CASCADE')


def downgrade() -> None:
    # Recreate judge_scores table as originally defined
    op.create_table(
        'judge_scores',
        sa.Column('bout_id', sa.String(length=16), nullable=False),
        sa.Column('fighter_id', sa.String(length=16), nullable=False),
        sa.Column('judge1_score', sa.Integer(), nullable=True),
        sa.Column('judge2_score', sa.Integer(), nullable=True),
        sa.Column('judge3_score', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['bout_id'], ['bouts.bout_id']),
        sa.ForeignKeyConstraint(['fighter_id'], ['fighters.fighter_id']),
        sa.PrimaryKeyConstraint('bout_id', 'fighter_id'),
    )
