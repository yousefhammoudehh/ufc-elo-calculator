"""
Add weight_class_code to bouts table

Revision ID: 20251028_add_bout_weight_class
Revises: 20251027_drop_judge_scores
Create Date: 2025-10-28
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251028_add_bout_weight_class'
down_revision = '20251027_drop_judge_scores'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('bouts', sa.Column('weight_class_code', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('bouts', 'weight_class_code')
