"""Make promotion strength a float

Revision ID: 20251028_make_strength_float
Revises: 20251028_add_bout_weight_class
Create Date: 2025-10-28
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251028_make_strength_float'
down_revision = '20251028_add_bout_weight_class'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Alter promotions.strength from NUMERIC(5,2) to FLOAT (double precision)
    op.alter_column(
        'promotions',
        'strength',
        type_=sa.Float(),
        existing_type=sa.Numeric(5, 2),
        postgresql_using='strength::double precision',
    )


def downgrade() -> None:
    # Revert to NUMERIC(5,2), rounding as needed
    op.alter_column(
        'promotions',
        'strength',
        type_=sa.Numeric(5, 2),
        existing_type=sa.Float(),
        postgresql_using='ROUND(strength::numeric, 2)',
    )
