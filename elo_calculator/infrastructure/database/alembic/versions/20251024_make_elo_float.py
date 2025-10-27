"""Make ELO columns float

Revision ID: 20251024_make_elo_float
Revises: d7e3f3322075
Create Date: 2025-10-24
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251024_make_elo_float'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fighters table
    op.alter_column('fighters', 'entry_elo', type_=sa.Float(), postgresql_using='entry_elo::double precision')
    op.alter_column('fighters', 'current_elo', type_=sa.Float(), postgresql_using='current_elo::double precision')
    op.alter_column('fighters', 'peak_elo', type_=sa.Float(), postgresql_using='peak_elo::double precision')

    # Bout participants
    op.alter_column('bout_participants', 'elo_before', type_=sa.Float(), postgresql_using='elo_before::double precision')
    op.alter_column('bout_participants', 'elo_after', type_=sa.Float(), postgresql_using='elo_after::double precision')


def downgrade() -> None:
    # Revert back to integers
    op.alter_column('fighters', 'entry_elo', type_=sa.Integer(), postgresql_using='round(entry_elo)::int')
    op.alter_column('fighters', 'current_elo', type_=sa.Integer(), postgresql_using='round(current_elo)::int')
    op.alter_column('fighters', 'peak_elo', type_=sa.Integer(), postgresql_using='round(peak_elo)::int')

    op.alter_column('bout_participants', 'elo_before', type_=sa.Integer(), postgresql_using='round(elo_before)::int')
    op.alter_column('bout_participants', 'elo_after', type_=sa.Integer(), postgresql_using='round(elo_after)::int')
