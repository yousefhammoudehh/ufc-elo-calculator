from sqlalchemy import Boolean, CheckConstraint, Column, Date, ForeignKey, Integer, Numeric, String, Table, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import text

from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.database.engine import metadata

# 1. Promotions
promotions = Table(
    'promotions',
    metadata,
    Column('promotion_id', UUID(as_uuid=False), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('name', String, nullable=False),
    Column('link', Text),
    Column('strength', Numeric(5, 2)),
)

# 2. Fighters
fighters = Table(
    'fighters',
    metadata,
    Column('fighter_id', String(16), primary_key=True),
    Column('name', String, nullable=False),
    Column('entry_elo', Integer),
    Column('current_elo', Integer),
    Column('peak_elo', Integer),
)

# 3. Events
events = Table(
    'events',
    metadata,
    Column('event_id', UUID(as_uuid=False), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('event_date', Date, nullable=False),
    Column('name', String),
)

# 4. Bouts
bouts = Table(
    'bouts',
    metadata,
    Column('bout_id', String(16), primary_key=True),
    Column('event_id', UUID(as_uuid=False), ForeignKey('events.event_id')),
    Column('is_title_fight', Boolean),
    Column('method', String),
    Column('round_num', Integer),
    Column('time_sec', Integer),
    Column('time_format', String),
)

# 5. Bout Participants
bout_participants = Table(
    'bout_participants',
    metadata,
    Column('bout_id', String(16), ForeignKey('bouts.bout_id'), primary_key=True),
    Column('fighter_id', String(16), ForeignKey('fighters.fighter_id'), primary_key=True),
    Column('outcome', String, CheckConstraint(f'outcome IN {tuple(o.value for o in FightOutcome)}'), nullable=False),
    Column('kd', Integer),
    Column('sig_strikes', Integer),
    Column('sig_strikes_thrown', Integer),
    Column('total_strikes', Integer),
    Column('total_strikes_thrown', Integer),
    Column('td', Integer),
    Column('td_attempts', Integer),
    Column('sub_attempts', Integer),
    Column('reversals', Integer),
    Column('control_time_sec', Integer),
    Column('head_ss', Integer),
    Column('body_ss', Integer),
    Column('leg_ss', Integer),
    Column('distance_ss', Integer),
    Column('clinch_ss', Integer),
    Column('ground_ss', Integer),
    Column('strike_accuracy', Numeric(5, 2)),
    Column('elo_before', Integer),
    Column('elo_after', Integer),
    Column('ufc_fights_before', Integer),
    Column('days_since_last_fight', Integer),
)

# 6. Judge Scores
judge_scores = Table(
    'judge_scores',
    metadata,
    Column('bout_id', String(16), ForeignKey('bouts.bout_id'), primary_key=True),
    Column('fighter_id', String(16), ForeignKey('fighters.fighter_id'), primary_key=True),
    Column('judge1_score', Integer),
    Column('judge2_score', Integer),
    Column('judge3_score', Integer),
)

# 7. Pre-UFC Bouts
pre_ufc_bouts = Table(
    'pre_ufc_bouts',
    metadata,
    Column('bout_id', UUID(as_uuid=False), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('fighter_id', String(16), ForeignKey('fighters.fighter_id')),
    Column('promotion_id', UUID(as_uuid=False), ForeignKey('promotions.promotion_id')),
    Column('result', String, CheckConstraint(f'result IN {tuple(r.value for r in FightOutcome)}')),
)
