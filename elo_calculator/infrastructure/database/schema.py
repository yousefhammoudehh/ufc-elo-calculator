from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import text

from elo_calculator.domain.shared.enumerations import (
    BoutCornerEnum,
    BoutResultEnum,
    DecisionTypeEnum,
    FighterGenderEnum,
    MethodGroupEnum,
    SourceSystemEnum,
    StrEnum,
    WeightUnitEnum,
)
from elo_calculator.infrastructure.database.engine import metadata

# ---------------------------------
# 2.1 Canonical IDs and Dimensions
# ---------------------------------

dim_sport_table = Table(
    'dim_sport',
    metadata,
    Column('sport_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('sport_key', String(64), nullable=False, unique=True),
    Column('is_mma', Boolean, nullable=False, server_default=text('false')),
    Column('translation_to_mma', Numeric(6, 4)),
)

dim_promotion_table = Table(
    'dim_promotion',
    metadata,
    Column('promotion_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('promotion_name', Text, nullable=False),
    Column('tapology_promotion_id', BigInteger, index=True),
    Column('tapology_promotion_url', Text),
    Column('tapology_slug', String(255)),
    Column('strength', Numeric(6, 4)),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id')),
)
Index(
    'ux_dim_promotion_tapology_slug',
    dim_promotion_table.c.tapology_slug,
    unique=True,
    postgresql_where=dim_promotion_table.c.tapology_slug.is_not(None),
)

dim_division_table = Table(
    'dim_division',
    metadata,
    Column('division_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id'), nullable=False),
    Column('division_key', String(64), nullable=False),
    Column('display_name', String(128)),
    Column('sex', StrEnum(FighterGenderEnum, length=4), nullable=False, server_default=text("'U'")),
    Column('limit_lbs', Numeric(8, 2)),
    Column('lbs_min', Numeric(8, 2)),
    Column('lbs_max', Numeric(8, 2)),
    Column('is_openweight', Boolean, nullable=False, server_default=text('false')),
    Column('is_canonical_mma', Boolean, nullable=False, server_default=text('false')),
)

Index(
    'ux_dim_division',
    dim_division_table.c.sport_id,
    dim_division_table.c.division_key,
    dim_division_table.c.sex,
    unique=True,
)

dim_weight_limit_table = Table(
    'dim_weight_limit',
    metadata,
    Column('weight_limit_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('unit', StrEnum(WeightUnitEnum, length=8), nullable=False),
    Column('value', Numeric(8, 2), nullable=False),
    Column('weight_lbs', Numeric(8, 2), nullable=False),
    CheckConstraint("unit IN ('lb','kg')", name='ck_dim_weight_limit_unit'),
)

Index('ux_dim_weight_limit_unit_value', dim_weight_limit_table.c.unit, dim_weight_limit_table.c.value, unique=True)

bridge_weight_class_source_table = Table(
    'bridge_weight_class_source',
    metadata,
    Column('bridge_weight_class_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('source_key', String(32), nullable=False),
    Column('raw_weight_class', Text, nullable=False),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id'), nullable=False),
    Column('promotion_id', UUID(as_uuid=True), ForeignKey('dim_promotion.promotion_id')),
    Column('division_id', UUID(as_uuid=True), ForeignKey('dim_division.division_id')),
    Column('weight_limit_id', UUID(as_uuid=True), ForeignKey('dim_weight_limit.weight_limit_id')),
    Column('is_catchweight', Boolean, nullable=False, server_default=text('false')),
    Column('is_openweight', Boolean, nullable=False, server_default=text('false')),
    Column('parse_confidence', Numeric(4, 3), nullable=False, server_default=text('0.500')),
    Column('notes', Text),
)

Index(
    'ux_bridge_weight_class_source',
    bridge_weight_class_source_table.c.source_key,
    bridge_weight_class_source_table.c.sport_id,
    bridge_weight_class_source_table.c.promotion_id,
    bridge_weight_class_source_table.c.raw_weight_class,
    unique=True,
)

dim_ruleset_table = Table(
    'dim_ruleset',
    metadata,
    Column('ruleset_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('ruleset_key', String(64), nullable=False, unique=True),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id')),
    Column('rounds_scheduled', SmallInteger, nullable=False),
    Column('round_seconds', SmallInteger, nullable=False, server_default=text('300')),
    Column('judging_standard', Text, nullable=False, server_default=text("'ABC_UNIFIED'")),
)

dim_result_ontology_table = Table(
    'dim_result_ontology',
    metadata,
    Column('result_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('outcome_key', StrEnum(BoutResultEnum, length=8), nullable=False),
    Column('method_group', StrEnum(MethodGroupEnum, length=16), nullable=False),
    Column('decision_type', StrEnum(DecisionTypeEnum, length=8)),
    Column('is_finish', Boolean, nullable=False, server_default=text('false')),
)

Index(
    'ux_result_ontology',
    dim_result_ontology_table.c.outcome_key,
    dim_result_ontology_table.c.method_group,
    text("COALESCE(decision_type, '')"),
    unique=True,
)

dim_fighter_table = Table(
    'dim_fighter',
    metadata,
    Column('fighter_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('display_name', Text, nullable=False),
    Column('nickname', Text),
    Column('dob_raw', String(64)),
    Column('birth_date', Date),
    Column('birth_place', Text),
    Column('country_code', String(8), nullable=False, server_default=text("'UNK'")),
    Column('fighting_out_of', Text),
    Column('affiliation_gym', Text),
    Column('foundation_style', Text),
    Column('profile_image_url', Text),
    Column('record_raw', Text),
    Column('tapology_slug', String(255), index=True),
    Column('height_in', Numeric(6, 2)),
    Column('height_cm', Numeric(6, 2)),
    Column('reach_in', Numeric(6, 2)),
    Column('reach_cm', Numeric(6, 2)),
    Column('stance', String(64)),
    Column('sex', StrEnum(FighterGenderEnum, length=4), nullable=False, server_default=text("'U'")),
    Column('source_fetched_at', DateTime(timezone=True)),
    Column('source_file', Text),
    Column('source_meta_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)

bridge_fighter_source_table = Table(
    'bridge_fighter_source',
    metadata,
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id')),
    Column('source_key', StrEnum(SourceSystemEnum, length=16), primary_key=True),
    Column('source_fighter_id', String(255), primary_key=True),
    Column('source_slug', String(255)),
    Column('fetched_at', DateTime(timezone=True)),
    Column('source_file', Text),
    Column('source_metadata_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)

Index(
    'ux_bridge_fighter_source_fighter_source',
    bridge_fighter_source_table.c.fighter_id,
    bridge_fighter_source_table.c.source_key,
    unique=True,
)

# ---------------------------------
# 2.2 Canonical Events/Bouts/Rounds
# ---------------------------------

fact_event_ufcstats_table = Table(
    'fact_event_ufcstats',
    metadata,
    Column('ufcstats_event_fact_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('ufcstats_event_id', String(16), nullable=False, index=True),
    Column('event_date', Date, nullable=False, index=True),
    Column('event_name', Text, nullable=False),
    Column('ufcstats_event_url', Text),
    Column('ufcstats_event_uuid', UUID(as_uuid=True)),
    Column('num_fights', SmallInteger),
    Column('location', Text),
    Column('fetched_at', DateTime(timezone=True)),
    Column('source_file', Text),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

Index('ux_fact_event_ufcstats_source_event_id', fact_event_ufcstats_table.c.ufcstats_event_id, unique=True)
Index('ix_fact_event_ufcstats_date', fact_event_ufcstats_table.c.event_date)

fact_event_tapology_table = Table(
    'fact_event_tapology',
    metadata,
    Column('tapology_event_fact_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('tapology_event_slug', String(255), nullable=False, index=True),
    Column('event_date', Date, nullable=False, index=True),
    Column('event_date_raw', Text),
    Column('event_name', Text, nullable=False),
    Column('tapology_event_url', Text),
    Column('promotion_tapology_slug', String(255)),
    Column('promotion_name', Text),
    Column('source_file', Text),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

Index('ux_fact_event_tapology_source_event_slug', fact_event_tapology_table.c.tapology_event_slug, unique=True)
Index('ix_fact_event_tapology_date', fact_event_tapology_table.c.event_date)

bridge_event_source_table = Table(
    'bridge_event_source',
    metadata,
    Column('ufcstats_event_id', String(16), primary_key=True),
    Column('tapology_event_slug', String(255), nullable=False),
    Column('ufcstats_event_fact_id', UUID(as_uuid=True), ForeignKey('fact_event_ufcstats.ufcstats_event_fact_id')),
    Column('tapology_event_fact_id', UUID(as_uuid=True), ForeignKey('fact_event_tapology.tapology_event_fact_id')),
    Column('checkpoint_event_id', UUID(as_uuid=True), ForeignKey('fact_event.event_id')),
    Column('mapped_bout_count', Integer, nullable=False, server_default=text('0')),
    Column('confidence', Numeric(6, 4), nullable=False, server_default=text('0.0')),
    Column('source_file', Text),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

Index('ux_bridge_event_source_tapology_event_slug', bridge_event_source_table.c.tapology_event_slug, unique=True)

fact_event_table = Table(
    'fact_event',
    metadata,
    Column('event_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('event_date', Date, nullable=False, index=True),
    Column('event_date_raw', Text),
    Column('event_name', Text, nullable=False),
    Column('promotion_id', UUID(as_uuid=True), ForeignKey('dim_promotion.promotion_id')),
    Column('tapology_event_slug', String(255)),
    Column('tapology_event_url', Text),
    Column('ufcstats_event_id', String(16)),
    Column('ufcstats_event_url', Text),
    Column('ufcstats_event_uuid', UUID(as_uuid=True)),
    Column('num_fights', SmallInteger),
    Column('location', Text),
    Column('fetched_at', DateTime(timezone=True)),
    Column('source_file', Text),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

Index('ix_event_promo_date', fact_event_table.c.promotion_id, fact_event_table.c.event_date)
Index(
    'ux_fact_event_tapology_event_slug',
    fact_event_table.c.tapology_event_slug,
    unique=True,
    postgresql_where=fact_event_table.c.tapology_event_slug.is_not(None),
)
Index(
    'ux_fact_event_ufcstats_event_id',
    fact_event_table.c.ufcstats_event_id,
    unique=True,
    postgresql_where=fact_event_table.c.ufcstats_event_id.is_not(None),
)

fact_bout_table = Table(
    'fact_bout',
    metadata,
    Column('bout_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('event_id', UUID(as_uuid=True), ForeignKey('fact_event.event_id'), nullable=False, index=True),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id'), nullable=False),
    Column('ruleset_id', UUID(as_uuid=True), ForeignKey('dim_ruleset.ruleset_id')),
    Column('division_id', UUID(as_uuid=True), ForeignKey('dim_division.division_id')),
    Column('mma_division_id', UUID(as_uuid=True), ForeignKey('dim_division.division_id')),
    Column('weight_limit_id', UUID(as_uuid=True), ForeignKey('dim_weight_limit.weight_limit_id')),
    Column('weight_class_raw', Text),
    Column('weight_lbs', Numeric(6, 2)),
    Column('catch_weight_lbs', Numeric(8, 2)),
    Column('catch_delta_lbs', Numeric(8, 2)),
    Column('is_catchweight', Boolean, nullable=False, server_default=text('false')),
    Column('is_openweight', Boolean, nullable=False, server_default=text('false')),
    Column('is_amateur', Boolean, nullable=False, server_default=text('false')),
    Column('is_title_fight', Boolean, nullable=False, server_default=text('false')),
    Column('scheduled_rounds', SmallInteger),
    Column('tapology_bout_ref', String(64), index=True),
    Column('ufcstats_fight_id', String(16), index=True),
    Column('winner_fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id')),
    Column('referee', Text),
    Column('tapology_method_raw', String(32)),
    Column('tapology_method_details_raw', Text),
    Column('ufcstats_method_raw', String(64)),
    Column('method_group', StrEnum(MethodGroupEnum, length=16)),
    Column('decision_type', StrEnum(DecisionTypeEnum, length=8)),
    Column('finish_round', SmallInteger),
    Column('finish_time_seconds', SmallInteger),
    Column('finish_time_total_seconds', Integer),
    Column('fetched_at', DateTime(timezone=True)),
    Column('source_file', Text),
    Column('source_meta_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

Index('ix_bout_sport_date', fact_bout_table.c.sport_id, fact_bout_table.c.event_id)
Index(
    'ux_fact_bout_tapology_bout_ref',
    fact_bout_table.c.tapology_bout_ref,
    unique=True,
    postgresql_where=fact_bout_table.c.tapology_bout_ref.is_not(None),
)
Index(
    'ux_fact_bout_ufcstats_fight_id',
    fact_bout_table.c.ufcstats_fight_id,
    unique=True,
    postgresql_where=fact_bout_table.c.ufcstats_fight_id.is_not(None),
)
Index('ix_fact_bout_division', fact_bout_table.c.division_id)
Index('ix_fact_bout_mma_division', fact_bout_table.c.mma_division_id)
Index('ix_fact_bout_weight_limit', fact_bout_table.c.weight_limit_id)

fact_bout_participant_table = Table(
    'fact_bout_participant',
    metadata,
    Column('bout_id', UUID(as_uuid=True), ForeignKey('fact_bout.bout_id'), primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('corner', StrEnum(BoutCornerEnum, length=16), nullable=False, server_default=text("'unknown'")),
    Column('outcome_key', StrEnum(BoutResultEnum, length=8), nullable=False),
    Column('result_id', UUID(as_uuid=True), ForeignKey('dim_result_ontology.result_id')),
    Column('opponent_name', Text),
    Column('opponent_source_slug', String(255)),
    Column('prefight_record', Text),
    Column('opponent_prefight_record', Text),
    Column('fetched_at', DateTime(timezone=True)),
    Column('source_file', Text),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
    CheckConstraint("corner IN ('red', 'blue', 'unknown')", name='ck_fact_bout_participant_corner'),
)

Index('ix_participant_fighter', fact_bout_participant_table.c.fighter_id)
Index('ix_participant_bout', fact_bout_participant_table.c.bout_id)

fact_round_table = Table(
    'fact_round',
    metadata,
    Column('bout_id', UUID(as_uuid=True), ForeignKey('fact_bout.bout_id'), primary_key=True),
    Column('round_num', SmallInteger, primary_key=True),
    Column('round_seconds', SmallInteger, nullable=False, server_default=text('300')),
    Column('completed', Boolean, nullable=False, server_default=text('true')),
)

# -----------------------------
# 2.3 Round/Fight Stat Artifacts
# -----------------------------

fact_round_stats_table = Table(
    'fact_round_stats',
    metadata,
    Column('bout_id', UUID(as_uuid=True), primary_key=True),
    Column('round_num', SmallInteger, primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('kd', SmallInteger),
    Column('sig_landed', SmallInteger),
    Column('sig_attempted', SmallInteger),
    Column('total_landed', SmallInteger),
    Column('total_attempted', SmallInteger),
    Column('td_landed', SmallInteger),
    Column('td_attempted', SmallInteger),
    Column('sig_raw', String(32)),
    Column('total_raw', String(32)),
    Column('td_raw', String(32)),
    Column('sub_attempts', SmallInteger),
    Column('rev', SmallInteger),
    Column('ctrl_seconds', SmallInteger),
    Column('fetched_at', DateTime(timezone=True)),
    ForeignKeyConstraint(
        ['bout_id', 'round_num'], ['fact_round.bout_id', 'fact_round.round_num'], name='fk_fact_round_stats_round'
    ),
)

Index('ix_round_stats_fighter', fact_round_stats_table.c.fighter_id, fact_round_stats_table.c.bout_id)
Index('ix_round_stats_round_key', fact_round_stats_table.c.bout_id, fact_round_stats_table.c.round_num)

fact_round_sig_by_target_table = Table(
    'fact_round_sig_by_target',
    metadata,
    Column('bout_id', UUID(as_uuid=True), primary_key=True),
    Column('round_num', SmallInteger, primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('head_landed', SmallInteger),
    Column('head_attempted', SmallInteger),
    Column('head_raw', String(32)),
    Column('body_landed', SmallInteger),
    Column('body_attempted', SmallInteger),
    Column('body_raw', String(32)),
    Column('leg_landed', SmallInteger),
    Column('leg_attempted', SmallInteger),
    Column('leg_raw', String(32)),
    Column('fetched_at', DateTime(timezone=True)),
    ForeignKeyConstraint(
        ['bout_id', 'round_num'], ['fact_round.bout_id', 'fact_round.round_num'], name='fk_fact_round_sig_target_round'
    ),
)

fact_round_sig_by_position_table = Table(
    'fact_round_sig_by_position',
    metadata,
    Column('bout_id', UUID(as_uuid=True), primary_key=True),
    Column('round_num', SmallInteger, primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('distance_landed', SmallInteger),
    Column('distance_attempted', SmallInteger),
    Column('distance_raw', String(32)),
    Column('clinch_landed', SmallInteger),
    Column('clinch_attempted', SmallInteger),
    Column('clinch_raw', String(32)),
    Column('ground_landed', SmallInteger),
    Column('ground_attempted', SmallInteger),
    Column('ground_raw', String(32)),
    Column('fetched_at', DateTime(timezone=True)),
    ForeignKeyConstraint(
        ['bout_id', 'round_num'],
        ['fact_round.bout_id', 'fact_round.round_num'],
        name='fk_fact_round_sig_position_round',
    ),
)

fact_fight_totals_table = Table(
    'fact_fight_totals',
    metadata,
    Column('bout_id', UUID(as_uuid=True), ForeignKey('fact_bout.bout_id'), primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('kd', SmallInteger),
    Column('sig_landed', SmallInteger),
    Column('sig_attempted', SmallInteger),
    Column('total_landed', SmallInteger),
    Column('total_attempted', SmallInteger),
    Column('td_landed', SmallInteger),
    Column('td_attempted', SmallInteger),
    Column('sig_raw', String(32)),
    Column('total_raw', String(32)),
    Column('td_raw', String(32)),
    Column('sub_attempts', SmallInteger),
    Column('rev', SmallInteger),
    Column('ctrl_seconds', Integer),
    Column('fetched_at', DateTime(timezone=True)),
)

Index('ix_fight_totals_fighter', fact_fight_totals_table.c.fighter_id, fact_fight_totals_table.c.bout_id)

# ----------------------------------------
# 2.4 Computed Artifacts (Ratings + PS)
# ----------------------------------------

dim_rating_system_table = Table(
    'dim_rating_system',
    metadata,
    Column('system_id', UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()')),
    Column('system_key', String(64), nullable=False),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id')),
    Column('description', Text),
    Column('param_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column('code_version', String(64), nullable=False),
    Column('created_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

Index(
    'ux_dim_rating_system_key_version',
    dim_rating_system_table.c.system_key,
    dim_rating_system_table.c.code_version,
    unique=True,
)

fact_rating_snapshot_table = Table(
    'fact_rating_snapshot',
    metadata,
    Column('system_id', UUID(as_uuid=True), ForeignKey('dim_rating_system.system_id'), primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('as_of_date', Date, primary_key=True),
    Column('rating_mean', Numeric(10, 4), nullable=False),
    Column('rating_rd', Numeric(10, 4)),
    Column('rating_vol', Numeric(10, 6)),
    Column('extra_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    postgresql_partition_by='RANGE (as_of_date)',
)

Index(
    'ix_rating_snapshot_fighter_date', fact_rating_snapshot_table.c.fighter_id, fact_rating_snapshot_table.c.as_of_date
)
Index('ix_rating_snapshot_system_date', fact_rating_snapshot_table.c.system_id, fact_rating_snapshot_table.c.as_of_date)

fact_rating_delta_table = Table(
    'fact_rating_delta',
    metadata,
    Column('system_id', UUID(as_uuid=True), ForeignKey('dim_rating_system.system_id'), primary_key=True),
    Column('bout_id', UUID(as_uuid=True), ForeignKey('fact_bout.bout_id'), primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('pre_rating', Numeric(10, 4), nullable=False),
    Column('post_rating', Numeric(10, 4), nullable=False),
    Column('delta_rating', Numeric(10, 4), nullable=False),
    Column('expected_win_prob', Numeric(8, 6)),
    Column('target_score', Numeric(8, 6)),
    Column('k_effective', Numeric(10, 6)),
    Column('tier_used', String(1), nullable=False),
    Column('debug_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    CheckConstraint("tier_used IN ('A', 'B', 'C')", name='ck_fact_rating_delta_tier'),
)

Index('ix_rating_delta_fighter', fact_rating_delta_table.c.fighter_id, fact_rating_delta_table.c.bout_id)
Index('ix_rating_delta_bout', fact_rating_delta_table.c.bout_id)

fact_round_ps_table = Table(
    'fact_round_ps',
    metadata,
    Column('ps_version', String(64), primary_key=True),
    Column('bout_id', UUID(as_uuid=True), ForeignKey('fact_bout.bout_id'), primary_key=True),
    Column('round_num', SmallInteger, primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('ps_round', Numeric(8, 6), nullable=False),
    Column('dmg_index', Numeric(10, 6), nullable=False),
    Column('dom_index', Numeric(10, 6), nullable=False),
    Column('dur_index', Numeric(10, 6), nullable=False),
    Column('ten_nine_p', Numeric(8, 6)),
    Column('ten_eight_p', Numeric(8, 6)),
    Column('ten_seven_p', Numeric(8, 6)),
    Column('debug_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)

Index(
    'ix_round_ps_fighter',
    fact_round_ps_table.c.fighter_id,
    fact_round_ps_table.c.bout_id,
    fact_round_ps_table.c.round_num,
)

fact_fight_ps_table = Table(
    'fact_fight_ps',
    metadata,
    Column('ps_version', String(64), primary_key=True),
    Column('bout_id', UUID(as_uuid=True), ForeignKey('fact_bout.bout_id'), primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('ps_fight', Numeric(8, 6), nullable=False),
    Column('quality_of_win', Numeric(10, 6), nullable=False),
    Column('debug_json', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)

# --------------------------
# 2.5 Serving / API Artifacts
# --------------------------

serving_current_rankings_table = Table(
    'serving_current_rankings',
    metadata,
    Column('system_id', UUID(as_uuid=True), ForeignKey('dim_rating_system.system_id'), primary_key=True),
    Column('sport_id', UUID(as_uuid=True), ForeignKey('dim_sport.sport_id'), primary_key=True),
    Column('division_id', UUID(as_uuid=True), ForeignKey('dim_division.division_id'), primary_key=True),
    Column('as_of_date', Date, primary_key=True),
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('rank', Integer, nullable=False),
    Column('rating_mean', Numeric(10, 4), nullable=False),
    Column('rd', Numeric(10, 4)),
    Column('promotion_id_last', UUID(as_uuid=True), ForeignKey('dim_promotion.promotion_id')),
    Column('last_fight_date', Date),
)

Index(
    'ix_serving_current_rankings_lookup',
    serving_current_rankings_table.c.system_id,
    serving_current_rankings_table.c.sport_id,
    serving_current_rankings_table.c.division_id,
    serving_current_rankings_table.c.as_of_date,
    serving_current_rankings_table.c.rank,
)

serving_fighter_profile_table = Table(
    'serving_fighter_profile',
    metadata,
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('as_of_date', Date, primary_key=True),
    Column('system_id', UUID(as_uuid=True), ForeignKey('dim_rating_system.system_id'), primary_key=True),
    Column('rating_mean', Numeric(10, 4), nullable=False),
    Column('rd', Numeric(10, 4)),
    Column('strike', Numeric(10, 4)),
    Column('grapple', Numeric(10, 4)),
    Column('durability', Numeric(10, 4)),
    Column('peak_rating', Numeric(10, 4)),
    Column('streaks', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column('sos_metrics', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)

serving_fighter_timeseries_table = Table(
    'serving_fighter_timeseries',
    metadata,
    Column('fighter_id', UUID(as_uuid=True), ForeignKey('dim_fighter.fighter_id'), primary_key=True),
    Column('system_id', UUID(as_uuid=True), ForeignKey('dim_rating_system.system_id'), primary_key=True),
    Column('date', Date, primary_key=True),
    Column('rating_mean', Numeric(10, 4), nullable=False),
    Column('rd', Numeric(10, 4)),
)

Index(
    'ix_serving_fighter_timeseries_lookup',
    serving_fighter_timeseries_table.c.fighter_id,
    serving_fighter_timeseries_table.c.system_id,
    serving_fighter_timeseries_table.c.date.desc(),
)

pipeline_watermark_table = Table(
    'pipeline_watermark',
    metadata,
    Column('pipeline_key', String(128), primary_key=True),
    Column('last_processed_event_date', Date, nullable=False),
    Column('last_processed_bout_id', UUID(as_uuid=True)),
    Column('updated_at', DateTime(timezone=True), nullable=False, server_default=text('now()')),
)

CANONICAL_TABLES = (
    dim_sport_table,
    dim_promotion_table,
    dim_division_table,
    dim_weight_limit_table,
    bridge_weight_class_source_table,
    dim_ruleset_table,
    dim_result_ontology_table,
    dim_fighter_table,
    bridge_fighter_source_table,
    fact_event_ufcstats_table,
    fact_event_tapology_table,
    bridge_event_source_table,
    fact_event_table,
    fact_bout_table,
    fact_bout_participant_table,
    fact_round_table,
    fact_round_stats_table,
    fact_round_sig_by_target_table,
    fact_round_sig_by_position_table,
    fact_fight_totals_table,
    dim_rating_system_table,
    fact_rating_snapshot_table,
    fact_rating_delta_table,
    fact_round_ps_table,
    fact_fight_ps_table,
    serving_current_rankings_table,
    serving_fighter_profile_table,
    serving_fighter_timeseries_table,
    pipeline_watermark_table,
)

ALL_TABLES = CANONICAL_TABLES
