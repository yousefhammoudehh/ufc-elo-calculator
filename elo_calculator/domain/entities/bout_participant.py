from dataclasses import dataclass
from decimal import Decimal

from elo_calculator.domain.entities.base_entity import BaseEntityBase
from elo_calculator.domain.shared.enumerations import FightOutcome


@dataclass
class BoutParticipant(BaseEntityBase):
    """Bout participant entity representing a fighter's performance and stats in a bout."""

    bout_id: str
    fighter_id: str
    outcome: FightOutcome
    kd: int | None = None
    sig_strikes: int | None = None
    sig_strikes_thrown: int | None = None
    total_strikes: int | None = None
    total_strikes_thrown: int | None = None
    td: int | None = None
    td_attempts: int | None = None
    sub_attempts: int | None = None
    reversals: int | None = None
    control_time_sec: int | None = None
    head_ss: int | None = None
    body_ss: int | None = None
    leg_ss: int | None = None
    distance_ss: int | None = None
    clinch_ss: int | None = None
    ground_ss: int | None = None
    strike_accuracy: Decimal | None = None
    elo_before: int | None = None
    elo_after: int | None = None
    ufc_fights_before: int | None = None
    days_since_last_fight: int | None = None
