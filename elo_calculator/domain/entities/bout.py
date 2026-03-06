"""Bout domain entities and value objects."""

from dataclasses import dataclass, field

from elo_calculator.domain.entities.base_entity import BaseEntity

# ------------------------------------------------------------------
# Value objects (no identity — just data carried with a Bout)
# ------------------------------------------------------------------


@dataclass(slots=True)
class BoutParticipant:
    """One fighter's participation in a bout."""

    fighter_id: str = ''
    display_name: str = ''
    corner: str = 'unknown'
    outcome_key: str = ''


@dataclass(slots=True)
class FighterBoutStats:
    """Aggregated fight-level totals for one fighter in a bout."""

    fighter_id: str = ''
    kd: int = 0
    sig_landed: int = 0
    sig_attempted: int = 0
    total_landed: int = 0
    total_attempted: int = 0
    td_landed: int = 0
    td_attempted: int = 0
    sub_attempts: int = 0
    ctrl_seconds: int = 0


@dataclass(slots=True)
class BoutRatingDelta:
    """Rating change for one fighter in a bout under one system."""

    fighter_id: str = ''
    system_key: str = ''
    pre_rating: float = 0.0
    post_rating: float = 0.0
    delta_rating: float = 0.0
    expected_win_prob: float | None = None


@dataclass(slots=True)
class BoutFightPS:
    """Performance-score artifacts for one fighter in a bout."""

    fighter_id: str = ''
    ps_fight: float = 0.0
    quality_of_win: float = 0.0


# ------------------------------------------------------------------
# Bout entities
# ------------------------------------------------------------------


@dataclass(slots=True)
class BoutSummary(BaseEntity):
    """Lightweight bout for listings (fight cards, fighter history)."""

    bout_id: str = ''
    event_id: str = ''
    event_date: str = ''
    event_name: str = ''
    division_key: str | None = None
    weight_class_raw: str | None = None
    is_title_fight: bool = False
    method_group: str | None = None
    decision_type: str | None = None
    finish_round: int | None = None
    finish_time_seconds: int | None = None
    participants: list[BoutParticipant] = field(default_factory=list)


@dataclass(slots=True)
class BoutDetail(BaseEntity):
    """Full bout with stats, rating changes and performance scores."""

    bout_id: str = ''
    event_id: str = ''
    event_date: str = ''
    event_name: str = ''
    sport_key: str = ''
    division_key: str | None = None
    weight_class_raw: str | None = None
    is_title_fight: bool = False
    method_group: str | None = None
    decision_type: str | None = None
    finish_round: int | None = None
    finish_time_seconds: int | None = None
    scheduled_rounds: int | None = None
    referee: str | None = None
    participants: list[BoutParticipant] = field(default_factory=list)
    fight_stats: list[FighterBoutStats] = field(default_factory=list)
    rating_changes: list[BoutRatingDelta] = field(default_factory=list)
    performance_scores: list[BoutFightPS] = field(default_factory=list)
