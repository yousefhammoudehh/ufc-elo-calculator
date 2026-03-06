"""Rating-related domain entities and value objects."""

from dataclasses import dataclass

from elo_calculator.domain.entities.base_entity import BaseEntity


@dataclass(slots=True)
class CurrentRanking(BaseEntity):
    """One row in the current-rankings leaderboard."""

    system_key: str = ''
    division_key: str = ''
    division_display_name: str | None = None
    sex: str = 'U'
    as_of_date: str = ''
    fighter_id: str = ''
    display_name: str = ''
    rank: int = 0
    rating_mean: float = 0.0
    rd: float | None = None
    last_fight_date: str | None = None


@dataclass(slots=True)
class RatingTimeseriesPoint:
    """Single data-point in a fighter's rating history."""

    date: str = ''
    rating_mean: float = 0.0
    rd: float | None = None


@dataclass(slots=True)
class FighterRatingProfile:
    """A fighter's current rating for one system (used in profile views)."""

    system_key: str = ''
    rating_mean: float = 0.0
    rd: float | None = None
    peak_rating: float = 0.0
