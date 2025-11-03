from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from elo_calculator.domain.shared.enumerations import FightOutcome


@dataclass
class ScrapedEvent:
    """Minimal event details scraped from an external site.

    We deliberately keep this as a simple value object, separate from persistence entities.
    """

    event_link: str
    event_date: date | None = None
    title: str | None = None


@dataclass
class ScrapedFighter:
    """Minimal fighter info scraped from an external site for ingestion/use-case mapping."""

    name: str | None
    fighter_link: str


@dataclass
class ScrapedPromotion:
    name: str
    link: str | None = None


@dataclass
class ScrapedPreUfcBout:
    """Pre-UFC bout details collected from a fighter page.

    We keep this VO rich even if persistence currently stores a subset; future enrichment can consume more fields.
    """

    result: FightOutcome | None = None
    promotion: ScrapedPromotion | None = None
    bout_date: date | None = None
    opponent_name: str | None = None
    method: str | None = None
    rounds: int | None = None


@dataclass
class ScrapedFighterProfile:
    tapology_link: str
    name: str | None = None
    stats_link: str | None = None
    pre_ufc_bouts: list[ScrapedPreUfcBout] | None = None

    def iter_pre_ufc_bouts(self) -> Iterable[ScrapedPreUfcBout]:
        return self.pre_ufc_bouts or []


# --- UFCStats fight scraping models ---


@dataclass
class ScrapedFightFighter:
    """Per-fighter stats for a single UFC bout as scraped from UFCStats."""

    name: str | None = None
    fighter_id: str | None = None  # UFCStats fighter-details id token
    result: str | None = None  # 'W' | 'L' | 'D' | 'NC'
    # General stats
    kd: int | None = None
    sig_strikes: int | None = None
    sig_strikes_thrown: int | None = None
    total_strikes: int | None = None
    total_strikes_thrown: int | None = None
    td: int | None = None
    td_attempts: int | None = None
    td_percent: float | None = None
    sub_attempts: int | None = None
    rev: int | None = None
    ctrl: int | None = None  # seconds
    # Significant strikes breakdown
    head_ss: int | None = None
    body_ss: int | None = None
    leg_ss: int | None = None
    distance_ss: int | None = None
    clinch_ss: int | None = None
    ground_ss: int | None = None
    # Derived
    sig_strike_percent: float | None = None  # 0..100
    strike_accuracy: float | None = None  # 0..1


@dataclass
class ScrapedFight:
    """Full fight payload scraped from a UFCStats fight page."""

    fight_id: str
    method: str
    round_num: int | None
    time_sec: int | None
    time_format: str | None
    event_date: date | None
    fighter1: ScrapedFightFighter
    fighter2: ScrapedFightFighter
    is_title_fight: bool
    weight_class_code: int | None = None
