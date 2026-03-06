"""Fighter domain entity — maps to ``dim_fighter``."""

from dataclasses import dataclass

from elo_calculator.domain.entities.base_entity import BaseEntity


@dataclass(slots=True)
class Fighter(BaseEntity):
    """Canonical fighter profile."""

    fighter_id: str = ''
    display_name: str = ''
    nickname: str | None = None
    birth_date: str | None = None
    birth_place: str | None = None
    country_code: str = 'UNK'
    fighting_out_of: str | None = None
    affiliation_gym: str | None = None
    foundation_style: str | None = None
    profile_image_url: str | None = None
    height_cm: float | None = None
    reach_cm: float | None = None
    stance: str | None = None
    sex: str = 'U'
