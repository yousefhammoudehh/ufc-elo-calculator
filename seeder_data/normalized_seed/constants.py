from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from elo_calculator.domain.shared.enumerations import FighterGenderEnum, WeightUnitEnum


@dataclass(frozen=True)
class WeightClassParseResult:
    sport_key: str
    raw_weight_class: str
    sex: FighterGenderEnum
    division_key: str
    display_name: str
    limit_lbs: Decimal | None
    lbs_min: Decimal | None
    lbs_max: Decimal | None
    is_openweight: bool
    is_canonical_mma: bool
    mma_division_key: str | None
    weight_limit_unit: WeightUnitEnum | None
    weight_limit_value: Decimal | None
    weight_limit_lbs: Decimal | None
    is_catchweight: bool
    parse_confidence: Decimal
    notes: str | None


@dataclass(frozen=True)
class RulesetNormalized:
    key: str
    sport_key: str
    rounds_scheduled: int
    round_seconds: int
    judging_standard: str


MMA_DIVISION_LIMITS_LBS: dict[str, Decimal] = {
    'ATOM': Decimal('105'),
    'STRAW': Decimal('115'),
    'FLY': Decimal('125'),
    'BW': Decimal('135'),
    'FW': Decimal('145'),
    'LW': Decimal('155'),
    'WW': Decimal('170'),
    'MW': Decimal('185'),
    'LHW': Decimal('205'),
    'HW': Decimal('265'),
}

MMA_DIVISION_BOUNDS_LBS: dict[str, tuple[Decimal, Decimal]] = {
    'ATOM': (Decimal('0'), Decimal('105')),
    'STRAW': (Decimal('106'), Decimal('115')),
    'FLY': (Decimal('116'), Decimal('125')),
    'BW': (Decimal('126'), Decimal('135')),
    'FW': (Decimal('136'), Decimal('145')),
    'LW': (Decimal('146'), Decimal('155')),
    'WW': (Decimal('156'), Decimal('170')),
    'MW': (Decimal('171'), Decimal('185')),
    'LHW': (Decimal('186'), Decimal('205')),
    'HW': (Decimal('206'), Decimal('265')),
    'SHW': (Decimal('266'), Decimal('2000')),
}

MMA_DIVISION_ORDER = ('ATOM', 'STRAW', 'FLY', 'BW', 'FW', 'LW', 'WW', 'MW', 'LHW', 'HW')

CLASSIC_MMA_LIMITS_LBS = {
    Decimal('105'),
    Decimal('115'),
    Decimal('125'),
    Decimal('135'),
    Decimal('145'),
    Decimal('155'),
    Decimal('170'),
    Decimal('185'),
    Decimal('205'),
    Decimal('265'),
}
