from enum import Enum, IntEnum


class StrEnum(str, Enum):
    """String-valued Enum base class.

    Inherits from str and Enum so members compare and serialize as plain strings.
    """

    def __str__(self) -> str:  # pragma: no cover - trivial
        return str(self.value)


class FightOutcome(StrEnum):
    """Fight outcome types for UFC bouts."""

    WIN = 'win'
    LOSS = 'loss'
    DRAW = 'draw'
    NO_CONTEST = 'nc'


class WeightClassCode(IntEnum):
    """Numeric codes for UFC weight classes, distinct for men's and women's divisions."""

    UNKNOWN = -1
    OPENWEIGHT = 0
    # Men's
    MEN_FLYWEIGHT = 101
    MEN_BANTAMWEIGHT = 102
    MEN_FEATHERWEIGHT = 103
    MEN_LIGHTWEIGHT = 104
    MEN_WELTERWEIGHT = 105
    MEN_MIDDLEWEIGHT = 106
    MEN_LIGHT_HEAVYWEIGHT = 107
    MEN_HEAVYWEIGHT = 108
    # Women's
    WOMEN_STRAWWEIGHT = 201
    WOMEN_FLYWEIGHT = 202
    WOMEN_BANTAMWEIGHT = 203
    WOMEN_FEATHERWEIGHT = 204


# Max weight in lbs per division
WEIGHT_CLASS_MAX_LBS: dict[WeightClassCode, int] = {
    WeightClassCode.MEN_FLYWEIGHT: 125,
    WeightClassCode.MEN_BANTAMWEIGHT: 135,
    WeightClassCode.MEN_FEATHERWEIGHT: 145,
    WeightClassCode.MEN_LIGHTWEIGHT: 155,
    WeightClassCode.MEN_WELTERWEIGHT: 170,
    WeightClassCode.MEN_MIDDLEWEIGHT: 185,
    WeightClassCode.MEN_LIGHT_HEAVYWEIGHT: 205,
    WeightClassCode.MEN_HEAVYWEIGHT: 265,
    WeightClassCode.WOMEN_STRAWWEIGHT: 115,
    WeightClassCode.WOMEN_FLYWEIGHT: 125,
    WeightClassCode.WOMEN_BANTAMWEIGHT: 135,
    WeightClassCode.WOMEN_FEATHERWEIGHT: 145,
}
