from enum import Enum
from typing import Any, TypeVar

from sqlalchemy.engine import Dialect
from sqlalchemy.types import String, TypeDecorator

EnumT = TypeVar('EnumT', bound=Enum)


class StrEnum(TypeDecorator[Enum]):
    """SQLAlchemy TypeDecorator so that we can store enums as strings in the database.
    By default, SQLAalchemy will store `.name` in the database, but we override this logic to
    allow us to reduce the use of casting to `EnumType(value)`, and reduce the use of the
    `.value` property when using the field elsewhere.
    """

    impl = String
    cache_ok = True

    def __init__(self, enumtype: type[Enum], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value: Enum | str | None, _dialect: Dialect) -> str | None:
        if value:
            return value.value if isinstance(value, Enum) else value
        return None

    def process_result_value(self, value: str | None, _dialect: Dialect) -> Enum | None:
        if value is None:
            return None

        try:
            return self._enumtype(value)
        except ValueError:
            unknown_member = getattr(self._enumtype, 'UNKNOWN', None)
            if isinstance(unknown_member, self._enumtype):
                return unknown_member
            return None


class SourceSystemEnum(str, Enum):
    TAPOLOGY = 'tapology'
    UFCSTATS = 'ufcstats'
    UNKNOWN = 'unknown'


class SportTypeEnum(str, Enum):
    MMA = 'mma'
    GRAPPLING = 'grappling'
    KICKBOXING = 'kickboxing'
    BOXING = 'boxing'
    MUAY = 'muay'
    KNUCKLE = 'knuckle'
    CUSTOM = 'custom'
    KARATE = 'karate'
    WRESTLING = 'wrestling'
    SHOOTBOXING = 'shootboxing'
    KNUCKLE_MMA = 'knuckle_mma'
    SANDA = 'sanda'
    JUDO = 'judo'
    BOXING_CAGE = 'boxing_cage'
    SLAP = 'slap'
    COMBAT_JJ = 'combat_jj'
    SAMBO = 'sambo'
    LETHWEI = 'lethwei'
    VALETUDO = 'valetudo'
    ICE_FIGHTING = 'ice_fighting'
    PANCRASE = 'pancrase'
    UNKNOWN = 'unknown'


class BoutResultEnum(str, Enum):
    WIN = 'W'
    LOSS = 'L'
    DRAW = 'D'
    NO_CONTEST = 'NC'
    UNKNOWN = 'UNKNOWN'


class MethodGroupEnum(str, Enum):
    KO = 'KO'
    TKO = 'TKO'
    SUB = 'SUB'
    DEC = 'DEC'
    DQ = 'DQ'
    OVERTURNED = 'OVERTURNED'
    OTHER = 'OTHER'
    UNKNOWN = 'UNKNOWN'


class DecisionTypeEnum(str, Enum):
    UD = 'UD'
    SD = 'SD'
    MD = 'MD'
    TD = 'TD'
    DRAW = 'DRAW'
    NA = 'NA'
    UNKNOWN = 'UNKNOWN'


class BoutCornerEnum(str, Enum):
    RED = 'red'
    BLUE = 'blue'
    UNKNOWN = 'unknown'


class TapologyMethodEnum(str, Enum):
    TKO = 'TKO'
    KO = 'KO'
    SUB = 'SUB'
    DEC = 'DEC'
    DQ = 'DQ'
    UNKNOWN = 'UNKNOWN'


class UfcStatsMethodEnum(str, Enum):
    DECISION = 'Decision'
    DECISION_UNANIMOUS = 'Decision - Unanimous'
    KO_TKO = 'KO/TKO'
    SUBMISSION = 'Submission'
    DECISION_SPLIT = 'Decision - Split'
    TKO_DOCTOR_STOPPAGE = "TKO - Doctor's Stoppage"
    DECISION_MAJORITY = 'Decision - Majority'
    OVERTURNED = 'Overturned'
    COULD_NOT_CONTINUE = 'Could Not Continue'
    DQ = 'DQ'
    OTHER = 'Other'
    UNKNOWN = 'UNKNOWN'


class FighterGenderEnum(str, Enum):
    MALE = 'M'
    FEMALE = 'F'
    UNKNOWN = 'U'


class WeightUnitEnum(str, Enum):
    LB = 'lb'
    KG = 'kg'
    UNKNOWN = 'unknown'


class BooleanTextEnum(str, Enum):
    TRUE = 'True'
    FALSE = 'False'


def coerce_enum(raw_value: str | Enum | None, enum_type: type[EnumT], default: EnumT | None = None) -> EnumT | None:
    coerced_value = default

    if raw_value is not None:
        if isinstance(raw_value, enum_type):
            coerced_value = raw_value
        else:
            normalized = str(raw_value).strip()
            if normalized:
                try:
                    coerced_value = enum_type(normalized)
                except ValueError:
                    if default is None:
                        unknown_member = getattr(enum_type, 'UNKNOWN', None)
                        coerced_value = unknown_member if isinstance(unknown_member, enum_type) else None

    return coerced_value


def parse_bool_text(raw_value: str | bool | None) -> bool | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, bool):
        return raw_value

    normalized = str(raw_value).strip().lower()
    if not normalized:
        return None
    if normalized in {'true', 't', '1', 'yes', 'y'}:
        return True
    if normalized in {'false', 'f', '0', 'no', 'n'}:
        return False
    return None


def normalize_tapology_method(raw_value: str | None) -> TapologyMethodEnum:
    return coerce_enum(raw_value, TapologyMethodEnum, TapologyMethodEnum.UNKNOWN) or TapologyMethodEnum.UNKNOWN


def normalize_ufcstats_method(raw_value: str | None) -> UfcStatsMethodEnum:
    return coerce_enum(raw_value, UfcStatsMethodEnum, UfcStatsMethodEnum.UNKNOWN) or UfcStatsMethodEnum.UNKNOWN


def normalize_sport_type(raw_value: str | None) -> SportTypeEnum:
    return coerce_enum(raw_value, SportTypeEnum, SportTypeEnum.UNKNOWN) or SportTypeEnum.UNKNOWN


def normalize_bout_result(raw_value: str | None) -> BoutResultEnum:
    if raw_value is None:
        return BoutResultEnum.UNKNOWN

    normalized = str(raw_value).strip().upper()
    if normalized == 'W':
        return BoutResultEnum.WIN
    if normalized == 'L':
        return BoutResultEnum.LOSS
    if normalized in {'D', 'DRAW'}:
        return BoutResultEnum.DRAW
    if normalized in {'NC', 'NO CONTEST', 'NO-CONTEST'}:
        return BoutResultEnum.NO_CONTEST
    return BoutResultEnum.UNKNOWN


def inverse_bout_result(result: BoutResultEnum) -> BoutResultEnum:
    if result == BoutResultEnum.WIN:
        return BoutResultEnum.LOSS
    if result == BoutResultEnum.LOSS:
        return BoutResultEnum.WIN
    if result == BoutResultEnum.DRAW:
        return BoutResultEnum.DRAW
    if result == BoutResultEnum.NO_CONTEST:
        return BoutResultEnum.NO_CONTEST
    return BoutResultEnum.UNKNOWN


def normalize_tapology_method_group(  # noqa: PLR0911
    method: str | None, details: str | None
) -> tuple[MethodGroupEnum, DecisionTypeEnum]:
    method_value = (method or '').strip().upper()
    details_value = (details or '').strip().lower()

    if 'overturned' in details_value:
        return MethodGroupEnum.OVERTURNED, DecisionTypeEnum.NA

    if 'SUB' in method_value:
        return MethodGroupEnum.SUB, DecisionTypeEnum.NA

    if method_value == 'DQ':
        return MethodGroupEnum.DQ, DecisionTypeEnum.NA

    if method_value == 'KO' or 'knockout' in details_value:
        return MethodGroupEnum.KO, DecisionTypeEnum.NA

    if (
        method_value == 'TKO'
        or 'doctor' in details_value
        or 'corner stoppage' in details_value
        or 'injury' in details_value
    ):
        return MethodGroupEnum.TKO, DecisionTypeEnum.NA

    if method_value == 'DEC' or 'decision' in details_value:
        if 'unanimous' in details_value:
            return MethodGroupEnum.DEC, DecisionTypeEnum.UD
        if 'split' in details_value:
            return MethodGroupEnum.DEC, DecisionTypeEnum.SD
        if 'majority' in details_value:
            return MethodGroupEnum.DEC, DecisionTypeEnum.MD
        if 'technical decision' in details_value:
            return MethodGroupEnum.DEC, DecisionTypeEnum.TD
        if 'draw' in details_value:
            return MethodGroupEnum.DEC, DecisionTypeEnum.DRAW
        return MethodGroupEnum.DEC, DecisionTypeEnum.NA

    if method_value:
        return MethodGroupEnum.OTHER, DecisionTypeEnum.NA
    return MethodGroupEnum.UNKNOWN, DecisionTypeEnum.UNKNOWN


def normalize_ufcstats_method_group(  # noqa: PLR0911
    method: str | None, details: str | None
) -> tuple[MethodGroupEnum, DecisionTypeEnum]:
    method_value = (method or '').strip().lower()
    details_value = (details or '').strip().lower()

    normalized = method_value or details_value
    if not normalized:
        return MethodGroupEnum.UNKNOWN, DecisionTypeEnum.UNKNOWN

    if 'overturned' in normalized:
        return MethodGroupEnum.OVERTURNED, DecisionTypeEnum.NA
    if 'dq' in normalized:
        return MethodGroupEnum.DQ, DecisionTypeEnum.NA
    if 'submission' in normalized or normalized == 'sub':
        return MethodGroupEnum.SUB, DecisionTypeEnum.NA
    if 'ko/tko' in normalized or 'doctor' in normalized:
        return MethodGroupEnum.TKO, DecisionTypeEnum.NA
    if normalized in {'decision - unanimous', 'u-dec'}:
        return MethodGroupEnum.DEC, DecisionTypeEnum.UD
    if normalized in {'decision - split', 's-dec'}:
        return MethodGroupEnum.DEC, DecisionTypeEnum.SD
    if normalized in {'decision - majority', 'm-dec'}:
        return MethodGroupEnum.DEC, DecisionTypeEnum.MD
    if normalized in {'decision', 'decision - technical', 't-dec'}:
        return MethodGroupEnum.DEC, DecisionTypeEnum.NA
    if 'decision' in normalized and 'draw' in normalized:
        return MethodGroupEnum.DEC, DecisionTypeEnum.DRAW
    if 'could not continue' in normalized:
        return MethodGroupEnum.OTHER, DecisionTypeEnum.NA
    if 'ko' in normalized:
        return MethodGroupEnum.KO, DecisionTypeEnum.NA
    return MethodGroupEnum.OTHER, DecisionTypeEnum.NA


def is_finish_method_group(method_group: MethodGroupEnum) -> bool:
    return method_group in {MethodGroupEnum.KO, MethodGroupEnum.TKO, MethodGroupEnum.SUB, MethodGroupEnum.DQ}
