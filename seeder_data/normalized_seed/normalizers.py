from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

from elo_calculator.domain.shared.enumerations import (
    BoutResultEnum,
    DecisionTypeEnum,
    FighterGenderEnum,
    MethodGroupEnum,
    WeightUnitEnum,
)
from seeder_data.normalized_seed.constants import (
    CLASSIC_MMA_LIMITS_LBS,
    MMA_DIVISION_BOUNDS_LBS,
    MMA_DIVISION_LIMITS_LBS,
    MMA_DIVISION_ORDER,
    RulesetNormalized,
    WeightClassParseResult,
)
from seeder_data.normalized_seed.helpers import clean_text, parse_decimal, parse_gender

NUMERIC_RE = re.compile(r'(\d+(?:\.\d+)?)')
ALNUM_RE = re.compile(r'[^A-Z0-9]+')

SPORTS_PROBABLY_KG = {'kickboxing', 'shootboxing', 'lethwei'}
SPORTS_PROBABLY_LB = {'boxing', 'boxing_cage'}

DIVISION_PHRASES = (
    ('super heavyweight', 'SHW', 'Super Heavyweight'),
    ('light heavyweight', 'LHW', 'Light Heavyweight'),
    ('middleweight', 'MW', 'Middleweight'),
    ('welterweight', 'WW', 'Welterweight'),
    ('super lightweight', 'LW', 'Super Lightweight'),
    ('lightweight', 'LW', 'Lightweight'),
    ('featherweight', 'FW', 'Featherweight'),
    ('bantamweight', 'BW', 'Bantamweight'),
    ('flyweight', 'FLY', 'Flyweight'),
    ('strawweight', 'STRAW', 'Strawweight'),
    ('atomweight', 'ATOM', 'Atomweight'),
    ('open weight', 'OPEN', 'Open Weight'),
    ('openweight', 'OPEN', 'Open Weight'),
    ('open', 'OPEN', 'Open Weight'),
)

DISPLAY_BY_MMA_CODE = {
    'ATOM': 'Atomweight',
    'STRAW': 'Strawweight',
    'FLY': 'Flyweight',
    'BW': 'Bantamweight',
    'FW': 'Featherweight',
    'LW': 'Lightweight',
    'WW': 'Welterweight',
    'MW': 'Middleweight',
    'LHW': 'Light Heavyweight',
    'HW': 'Heavyweight',
    'SHW': 'Super Heavyweight',
}
MMA_TITLE_ROUNDS = 5
MMA_NON_TITLE_ROUNDS = 3


@dataclass(frozen=True)
class WeightClassContext:
    sport_key: str
    cleaned_weight_class: str
    sex: FighterGenderEnum
    is_catchweight: bool
    is_openweight: bool
    phrase_code: str | None
    phrase_name: str | None
    unit: WeightUnitEnum | None
    value: Decimal | None
    weight_lbs: Decimal | None
    confidence: Decimal
    note: str | None


def _to_two_dp(value: Decimal) -> Decimal:
    return value.quantize(Decimal('0.01'))


def _sanitize_key_token(value: str) -> str:
    return ALNUM_RE.sub('_', value.strip().upper()).strip('_') or 'UNKNOWN'


def _extract_first_numeric(text_value: str | None) -> Decimal | None:
    if text_value is None:
        return None
    matched = NUMERIC_RE.search(text_value)
    if matched is None:
        return None
    return parse_decimal(matched.group(1))


def _infer_from_unlabeled_numeric(
    sport_key: str, numeric_value: Decimal
) -> tuple[WeightUnitEnum, Decimal, Decimal, str]:
    if sport_key in SPORTS_PROBABLY_KG:
        if numeric_value <= Decimal('120') and numeric_value not in CLASSIC_MMA_LIMITS_LBS:
            return (
                WeightUnitEnum.KG,
                _to_two_dp(numeric_value * Decimal('2.20462')),
                Decimal('0.700'),
                'heuristic_non_mma_numeric_kg',
            )
        return WeightUnitEnum.LB, numeric_value, Decimal('0.450'), 'heuristic_non_mma_numeric_lb'

    if sport_key in SPORTS_PROBABLY_LB:
        return WeightUnitEnum.LB, numeric_value, Decimal('0.700'), 'boxing_default_lb'

    return WeightUnitEnum.LB, numeric_value, Decimal('0.400'), 'generic_numeric_lb_low_confidence'


def _infer_weight_limit(
    sport_key: str, raw_weight_class: str | None, raw_weight_lbs: str | None
) -> tuple[WeightUnitEnum | None, Decimal | None, Decimal | None, Decimal, str | None]:
    lowered = (raw_weight_class or '').lower()
    raw_lbs = parse_decimal(raw_weight_lbs)
    parsed_numeric = _extract_first_numeric(raw_weight_class)
    unit: WeightUnitEnum | None = None
    value: Decimal | None = None
    weight_lbs: Decimal | None = None
    confidence = Decimal('0.500')
    note: str | None = None

    if 'kg' in lowered or 'kgs' in lowered:
        if parsed_numeric is None:
            confidence = Decimal('0.300')
            note = 'kg_token_without_numeric_value'
        else:
            value = _to_two_dp(parsed_numeric)
            weight_lbs = _to_two_dp(value * Decimal('2.20462'))
            unit = WeightUnitEnum.KG
            confidence = Decimal('0.980')
    elif any(token in lowered for token in ('lbs', 'lb', 'pound')):
        if parsed_numeric is None:
            confidence = Decimal('0.300')
            note = 'lb_token_without_numeric_value'
        else:
            value = _to_two_dp(parsed_numeric)
            weight_lbs = value
            unit = WeightUnitEnum.LB
            confidence = Decimal('0.980')
    elif parsed_numeric is not None:
        numeric_value = _to_two_dp(parsed_numeric)
        value = numeric_value
        unit, weight_lbs, confidence, note = _infer_from_unlabeled_numeric(sport_key, numeric_value)
    elif raw_lbs is not None:
        value = _to_two_dp(raw_lbs)
        weight_lbs = value
        unit = WeightUnitEnum.LB
        confidence = Decimal('0.960')
        note = 'tapology_weight_lbs_column'

    return unit, value, weight_lbs, confidence, note


def _nearest_mma_division(weight_lbs: Decimal) -> str:
    nearest_key = 'HW'
    nearest_delta = Decimal('9999')
    for code in MMA_DIVISION_ORDER:
        delta = abs(MMA_DIVISION_LIMITS_LBS[code] - weight_lbs)
        if delta < nearest_delta:
            nearest_delta = delta
            nearest_key = code
    return nearest_key


def _parse_division_phrase(raw_weight_class: str | None) -> tuple[str | None, str | None]:
    lowered = (raw_weight_class or '').lower()
    for phrase, code, display_name in DIVISION_PHRASES:
        if phrase in lowered:
            return code, display_name
    return None, None


def _build_mma_weight_class_result(ctx: WeightClassContext) -> WeightClassParseResult:
    notes = [ctx.note] if ctx.note else []

    if ctx.is_openweight or ctx.phrase_code == 'OPEN':
        return WeightClassParseResult(
            sport_key='mma',
            raw_weight_class=ctx.cleaned_weight_class,
            sex=ctx.sex,
            division_key='MMA_OPEN',
            display_name='Open Weight',
            limit_lbs=None,
            lbs_min=None,
            lbs_max=None,
            is_openweight=True,
            is_canonical_mma=True,
            mma_division_key='MMA_OPEN',
            weight_limit_unit=ctx.unit,
            weight_limit_value=ctx.value,
            weight_limit_lbs=ctx.weight_lbs,
            is_catchweight=False,
            parse_confidence=Decimal('0.980'),
            notes='open_weight',
        )

    mma_code: str | None = None
    confidence = ctx.confidence
    if ctx.phrase_code in MMA_DIVISION_BOUNDS_LBS:
        mma_code = ctx.phrase_code
        confidence = max(ctx.confidence, Decimal('0.970'))
    elif ctx.phrase_code == 'SHW':
        mma_code = 'SHW'
        confidence = max(ctx.confidence, Decimal('0.900'))
    elif ctx.weight_lbs is not None:
        mma_code = _nearest_mma_division(ctx.weight_lbs)
        confidence = max(confidence, Decimal('0.750'))
        notes.append('mma_division_from_numeric_limit')

    if mma_code is None:
        return WeightClassParseResult(
            sport_key='mma',
            raw_weight_class=ctx.cleaned_weight_class,
            sex=ctx.sex,
            division_key='MMA_UNKNOWN',
            display_name=ctx.phrase_name or ctx.cleaned_weight_class,
            limit_lbs=None,
            lbs_min=None,
            lbs_max=None,
            is_openweight=False,
            is_canonical_mma=False,
            mma_division_key='MMA_UNKNOWN',
            weight_limit_unit=ctx.unit,
            weight_limit_value=ctx.value,
            weight_limit_lbs=ctx.weight_lbs,
            is_catchweight=ctx.is_catchweight,
            parse_confidence=min(confidence, Decimal('0.600')),
            notes=';'.join(notes) or None,
        )

    display_name = DISPLAY_BY_MMA_CODE.get(mma_code, ctx.phrase_name or 'Unknown')
    limit_lbs = MMA_DIVISION_LIMITS_LBS.get(mma_code)
    bounds = MMA_DIVISION_BOUNDS_LBS.get(mma_code)

    return WeightClassParseResult(
        sport_key='mma',
        raw_weight_class=ctx.cleaned_weight_class,
        sex=ctx.sex,
        division_key=f'MMA_{mma_code}',
        display_name=display_name,
        limit_lbs=limit_lbs,
        lbs_min=bounds[0] if bounds else None,
        lbs_max=bounds[1] if bounds else None,
        is_openweight=False,
        is_canonical_mma=True,
        mma_division_key=f'MMA_{mma_code}',
        weight_limit_unit=ctx.unit,
        weight_limit_value=ctx.value,
        weight_limit_lbs=ctx.weight_lbs,
        is_catchweight=ctx.is_catchweight,
        parse_confidence=confidence,
        notes=';'.join(notes) or None,
    )


def _build_non_mma_weight_class_result(ctx: WeightClassContext) -> WeightClassParseResult:
    sport_prefix = _sanitize_key_token(ctx.sport_key)
    notes = [ctx.note] if ctx.note else []

    if ctx.is_openweight or ctx.phrase_code == 'OPEN':
        return WeightClassParseResult(
            sport_key=ctx.sport_key,
            raw_weight_class=ctx.cleaned_weight_class,
            sex=ctx.sex,
            division_key=f'{sport_prefix}_OPEN',
            display_name='Open Weight',
            limit_lbs=None,
            lbs_min=None,
            lbs_max=None,
            is_openweight=True,
            is_canonical_mma=False,
            mma_division_key=None,
            weight_limit_unit=ctx.unit,
            weight_limit_value=ctx.value,
            weight_limit_lbs=ctx.weight_lbs,
            is_catchweight=False,
            parse_confidence=max(ctx.confidence, Decimal('0.900')),
            notes='open_weight_non_mma',
        )

    if ctx.is_catchweight:
        return WeightClassParseResult(
            sport_key=ctx.sport_key,
            raw_weight_class=ctx.cleaned_weight_class,
            sex=ctx.sex,
            division_key=f'{sport_prefix}_CATCHWEIGHT',
            display_name='Catchweight',
            limit_lbs=ctx.weight_lbs,
            lbs_min=ctx.weight_lbs,
            lbs_max=ctx.weight_lbs,
            is_openweight=False,
            is_canonical_mma=False,
            mma_division_key=None,
            weight_limit_unit=ctx.unit,
            weight_limit_value=ctx.value,
            weight_limit_lbs=ctx.weight_lbs,
            is_catchweight=True,
            parse_confidence=max(ctx.confidence, Decimal('0.850')),
            notes=';'.join(notes) or None,
        )

    if ctx.phrase_code is not None:
        token = _sanitize_key_token(ctx.phrase_name or ctx.phrase_code)
        return WeightClassParseResult(
            sport_key=ctx.sport_key,
            raw_weight_class=ctx.cleaned_weight_class,
            sex=ctx.sex,
            division_key=f'{sport_prefix}_{token}',
            display_name=ctx.phrase_name or ctx.cleaned_weight_class,
            limit_lbs=None,
            lbs_min=None,
            lbs_max=None,
            is_openweight=False,
            is_canonical_mma=False,
            mma_division_key=None,
            weight_limit_unit=ctx.unit,
            weight_limit_value=ctx.value,
            weight_limit_lbs=ctx.weight_lbs,
            is_catchweight=False,
            parse_confidence=max(ctx.confidence, Decimal('0.850')),
            notes=';'.join(notes) or None,
        )

    division_display_name = ctx.phrase_name or ctx.cleaned_weight_class or 'Unknown'
    return WeightClassParseResult(
        sport_key=ctx.sport_key,
        raw_weight_class=ctx.cleaned_weight_class,
        sex=ctx.sex,
        division_key=f'{sport_prefix}_UNKNOWN',
        display_name=division_display_name,
        limit_lbs=ctx.weight_lbs,
        lbs_min=ctx.weight_lbs,
        lbs_max=ctx.weight_lbs,
        is_openweight=False,
        is_canonical_mma=False,
        mma_division_key=None,
        weight_limit_unit=ctx.unit,
        weight_limit_value=ctx.value,
        weight_limit_lbs=ctx.weight_lbs,
        is_catchweight=False,
        parse_confidence=min(ctx.confidence, Decimal('0.650')),
        notes=';'.join(notes) or None,
    )


def parse_weight_class(
    raw_weight_class: str | None, raw_gender: str | None, raw_weight_lbs: str | None, sport_key: str
) -> WeightClassParseResult:
    normalized_sport = clean_text(sport_key) or 'unknown'
    cleaned_weight_class = clean_text(raw_weight_class) or 'Unknown'
    lowered_weight_class = cleaned_weight_class.lower()

    sex = parse_gender(raw_gender)
    if "women's" in lowered_weight_class or 'women' in lowered_weight_class:
        sex = FighterGenderEnum.FEMALE

    is_catchweight = 'catch' in lowered_weight_class
    is_openweight = 'open' in lowered_weight_class
    phrase_code, phrase_name = _parse_division_phrase(cleaned_weight_class)
    unit, value, weight_lbs, confidence, note = _infer_weight_limit(
        normalized_sport, cleaned_weight_class, raw_weight_lbs
    )
    context = WeightClassContext(
        sport_key=normalized_sport,
        cleaned_weight_class=cleaned_weight_class,
        sex=sex,
        is_catchweight=is_catchweight,
        is_openweight=is_openweight,
        phrase_code=phrase_code,
        phrase_name=phrase_name,
        unit=unit,
        value=value,
        weight_lbs=weight_lbs,
        confidence=confidence,
        note=note,
    )

    if normalized_sport == 'mma':
        return _build_mma_weight_class_result(context)

    return _build_non_mma_weight_class_result(context)


def infer_ruleset(sport_key: str, scheduled_rounds: int | None, is_title_fight: bool) -> RulesetNormalized:
    round_seconds = 300
    judging_standard = 'ABC_UNIFIED'

    if sport_key == 'mma':
        if scheduled_rounds is None:
            scheduled_rounds = MMA_TITLE_ROUNDS if is_title_fight else MMA_NON_TITLE_ROUNDS
        if scheduled_rounds == MMA_TITLE_ROUNDS and is_title_fight:
            key = 'unified_mma_title_5x5'
        elif scheduled_rounds == MMA_TITLE_ROUNDS:
            key = 'unified_mma_5x5'
        elif scheduled_rounds == MMA_NON_TITLE_ROUNDS:
            key = 'unified_mma_3x5'
        else:
            key = f'unified_mma_{scheduled_rounds}x5'
        return RulesetNormalized(
            key=key,
            sport_key=sport_key,
            rounds_scheduled=scheduled_rounds,
            round_seconds=round_seconds,
            judging_standard=judging_standard,
        )

    if scheduled_rounds is None:
        return RulesetNormalized(
            key=f'{sport_key}_unknown',
            sport_key=sport_key,
            rounds_scheduled=0,
            round_seconds=round_seconds,
            judging_standard=judging_standard,
        )
    return RulesetNormalized(
        key=f'{sport_key}_{scheduled_rounds}x5',
        sport_key=sport_key,
        rounds_scheduled=scheduled_rounds,
        round_seconds=round_seconds,
        judging_standard=judging_standard,
    )


def derive_ufc_outcomes(
    winner_source_id: str | None, red_source_id: str | None, blue_source_id: str | None, method_group: MethodGroupEnum
) -> tuple[BoutResultEnum, BoutResultEnum]:
    if method_group == MethodGroupEnum.OVERTURNED:
        return BoutResultEnum.NO_CONTEST, BoutResultEnum.NO_CONTEST

    if winner_source_id and winner_source_id == red_source_id:
        return BoutResultEnum.WIN, BoutResultEnum.LOSS
    if winner_source_id and winner_source_id == blue_source_id:
        return BoutResultEnum.LOSS, BoutResultEnum.WIN
    return BoutResultEnum.DRAW, BoutResultEnum.DRAW


def normalize_decision_for_outcome(
    outcome: BoutResultEnum, method_group: MethodGroupEnum, decision_type: DecisionTypeEnum
) -> DecisionTypeEnum:
    if outcome == BoutResultEnum.DRAW:
        return DecisionTypeEnum.DRAW
    if method_group != MethodGroupEnum.DEC:
        return DecisionTypeEnum.NA
    if decision_type == DecisionTypeEnum.UNKNOWN:
        return DecisionTypeEnum.NA
    return decision_type
