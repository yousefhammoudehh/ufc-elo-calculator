from __future__ import annotations

import contextlib
import json
import math
import os
import random
import statistics as stats
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import date as date_type
from math import exp, log, log2
from typing import Any, ClassVar, cast
from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.application.elo_calculator import SCALE_S, logistic_expect, method_class
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.entities import Bout, BoutParticipant, Event, Fighter
from elo_calculator.domain.shared.enumerations import WEIGHT_CLASS_MAX_LBS, FightOutcome, WeightClassCode
from elo_calculator.infrastructure.artifacts_loader import load_artifacts
from elo_calculator.infrastructure.external_services.caching import CacheManager
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow

logger = get_logger()

# Module-level constants to avoid magic-number comparisons
MIN_PARTICIPANTS = 2
ODDS_FAVORITE_THRESHOLD = 0.5
# Activity/retirement thresholds (days/fights)
ACTIVE_F12_MIN = 1
ACTIVE_F24_MIN = 2
ACTIVE_LAST_DAYS = 548
RETIRED_LAST_DAYS = 1095

DIVISION_CODE_ALIASES: dict[int, tuple[int, ...]] = {
    115: (WeightClassCode.WOMEN_STRAWWEIGHT,),
    125: (WeightClassCode.MEN_FLYWEIGHT, WeightClassCode.WOMEN_FLYWEIGHT),
    135: (WeightClassCode.MEN_BANTAMWEIGHT, WeightClassCode.WOMEN_BANTAMWEIGHT),
    145: (WeightClassCode.MEN_FEATHERWEIGHT, WeightClassCode.WOMEN_FEATHERWEIGHT),
    155: (WeightClassCode.MEN_LIGHTWEIGHT,),
    170: (WeightClassCode.MEN_WELTERWEIGHT,),
    185: (WeightClassCode.MEN_MIDDLEWEIGHT,),
    205: (WeightClassCode.MEN_LIGHT_HEAVYWEIGHT,),
    265: (WeightClassCode.MEN_HEAVYWEIGHT,),
}

DIVISION_LABELS: dict[int, str] = {
    int(WeightClassCode.MEN_FLYWEIGHT): "Men's Flyweight (125)",
    int(WeightClassCode.MEN_BANTAMWEIGHT): "Men's Bantamweight (135)",
    int(WeightClassCode.MEN_FEATHERWEIGHT): "Men's Featherweight (145)",
    int(WeightClassCode.MEN_LIGHTWEIGHT): "Men's Lightweight (155)",
    int(WeightClassCode.MEN_WELTERWEIGHT): "Men's Welterweight (170)",
    int(WeightClassCode.MEN_MIDDLEWEIGHT): "Men's Middleweight (185)",
    int(WeightClassCode.MEN_LIGHT_HEAVYWEIGHT): "Men's Light Heavyweight (205)",
    int(WeightClassCode.MEN_HEAVYWEIGHT): "Men's Heavyweight (265)",
    int(WeightClassCode.WOMEN_STRAWWEIGHT): "Women's Strawweight (115)",
    int(WeightClassCode.WOMEN_FLYWEIGHT): "Women's Flyweight (125)",
    int(WeightClassCode.WOMEN_BANTAMWEIGHT): "Women's Bantamweight (135)",
    int(WeightClassCode.WOMEN_FEATHERWEIGHT): "Women's Featherweight (145)",
}


@dataclass
class EloPoint:
    bout_id: str
    event_id: Any | None
    event_date: Any | None
    event_link: str | None
    event_stats_link: str | None
    opponent_id: str | None
    opponent_name: str | None
    result: str | None
    elo_before: float | None
    elo_after: float | None
    delta: float | None
    event_name: str | None = None
    rank_after: int | None = None
    is_title_fight: bool | None = None
    weight_class_code: int | None = None


@dataclass
class EloChange:
    bout_id: str
    fighter_id: str
    fighter_name: str | None
    opponent_id: str | None
    opponent_name: str | None
    opponent_elo_before: float | None
    delta: float
    elo_before: float | None
    elo_after: float | None
    outcome: str | None
    event_id: Any | None
    event_name: str | None
    event_date: date_type | None


class AnalyticsService(BaseService):
    # Simple in-memory cache: key -> (expires_epoch, value)
    _CACHE: ClassVar[dict[str, tuple[float, Any]]] = {}
    # Optional adjustment artifacts loaded at startup
    _ARTIFACTS: ClassVar[dict[str, Any] | None] = None

    @staticmethod
    def _normalize_division_codes(code: int) -> tuple[int, ...]:
        alias = DIVISION_CODE_ALIASES.get(int(code))
        if alias:
            return tuple(int(c) for c in alias)
        return (int(code),)

    @staticmethod
    def _now() -> float:
        return time.time()

    @classmethod
    def _cache_get(cls, key: str) -> Any | None:
        exp_val = cls._CACHE.get(key)
        if not exp_val:
            return None
        exp, val = exp_val
        if cls._now() > exp:
            with contextlib.suppress(Exception):
                del cls._CACHE[key]
            return None
        return val

    @classmethod
    def _cache_put(cls, key: str, val: Any, ttl_sec: float) -> None:
        cls._CACHE[key] = (cls._now() + float(ttl_sec), val)

    @classmethod
    def init_artifacts(cls) -> None:
        """Load ridge/meta/platt artifacts into memory (optional)."""

        try:
            cls._ARTIFACTS = load_artifacts() or {}
        except Exception:  # pragma: no cover — non-critical optional init
            cls._ARTIFACTS = {}

    @staticmethod
    def _date_to_label(value: Any) -> str | None:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return None

    @staticmethod
    def _collect_recent_activity(
        parts: Iterable[BoutParticipant],
        bouts_map: Mapping[str, Bout],
        events_map: Mapping[Any, Event],
        cutoff: date_type | None,
    ) -> tuple[dict[str, int], dict[str, Any], dict[str, Any]]:
        recent_counts: dict[str, int] = defaultdict(int)
        recent_last: dict[str, Any] = {}
        last_any: dict[str, Any] = {}
        for bp in parts:
            fid = getattr(bp, 'fighter_id', None)
            if not fid:
                continue
            bout = bouts_map.get(bp.bout_id)
            if not bout:
                continue
            ev = events_map.get(bout.event_id) if bout and bout.event_id else None
            ev_date = getattr(ev, 'event_date', None)
            if ev_date is None:
                continue
            prev_all = last_any.get(fid)
            if prev_all is None or ev_date > prev_all:
                last_any[fid] = ev_date
            if cutoff is not None and ev_date < cutoff:
                continue
            recent_counts[fid] += 1
            prev_recent = recent_last.get(fid)
            if prev_recent is None or ev_date > prev_recent:
                recent_last[fid] = ev_date
        return recent_counts, recent_last, last_any

    async def _ewma_recent_elo(self, uow: UnitOfWork, fighter_id: str, *, half_life_days: int = 180) -> float | None:
        """Compute EWMA of recent elo_after for a fighter using event dates.

        Caches in Redis for 10 minutes per fighter/half-life.
        """
        cache = CacheManager(ttl=600)
        ck = f'analytics:ewma_elo:{fighter_id}:{half_life_days}'
        try:
            cached = await cache.get_json(ck)
            if isinstance(cached, (int, float)):
                return float(cached)
            if isinstance(cached, dict) and 'value' in cached:
                with contextlib.suppress(TypeError, ValueError):
                    return float(cached['value'])
        except Exception as exc:
            logger.debug('EWMA cache read failed for %s: %r', fighter_id, exc)

        # Collect dated elo_after points
        parts = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return None
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        rows_raw: list[tuple[date_type | None, float]] = []
        for p in parts:
            if p.elo_after is None:
                continue
            b = bouts_map.get(p.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            rows_raw.append((d, float(p.elo_after)))
        rows: list[tuple[date_type, float]] = [(d, v) for d, v in rows_raw if d is not None]
        if not rows:
            return None
        rows.sort(key=lambda t: t[0], reverse=True)
        ref = date_type.today()
        k = log(2.0) / max(1, half_life_days)
        num = den = 0.0
        for d, elo in rows[:30]:  # limit window for speed
            days = (ref - d).days
            w = exp(-k * max(0, days))
            num += elo * w
            den += w
        val = (num / den) if den else None
        if val is None:
            return None
        result_val = float(val)
        with contextlib.suppress(Exception):
            await cache.set_json(ck, {'value': result_val})
        return result_val

    @with_uow
    async def form_index(  # noqa: PLR0912, PLR0915
        self,
        uow: UnitOfWork,
        fighter_id: str,
        *,
        window: str = 'fights',  # 'fights' | 'days'
        n: int = 5,
        half_life_days: int = 180,
    ) -> dict[str, Any]:
        """Recency-weighted residuals vs expectation over a time/window.

        residual = (1 for win, 0.5 draw, 0 loss) - logistic_expect(elo_before_f, elo_before_opp)
        Aggregated with exponential decay by days with half-life `half_life_days`.
        """
        # Fetch all participations for fighter with event dates and opponent elo_before
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return {'fighter_id': fighter_id, 'fi': 0.0, 'count': 0, 'items': []}
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)
        # Build items with date, residual, opponent elo_before
        items: list[dict[str, Any]] = []
        for p in parts:
            b = bouts_map.get(p.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if d is None:
                continue
            # Outcome to target
            target = 1.0 if p.outcome == FightOutcome.WIN else (0.5 if p.outcome == FightOutcome.DRAW else 0.0)
            # Opponent
            opp_before: float | None = None
            opp_before_fallback: float = 1500.0
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id:
                    opp_before = float(op.elo_before) if op.elo_before is not None else None
                    break
            f_before = float(p.elo_before) if p.elo_before is not None else None
            if f_before is None or opp_before is None:
                continue
            exp_win = logistic_expect(float(f_before), float(opp_before), SCALE_S)
            residual = float(target) - float(exp_win)
            items.append(
                {
                    'date': d,
                    'residual': residual,
                    'elo_before': f_before,
                    'opp_elo_before': float(opp_before if opp_before is not None else opp_before_fallback),
                }
            )

        if not items:
            return {'fighter_id': fighter_id, 'fi': 0.0, 'count': 0, 'series': []}
        items.sort(key=lambda x: x['date'])
        # Window slice
        if window == 'fights':
            items = items[-max(1, int(n)) :]
        else:
            # days window
            try:
                last = items[-1]['date']
                start = last - timedelta(days=max(1, int(n)))
                items = [it for it in items if it['date'] >= start]
            except Exception:
                items = items[-max(1, int(n)) :]
        # Decay weights by age in days relative to most recent
        try:
            last = items[-1]['date']
            lam = log(2.0) / max(1.0, float(half_life_days))
            weighted_sum = 0.0
            wsum = 0.0
            for it in items:
                age_days = (last - it['date']).days if last and it['date'] else 0
                w = exp(-lam * float(age_days))
                weighted_sum += w * float(it['residual'])
                wsum += w
            fi = weighted_sum / wsum if wsum > 0 else 0.0
        except Exception:
            fi = sum(float(it['residual']) for it in items) / float(len(items))
        # Also compute opponent-weighted variant (schedule-adjusted) as info
        try:
            avg_opp = sum(it['opp_elo_before'] for it in items) / max(1, len(items))
        except Exception:
            avg_opp = None
        # Slim series for sparkline
        series = [{'date': it['date'], 'residual': float(it['residual'])} for it in items]
        return {'fighter_id': fighter_id, 'fi': fi, 'count': len(items), 'avg_opp_elo': avg_opp, 'series': series}

    @with_uow
    async def momentum_slope(self, uow: UnitOfWork, fighter_id: str, k: int = 6) -> dict[str, Any]:
        """OLS slope of (fight_index, elo_after) over last k fights; also per-180-days slope using dates."""
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return {'fighter_id': fighter_id, 'slope_per_fight': 0.0, 'count': 0}
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        pts: list[tuple[Any, float]] = []
        for p in parts:
            b = bouts_map.get(p.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if p.elo_after is None or d is None:
                continue
            pts.append((d, float(p.elo_after)))
        if not pts:
            return {'fighter_id': fighter_id, 'slope_per_fight': 0.0, 'count': 0}
        pts.sort(key=lambda t: t[0])
        pts = pts[-max(2, int(k)) :]
        # index slope
        n = len(pts)
        xs = list(range(n))
        ys = [v for _, v in pts]
        xbar = sum(xs) / n
        ybar = sum(ys) / n
        denom = sum((x - xbar) ** 2 for x in xs) or 1.0
        slope = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys, strict=False)) / denom
        # per-180-days slope from dates
        try:
            t0 = pts[0][0]
            xs_days = [(d - t0).days for d, _ in pts]
            xbar2 = sum(xs_days) / n
            denom2 = sum((x - xbar2) ** 2 for x in xs_days) or 1.0
            slope_per_day = sum((x - xbar2) * (y - ybar) for x, y in zip(xs_days, ys, strict=False)) / denom2
            slope_per_180d = slope_per_day * 180.0
        except Exception:
            slope_per_180d = None
        series = [{'date': d, 'elo': v} for d, v in pts]
        return {
            'fighter_id': fighter_id,
            'slope_per_fight': slope,
            'slope_per_180d': slope_per_180d,
            'count': n,
            'series': series,
        }

    @with_uow
    async def division_parity(self, uow: UnitOfWork, division: int, year: int | None = None) -> dict[str, Any]:
        """Compute division parity via Gini over ELO distribution.

        If `year` provided, uses last elo_after within that calendar year for bouts in the division; otherwise current_elo.
        """
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}
        vals: dict[str, float] = {}
        if year is None:
            # any fighter who has fought in this division at least once
            eligible = {
                p.fighter_id
                for p in bps
                if ((b := bouts_map.get(p.bout_id)) is not None and getattr(b, 'weight_class_code', None) == division)
            }
            for fid in eligible:
                f = fighters_map.get(fid)
                if f and f.current_elo is not None:
                    vals[fid] = float(f.current_elo)
        else:
            # last elo_after within year and division
            last_by_fid: dict[str, tuple[Any, float]] = {}
            for p in bps:
                b = bouts_map.get(p.bout_id)
                if not b or getattr(b, 'weight_class_code', None) != division:
                    continue
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                if d is None or d.year != year or p.elo_after is None:
                    continue
                t = (d, float(p.elo_after))
                cur = last_by_fid.get(p.fighter_id)
                if cur is None or d > cur[0]:
                    last_by_fid[p.fighter_id] = t
            for fid, (_d, v) in last_by_fid.items():
                vals[fid] = v
        arr = sorted(vals.values())
        n = len(arr)
        if n == 0:
            return {'division': division, 'year': year, 'gini': None, 'count': 0}
        mean = sum(arr) / n
        if mean <= 0:
            return {'division': division, 'year': year, 'gini': 0.0, 'count': n}
        # Gini efficient formula using sorted values
        cum = 0.0
        for i, x in enumerate(arr, start=1):
            cum += i * x
        gini = (2 * cum) / (n * sum(arr)) - (n + 1) / n
        return {'division': division, 'year': year, 'gini': float(gini), 'count': n}

    @with_uow
    async def division_churn(self, uow: UnitOfWork, division: int, year: int) -> dict[str, Any]:
        """Average |ΔELO| per bout in division for the year and turnover in top-10 between start and end of year."""
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        # avg |delta|
        deltas: list[float] = []
        for p in bps:
            b = bouts_map.get(p.bout_id)
            if not b or getattr(b, 'weight_class_code', None) != division:
                continue
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if d is None or d.year != year or p.elo_before is None or p.elo_after is None:
                continue
            deltas.append(abs(float(p.elo_after) - float(p.elo_before)))
        avg_abs_delta = (sum(deltas) / len(deltas)) if deltas else 0.0
        # turnover top-10: build ranks at start and end of year using division-only last elo up to those dates
        start = date(year, 1, 1)
        end = date(year, 12, 31)

        def ranks_at(snap_date: date) -> list[str]:
            latest_vals: dict[str, float] = {}
            latest_dates: dict[str, date] = {}
            for p in bps:
                b = bouts_map.get(p.bout_id)
                if not b or getattr(b, 'weight_class_code', None) != division:
                    continue
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                if d is None or d > snap_date or p.elo_after is None:
                    continue
                fid = p.fighter_id
                if fid not in latest_dates or d > latest_dates[fid]:
                    latest_vals[fid] = float(p.elo_after)
                    latest_dates[fid] = d
            items = sorted(latest_vals.items(), key=lambda kv: kv[1], reverse=True)
            return [fid for fid, _ in items[:10]]

        top_start = set(ranks_at(start))
        top_end = set(ranks_at(end))
        unchanged = len(top_start.intersection(top_end))
        turnover = 1.0 - (unchanged / 10.0) if top_start and top_end else None
        return {
            'division': division,
            'year': year,
            'avg_abs_delta': avg_abs_delta,
            'top10_unchanged': unchanged,
            'top10_turnover': turnover,
        }

    @with_uow
    async def rates_per_minute(self, uow: UnitOfWork, fighter_id: str) -> dict[str, Any]:
        """Per-minute pace/efficiency metrics aggregated across UFC bouts.

        Returns totals, minutes, and rates per minute for key stats; plus TD% and control share.
        """
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return {'fighter_id': fighter_id, 'minutes': 0.0, 'rates': {}}
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        totals: dict[str, float] = defaultdict(float)
        minutes_total = 0.0
        ctrl_for = 0.0
        ctrl_against = 0.0
        # For opponent join
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)
        for p in parts:
            b = bouts_map.get(p.bout_id)
            if not b:
                continue
            # Compute fight minutes
            with contextlib.suppress(Exception):
                is_decision = bool(getattr(b, 'method', '') and 'Decision' in getattr(b, 'method', ''))
                if is_decision:
                    scheduled5 = bool(
                        getattr(b, 'is_title_fight', False)
                        or (getattr(b, 'time_format', '') and '5 Rnd' in getattr(b, 'time_format', ''))
                    )
                    minutes = 25.0 if scheduled5 else 15.0
                else:
                    rnum = int(getattr(b, 'round_num', 1) or 1)
                    tsec = int(getattr(b, 'time_sec', 0) or 0)
                    minutes = ((rnum - 1) * 300 + tsec) / 60.0
            if minutes <= 0:
                continue
            minutes_total += minutes
            # Totals
            for k in (
                'sig_strikes',
                'sig_strikes_thrown',
                'total_strikes',
                'total_strikes_thrown',
                'kd',
                'td',
                'td_attempts',
                'sub_attempts',
                'control_time_sec',
            ):
                with contextlib.suppress(Exception):
                    totals[k] += float(getattr(p, k) or 0)
            ctrl_for += float(getattr(p, 'control_time_sec', 0) or 0)
            # Opponent control
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id:
                    ctrl_against += float(getattr(op, 'control_time_sec', 0) or 0)
                    break
        if minutes_total <= 0:
            return {'fighter_id': fighter_id, 'minutes': 0.0, 'rates': {}}

        def per_min(x: float) -> float:
            return x / minutes_total

        rates = {
            'sig_landed_per_min': per_min(totals['sig_strikes']),
            'sig_thrown_per_min': per_min(totals['sig_strikes_thrown']),
            'total_landed_per_min': per_min(totals['total_strikes']),
            'total_thrown_per_min': per_min(totals['total_strikes_thrown']),
            'kd_per_min': per_min(totals['kd']),
            'td_per_min': per_min(totals['td']),
            'td_att_per_min': per_min(totals['td_attempts']),
            'sub_att_per_min': per_min(totals['sub_attempts']),
            'td_pct': (totals['td'] / totals['td_attempts']) if totals['td_attempts'] > 0 else None,
            'control_share': (ctrl_for / (ctrl_for + ctrl_against)) if (ctrl_for + ctrl_against) > 0 else None,
        }
        return {'fighter_id': fighter_id, 'minutes': minutes_total, 'totals': totals, 'rates': rates}

    @with_uow
    async def event_shock(self, uow: UnitOfWork, event_id: str | None = None) -> dict[str, Any]:  # noqa: PLR0912
        """Event Shock Index and Net ELO Transfer for a given event (or latest if None).
        shock = sum(-log(p_win_winner)); net_transfer = sum(|delta_i|) across participants
        """
        # Select target event
        if event_id:
            try:
                eid = UUID(event_id)
            except Exception:
                eid = None
            ev = await uow.events.get_by_event_id(eid) if eid else None
        else:
            evs = await uow.events.get_all()
            ev = max(evs, key=lambda e: getattr(e, 'event_date', date_type.min) or date_type.min) if evs else None
        if not ev:
            return {'event_id': event_id, 'event_name': None, 'event_date': None, 'shock': 0.0, 'net_transfer': 0.0}
        bouts = [b for b in await uow.bouts.get_all() if b.event_id == ev.event_id]
        if not bouts:
            return {
                'event_id': ev.event_id,
                'event_name': getattr(ev, 'name', None),
                'event_date': getattr(ev, 'event_date', None),
                'shock': 0.0,
                'net_transfer': 0.0,
            }
        # Build participants per bout
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for bp in await uow.bout_participants.get_all():
            if any(bp.bout_id == b.bout_id for b in bouts):
                by_bout[bp.bout_id].append(bp)
        shock_sum = 0.0
        transfer_sum = 0.0
        for b in bouts:
            parts = by_bout.get(b.bout_id, [])
            if len(parts) < MIN_PARTICIPANTS:
                continue
            p1, p2 = parts[0], parts[1]
            # probabilities using elo_before
            if p1.elo_before is None or p2.elo_before is None or p1.elo_after is None or p2.elo_after is None:
                continue
            e1 = logistic_expect(float(p1.elo_before), float(p2.elo_before), SCALE_S)
            e2 = 1.0 - e1
            # Identify winner
            if p1.outcome == FightOutcome.WIN:
                pw = max(e1, 1e-6)
            elif p2.outcome == FightOutcome.WIN:
                pw = max(e2, 1e-6)
            else:
                pw = max(0.5, 1e-6)  # draw/NC treated as neutral
            shock_sum += -log(pw)
            # transfer is total absolute elo movement for bout
            d1 = float(p1.elo_after) - float(p1.elo_before)
            d2 = float(p2.elo_after) - float(p2.elo_before)
            transfer_sum += abs(d1) + abs(d2)
        return {
            'event_id': ev.event_id,
            'event_name': getattr(ev, 'name', None),
            'event_date': getattr(ev, 'event_date', None),
            'shock': shock_sum,
            'net_transfer': transfer_sum,
        }

    @with_uow
    async def events_shock_top(  # noqa: PLR0912
        self,
        uow: UnitOfWork,
        *,
        limit: int = 5,
        order: str = 'desc',
        max_events: int | None = None,
        window_days: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return top events by Shock Index. order='desc' for most shocking, 'asc' for most predictable.
        Optionally limit to most recent `max_events` by date to bound compute.
        """
        events: list[Event] = await uow.events.get_all()
        if not events:
            return []
        # Optionally bound to recent events
        if max_events:
            events.sort(key=lambda e: getattr(e, 'event_date', date_type.min) or date_type.min)
            events = events[-int(max_events) :]
        if window_days and window_days > 0:
            cutoff = date_type.today() - timedelta(days=int(window_days))
            events = [e for e in events if getattr(e, 'event_date', None) and e.event_date >= cutoff]
        bouts_all: list[Bout] = await uow.bouts.get_all()
        parts_all: list[BoutParticipant] = await uow.bout_participants.get_all()
        by_event_bouts: dict[Any, list[Bout]] = defaultdict(list)
        for b in bouts_all:
            by_event_bouts[getattr(b, 'event_id', None)].append(b)
        by_bout_parts: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in parts_all:
            by_bout_parts[p.bout_id].append(p)
        out: list[dict[str, Any]] = []
        for ev in events:
            bouts = by_event_bouts.get(getattr(ev, 'event_id', None), [])
            if not bouts:
                continue
            shock_sum = 0.0
            transfer_sum = 0.0
            fights_count = 0
            for b in bouts:
                parts = by_bout_parts.get(b.bout_id, [])
                if len(parts) < MIN_PARTICIPANTS:
                    continue
                p1, p2 = parts[0], parts[1]
                if p1.elo_before is None or p2.elo_before is None or p1.elo_after is None or p2.elo_after is None:
                    continue
                e1 = logistic_expect(float(p1.elo_before), float(p2.elo_before), SCALE_S)
                e2 = 1.0 - e1
                if p1.outcome == FightOutcome.WIN:
                    pw = max(e1, 1e-6)
                elif p2.outcome == FightOutcome.WIN:
                    pw = max(e2, 1e-6)
                else:
                    pw = max(0.5, 1e-6)
                shock_sum += -log(pw)
                d1 = float(p1.elo_after) - float(p1.elo_before)
                d2 = float(p2.elo_after) - float(p2.elo_before)
                transfer_sum += abs(d1) + abs(d2)
                fights_count += 1
            out.append(
                {
                    'event_id': ev.event_id,
                    'event_name': getattr(ev, 'name', None),
                    'event_date': getattr(ev, 'event_date', None),
                    'shock': shock_sum,
                    'net_transfer': transfer_sum,
                    'fights': fights_count,
                }
            )
        reverse = (order or 'desc').lower() != 'asc'
        out.sort(key=lambda r: (r.get('shock') or 0.0), reverse=reverse)
        return out[: max(1, int(limit))]

    @with_uow
    async def sos(self, uow: UnitOfWork, fighter_id: str, *, window: str = 'days', n: int = 365) -> dict[str, Any]:
        """Strength of Schedule: opponent elo_before over recent window.

        window: 'days' or 'fights'. If 'days', keeps bouts within last n days; if 'fights', last n fights.
        Returns mean, median, p25, p75 of opponent elo_before.
        """
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return {'fighter_id': fighter_id, 'count': 0, 'mean': None, 'median': None, 'p25': None, 'p75': None}
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)
        obs: list[tuple[Any, float]] = []
        for p in parts:
            b = bouts_map.get(p.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if d is None:
                continue
            opp_before = None
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id and op.elo_before is not None:
                    opp_before = float(op.elo_before)
                    break
            if opp_before is None:
                continue
            obs.append((d, opp_before))
        if not obs:
            return {'fighter_id': fighter_id, 'count': 0, 'mean': None, 'median': None, 'p25': None, 'p75': None}
        obs.sort(key=lambda t: t[0])
        if window == 'fights':
            obs = obs[-max(1, int(n)) :]
        else:
            try:
                last = obs[-1][0]
                start = last - timedelta(days=max(1, int(n)))
                obs = [t for t in obs if t[0] >= start]
            except Exception:
                obs = obs[-max(1, int(n)) :]
        vals = [v for _, v in obs]
        vals.sort()

        def pct(p: float) -> float | None:
            if not vals:
                return None
            i = max(0, min(len(vals) - 1, round(p * (len(vals) - 1))))
            return float(vals[i])

        return {
            'fighter_id': fighter_id,
            'count': len(vals),
            'mean': float(sum(vals) / len(vals)) if vals else None,
            'median': float(stats.median(vals)) if vals else None,
            'p25': pct(0.25),
            'p75': pct(0.75),
        }

    @with_uow
    async def quality_wins(self, uow: UnitOfWork, fighter_id: str, *, elo_threshold: float) -> dict[str, Any]:
        """Count wins vs opponents with elo_before >= elo_threshold at fight time.
        Returns: wins_q, wins_total, share.
        """
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return {'fighter_id': fighter_id, 'wins_q': 0, 'wins_total': 0, 'share': None, 'threshold': elo_threshold}
        # Unused lookups removed
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)
        wins_total = 0
        wins_q = 0
        for p in parts:
            if p.outcome != FightOutcome.WIN:
                continue
            wins_total += 1
            opp_before = None
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id and op.elo_before is not None:
                    opp_before = float(op.elo_before)
                    break
            if opp_before is None:
                continue
            if float(opp_before) >= float(elo_threshold):
                wins_q += 1
        share = (wins_q / wins_total) if wins_total > 0 else None
        return {
            'fighter_id': fighter_id,
            'wins_q': wins_q,
            'wins_total': wins_total,
            'share': share,
            'threshold': elo_threshold,
        }

    @with_uow
    async def style_profile(self, uow: UnitOfWork, fighter_id: str) -> dict[str, Any]:
        """Control share and phase mix (distance/clinch/ground) over recorded bouts.
        Returns averages and totals for clarity.
        """
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        if not parts:
            return {'fighter_id': fighter_id, 'control_share': None, 'phase_mix': None}
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)
        ctrl_for = 0.0
        ctrl_against = 0.0
        dist = clin = grd = 0.0
        sig = 0.0
        fights = 0
        for p in parts:
            fights += 1
            ctrl_for += float(getattr(p, 'control_time_sec', 0) or 0)
            dist += float(getattr(p, 'distance_ss', 0) or 0)
            clin += float(getattr(p, 'clinch_ss', 0) or 0)
            grd += float(getattr(p, 'ground_ss', 0) or 0)
            sig += float(getattr(p, 'sig_strikes', 0) or 0)
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id:
                    ctrl_against += float(getattr(op, 'control_time_sec', 0) or 0)
                    break
        control_share = (ctrl_for / (ctrl_for + ctrl_against)) if (ctrl_for + ctrl_against) > 0 else None
        phase_mix = None
        if sig > 0:
            phase_mix = {'distance_pct': dist / sig, 'clinch_pct': clin / sig, 'ground_pct': grd / sig}
        return {
            'fighter_id': fighter_id,
            'fights': fights,
            'control_share': control_share,
            'phase_mix': phase_mix,
            'totals': {
                'distance_ss': dist,
                'clinch_ss': clin,
                'ground_ss': grd,
                'sig_strikes': sig,
                'control_for_sec': ctrl_for,
                'control_against_sec': ctrl_against,
            },
        }

    @with_uow
    async def top_fighters_by_elo(self, uow: UnitOfWork, limit: int = 20) -> list[Fighter]:
        ck = f'top_elo:{limit}'
        cached = self._cache_get(ck)
        if cached is not None:
            return cast(list[Fighter], cached)
        data = await uow.fighters.get_top_fighters_by_elo(limit=limit)
        self._cache_put(ck, data, ttl_sec=600)
        return data

    @with_uow
    async def top_fighters_by_peak_elo(self, uow: UnitOfWork, limit: int = 20) -> list[Fighter]:
        ck = f'top_peak:{limit}'
        cached = self._cache_get(ck)
        if cached is not None:
            return cast(list[Fighter], cached)
        data = await uow.fighters.get_top_fighters_by_peak_elo(limit=limit)
        self._cache_put(ck, data, ttl_sec=600)
        return data

    @with_uow
    async def top_fighters_by_elo_gain(self, uow: UnitOfWork, limit: int = 20) -> list[dict[str, Any]]:
        ck = f'top_gain:{limit}'
        c = self._cache_get(ck)
        if c is not None:
            return cast(list[dict[str, Any]], c)
        fighters: list[Fighter] = await uow.fighters.get_all()
        rows: list[tuple[str, str | None, float]] = []
        for f in fighters:
            if f.entry_elo is None or f.current_elo is None:
                continue
            rows.append((f.fighter_id, f.name, float(f.current_elo) - float(f.entry_elo)))
        rows.sort(key=lambda t: t[2], reverse=True)
        out = [{'fighter_id': fid, 'name': name, 'value': val} for fid, name, val in rows[: max(1, int(limit))]]
        self._cache_put(ck, out, ttl_sec=600)
        return out

    @with_uow
    async def top_fighters_by_peak_elo_gain(self, uow: UnitOfWork, limit: int = 20) -> list[dict[str, Any]]:
        ck = f'top_peak_gain:{limit}'
        c = self._cache_get(ck)
        if c is not None:
            return cast(list[dict[str, Any]], c)
        fighters: list[Fighter] = await uow.fighters.get_all()
        rows: list[tuple[str, str | None, float]] = []
        for f in fighters:
            if f.entry_elo is None or f.peak_elo is None:
                continue
            rows.append((f.fighter_id, f.name, float(f.peak_elo) - float(f.entry_elo)))
        rows.sort(key=lambda t: t[2], reverse=True)
        out = [{'fighter_id': fid, 'name': name, 'value': val} for fid, name, val in rows[: max(1, int(limit))]]
        self._cache_put(ck, out, ttl_sec=600)
        return out

    @with_uow
    async def fighter_elo_history(self, uow: UnitOfWork, fighter_id: str) -> tuple[Fighter | None, list[EloPoint]]:
        fighter = await uow.fighters.get_by_fighter_id(fighter_id)
        if not fighter:
            return None, []
        points = await self._build_fighter_points(uow, fighter_id)
        await self._compute_ranks(uow, points)
        return fighter, points

    @with_uow
    async def top_elo_gains(self, uow: UnitOfWork, limit: int = 20) -> list[EloChange]:
        ck = f'high_gains:{limit}'
        c = self._cache_get(ck)
        if c is not None:
            return cast(list[EloChange], c)
        items = await self._all_elo_changes(uow)
        # Filter positive deltas; exclude NO_CONTEST
        items = [x for x in items if x.delta > 0 and x.outcome != 'NC']
        items.sort(key=lambda x: x.delta, reverse=True)
        out = items[:limit]
        self._cache_put(ck, out, ttl_sec=600)
        return out

    @with_uow
    async def lowest_elo_gains(self, uow: UnitOfWork, limit: int = 20) -> list[EloChange]:
        ck = f'low_gains:{limit}'
        c = self._cache_get(ck)
        if c is not None:
            return cast(list[EloChange], c)
        items = await self._all_elo_changes(uow)
        items = [x for x in items if x.delta > 0 and x.outcome != 'NC']
        items.sort(key=lambda x: x.delta)
        out = items[:limit]
        self._cache_put(ck, out, ttl_sec=600)
        return out

    @with_uow
    async def top_elo_losses(self, uow: UnitOfWork, limit: int = 20) -> list[EloChange]:
        ck = f'high_losses:{limit}'
        c = self._cache_get(ck)
        if c is not None:
            return cast(list[EloChange], c)
        items = await self._all_elo_changes(uow)
        items = [x for x in items if x.delta < 0 and x.outcome != 'NC']
        # Most negative first
        items.sort(key=lambda x: x.delta)
        out = items[:limit]
        self._cache_put(ck, out, ttl_sec=600)
        return out

    async def _all_elo_changes(self, uow: UnitOfWork) -> list[EloChange]:
        """Build a list of elo changes with opponent and event metadata for all participations."""
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        # Build maps for efficient lookups
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}

        # Group participants by bout to identify opponents quickly
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for bp in bps:
            by_bout[bp.bout_id].append(bp)

        outcome_map = {
            FightOutcome.WIN: 'W',
            FightOutcome.LOSS: 'L',
            FightOutcome.DRAW: 'D',
            FightOutcome.NO_CONTEST: 'NC',
        }

        out: list[EloChange] = []
        for bp in bps:
            before = float(bp.elo_before) if bp.elo_before is not None else None
            after = float(bp.elo_after) if bp.elo_after is not None else None
            if before is None or after is None:
                continue
            delta = after - before
            b = bouts_map.get(bp.bout_id)
            ev = events_map.get(b.event_id) if b and b.event_id else None
            # Opponent
            opp_id = None
            opp_name = None
            opp_elo_before = None
            for other in by_bout.get(bp.bout_id, []):
                if other.fighter_id != bp.fighter_id:
                    opp_id = other.fighter_id
                    opp_f = fighters_map.get(opp_id)
                    opp_name = opp_f.name if opp_f else None
                    if other.elo_before is not None:
                        opp_elo_before = float(other.elo_before)
                    break
            f = fighters_map.get(bp.fighter_id)
            out.append(
                EloChange(
                    bout_id=bp.bout_id,
                    fighter_id=bp.fighter_id,
                    fighter_name=f.name if f else None,
                    opponent_id=opp_id,
                    opponent_name=opp_name,
                    opponent_elo_before=opp_elo_before,
                    delta=delta,
                    elo_before=before,
                    elo_after=after,
                    outcome=outcome_map.get(bp.outcome),
                    event_id=ev.event_id if ev else None,
                    event_name=getattr(ev, 'name', None),
                    event_date=getattr(ev, 'event_date', None),
                )
            )
        # Stable order: by absolute date then delta, mostly for determinism in ties
        out.sort(key=lambda x: (x.event_date or date_type.min, x.delta), reverse=False)
        return out

    @with_uow
    async def elo_movers(  # noqa: PLR0912
        self, uow: UnitOfWork, *, direction: str = 'gains', window_days: int | None = None, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Aggregate elo deltas per fighter over an optional recent window."""

        cache = CacheManager(ttl=300)
        cache_key = f'analytics:elo_movers:{direction}:{window_days}:{limit}'
        cached = await cache.get_json(cache_key)
        if cached is not None:
            return cast(list[dict[str, Any]], cached)

        items = await self._all_elo_changes(uow)
        if window_days and window_days > 0:
            cutoff = date_type.today() - timedelta(days=int(window_days))
            items = [x for x in items if x.event_date and x.event_date >= cutoff]

        direction_norm = (direction or 'gains').lower()
        if direction_norm == 'losses':
            filtered = [x for x in items if x.delta < 0]
        elif direction_norm == 'net':
            filtered = items[:]
        else:
            filtered = [x for x in items if x.delta > 0]

        aggregates: dict[str, dict[str, Any]] = {}
        for chg in filtered:
            entry = aggregates.setdefault(
                chg.fighter_id,
                {
                    'fighter_id': chg.fighter_id,
                    'fighter_name': chg.fighter_name,
                    'delta': 0.0,
                    'fights': 0,
                    'avg_opponent_elo_sum': 0.0,
                    'avg_opponent_elo_count': 0,
                    'last_event_id': None,
                    'last_event_name': None,
                    'last_event_date': None,
                    'last_bout_id': None,
                },
            )
            entry['delta'] += float(chg.delta)
            entry['fights'] += 1
            if chg.opponent_elo_before is not None:
                entry['avg_opponent_elo_sum'] += float(chg.opponent_elo_before)
                entry['avg_opponent_elo_count'] += 1
            if chg.event_date is not None:
                prev_date = entry['last_event_date']
                if prev_date is None or chg.event_date > prev_date:
                    entry['last_event_date'] = chg.event_date
                    entry['last_event_id'] = chg.event_id
                    entry['last_event_name'] = chg.event_name
                    entry['last_bout_id'] = chg.bout_id

        output: list[dict[str, Any]] = []
        for data in aggregates.values():
            fights = int(data['fights'])
            if fights == 0:
                continue
            opp_count = int(data['avg_opponent_elo_count'])
            avg_opp = float(data['avg_opponent_elo_sum']) / opp_count if opp_count else None
            output.append(
                {
                    'fighter_id': data['fighter_id'],
                    'fighter_name': data['fighter_name'],
                    'delta': data['delta'],
                    'fights': fights,
                    'avg_opponent_elo': avg_opp,
                    'last_event_id': data['last_event_id'],
                    'last_event_name': data['last_event_name'],
                    'last_event_date': data['last_event_date'],
                    'last_bout_id': data['last_bout_id'],
                }
            )

        if direction_norm == 'losses':
            output.sort(key=lambda r: r['delta'])
        elif direction_norm == 'net':
            output.sort(key=lambda r: abs(r['delta']), reverse=True)
        else:
            output.sort(key=lambda r: r['delta'], reverse=True)

        trimmed = output[: max(1, int(limit))]
        await cache.set_json(cache_key, trimmed)
        return trimmed

    @with_uow
    async def random_bouts(self, uow: UnitOfWork, limit: int = 10) -> list[dict[str, Any]]:
        """Return a random selection of unique bouts with fighters and outcomes as fun facts.

        Each item: { bout_id, event_name, event_date, method, round_num, fighters: [ {id,name,outcome,delta} ] }
        """
        # Build per-bout groups from participants
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for bp in bps:
            by_bout[bp.bout_id].append(bp)

        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}

        bout_ids = list(by_bout.keys())
        if not bout_ids:
            return []
        random.shuffle(bout_ids)
        out: list[dict[str, Any]] = []
        for bid in bout_ids[: limit if limit > 0 else 10]:
            parts = by_bout.get(bid, [])
            fighters_info = []
            for p in parts:
                before = float(p.elo_before) if p.elo_before is not None else None
                after = float(p.elo_after) if p.elo_after is not None else None
                delta = (after - before) if (before is not None and after is not None) else None
                f = fighters_map.get(p.fighter_id)
                fighters_info.append(
                    {
                        'id': p.fighter_id,
                        'name': f.name if f else None,
                        'outcome': (
                            'W'
                            if p.outcome == FightOutcome.WIN
                            else 'L'
                            if p.outcome == FightOutcome.LOSS
                            else 'D'
                            if p.outcome == FightOutcome.DRAW
                            else 'NC'
                        ),
                        'delta': delta,
                    }
                )
            b = bouts_map.get(bid)
            ev = events_map.get(b.event_id) if b and b.event_id else None
            out.append(
                {
                    'bout_id': bid,
                    'event_name': getattr(ev, 'name', None),
                    'event_date': getattr(ev, 'event_date', None),
                    'method': getattr(b, 'method', None) if b else None,
                    'round_num': getattr(b, 'round_num', None) if b else None,
                    'fighters': fighters_info,
                }
            )
        return out

    @with_uow
    async def latest_event_elo(self, uow: UnitOfWork) -> dict[str, Any]:
        """Return ELO deltas for the latest event.

        Entries are one per bout (not per participant). Title fights first, then by max absolute delta desc.

        Response: { event_id, event_name, event_date, entries: [ { bout_id, is_title_fight,
          fighter1_id, fighter1_name, fighter1_outcome, fighter1_delta,
          fighter2_id, fighter2_name, fighter2_outcome, fighter2_delta } ] }
        """
        events = await uow.events.get_all()
        if not events:
            return {'event_id': None, 'event_name': None, 'event_date': None, 'entries': []}
        # pick latest by event_date
        latest = max(events, key=lambda e: getattr(e, 'event_date', date_type.min) or date_type.min)
        bouts = [b for b in await uow.bouts.get_all() if b.event_id == latest.event_id]
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for bp in await uow.bout_participants.get_all():
            if any(bp.bout_id == b.bout_id for b in bouts):
                by_bout[bp.bout_id].append(bp)
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}
        outcome_map = {
            FightOutcome.WIN: 'W',
            FightOutcome.LOSS: 'L',
            FightOutcome.DRAW: 'D',
            FightOutcome.NO_CONTEST: 'NC',
        }
        entries: list[dict[str, Any]] = []
        for b in bouts:
            parts = by_bout.get(b.bout_id, [])
            if len(parts) < MIN_PARTICIPANTS:
                continue
            p1, p2 = parts[0], parts[1]

            def dval(p: BoutParticipant) -> tuple[float | None, float | None, float | None]:
                be = float(p.elo_before) if p.elo_before is not None else None
                af = float(p.elo_after) if p.elo_after is not None else None
                de = (af - be) if (be is not None and af is not None) else None
                return be, af, de

            _b1, _a1, d1 = dval(p1)
            _b2, _a2, d2 = dval(p2)
            if d1 is None or d2 is None:
                continue
            f1 = fighters_map.get(p1.fighter_id)
            f2 = fighters_map.get(p2.fighter_id)
            entries.append(
                {
                    'bout_id': b.bout_id,
                    'is_title_fight': bool(getattr(b, 'is_title_fight', False)),
                    'fighter1_id': p1.fighter_id,
                    'fighter1_name': f1.name if f1 else None,
                    'fighter1_outcome': outcome_map.get(p1.outcome),
                    'fighter1_delta': d1,
                    'fighter2_id': p2.fighter_id,
                    'fighter2_name': f2.name if f2 else None,
                    'fighter2_outcome': outcome_map.get(p2.outcome),
                    'fighter2_delta': d2,
                    'method': getattr(b, 'method', None),
                }
            )
        # Sort: title fights first, then by max abs delta among the two fighters
        entries.sort(
            key=lambda x: (0 if x['is_title_fight'] else 1, -max(abs(x['fighter1_delta']), abs(x['fighter2_delta'])))
        )
        shock_metrics = await self.event_shock(event_id=str(latest.event_id))
        title_count = sum(1 for entry in entries if entry.get('is_title_fight'))
        return {
            'event_id': latest.event_id,
            'event_name': getattr(latest, 'name', None),
            'event_date': getattr(latest, 'event_date', None),
            'entries': entries,
            'shock_index': shock_metrics.get('shock') if isinstance(shock_metrics, dict) else None,
            'net_transfer': shock_metrics.get('net_transfer') if isinstance(shock_metrics, dict) else None,
            'title_bouts': title_count,
        }

    @with_uow
    async def event_elo(self, uow: UnitOfWork, event_id: str) -> dict[str, Any]:
        """Return ELO deltas for a specific event by `event_id`.

        Shape mirrors `latest_event_elo` for UI reuse.
        """
        # Repository expects a UUID; accept string ids from API and coerce safely.
        try:
            eid = UUID(event_id) if isinstance(event_id, str) else event_id
        except Exception:
            return {'event_id': event_id, 'event_name': None, 'event_date': None, 'entries': []}
        target = await uow.events.get_by_event_id(eid)
        if not target:
            return {'event_id': event_id, 'event_name': None, 'event_date': None, 'entries': []}
        bouts = [b for b in await uow.bouts.get_all() if b.event_id == target.event_id]
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for bp in await uow.bout_participants.get_all():
            if any(bp.bout_id == b.bout_id for b in bouts):
                by_bout[bp.bout_id].append(bp)
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}
        outcome_map = {
            FightOutcome.WIN: 'W',
            FightOutcome.LOSS: 'L',
            FightOutcome.DRAW: 'D',
            FightOutcome.NO_CONTEST: 'NC',
        }
        entries: list[dict[str, Any]] = []
        for b in bouts:
            parts = by_bout.get(b.bout_id, [])
            if len(parts) < MIN_PARTICIPANTS:
                continue
            p1, p2 = parts[0], parts[1]

            def dval(p: BoutParticipant) -> tuple[float | None, float | None, float | None]:
                be = float(p.elo_before) if p.elo_before is not None else None
                af = float(p.elo_after) if p.elo_after is not None else None
                de = (af - be) if (be is not None and af is not None) else None
                return be, af, de

            _, _, d1 = dval(p1)
            _, _, d2 = dval(p2)
            if d1 is None or d2 is None:
                continue
            f1 = fighters_map.get(p1.fighter_id)
            f2 = fighters_map.get(p2.fighter_id)
            entries.append(
                {
                    'bout_id': b.bout_id,
                    'is_title_fight': bool(getattr(b, 'is_title_fight', False)),
                    'fighter1_id': p1.fighter_id,
                    'fighter1_name': f1.name if f1 else None,
                    'fighter1_outcome': outcome_map.get(p1.outcome),
                    'fighter1_delta': d1,
                    'fighter2_id': p2.fighter_id,
                    'fighter2_name': f2.name if f2 else None,
                    'fighter2_outcome': outcome_map.get(p2.outcome),
                    'fighter2_delta': d2,
                    'method': getattr(b, 'method', None),
                }
            )
        entries.sort(
            key=lambda x: (0 if x['is_title_fight'] else 1, -max(abs(x['fighter1_delta']), abs(x['fighter2_delta'])))
        )
        shock_metrics = await self.event_shock(event_id=str(target.event_id))
        title_count = sum(1 for entry in entries if entry.get('is_title_fight'))
        return {
            'event_id': target.event_id,
            'event_name': getattr(target, 'name', None),
            'event_date': getattr(target, 'event_date', None),
            'entries': entries,
            'shock_index': shock_metrics.get('shock') if isinstance(shock_metrics, dict) else None,
            'net_transfer': shock_metrics.get('net_transfer') if isinstance(shock_metrics, dict) else None,
            'title_bouts': title_count,
        }

    @with_uow
    async def events_list(self, uow: UnitOfWork) -> list[dict[str, Any]]:
        """Return all events with minimal fields, sorted by date ascending (None dates first)."""
        evs = await uow.events.get_all()
        # Sort by date, None first, then by name to stabilize
        evs_sorted = sorted(
            evs, key=lambda e: ((getattr(e, 'event_date', None) or date_type.min), getattr(e, 'name', ''))
        )
        out: list[dict[str, Any]] = []
        for e in evs_sorted:
            out.append(
                {
                    'event_id': getattr(e, 'event_id', None),
                    'name': getattr(e, 'name', None),
                    'event_date': getattr(e, 'event_date', None),
                }
            )
        return out

    @with_uow
    async def top_fighter_stats(  # noqa: PLR0912, PLR0915
        self,
        uow: UnitOfWork,
        metric: str,
        limit: int = 20,
        *,
        since_year: int | None = None,
        division: int | None = None,
        rate: str = 'total',  # 'total' | 'per15'
        adjusted: bool = False,
    ) -> list[dict[str, Any]]:
        """Top fighters by aggregated stat over their recorded bouts.

        Supported metrics: 'kd', 'td', 'td_attempts', 'sub_attempts', 'reversals',
        'control_time_sec', 'sig_strikes', 'sig_strikes_thrown', 'total_strikes', 'total_strikes_thrown',
        'head_ss', 'body_ss', 'leg_ss', 'distance_ss', 'clinch_ss', 'ground_ss'.
        Returns list of { fighter_id, name, value, fights }.
        """
        allowed = {
            'kd',
            'td',
            'td_attempts',
            'sub_attempts',
            'reversals',
            'control_time_sec',
            'sig_strikes',
            'sig_strikes_thrown',
            'total_strikes',
            'total_strikes_thrown',
            'head_ss',
            'body_ss',
            'leg_ss',
            'distance_ss',
            'clinch_ss',
            'ground_ss',
        }
        if metric not in allowed:
            raise ValueError(f'Unsupported metric: {metric}')
        parts: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        totals: dict[str, float] = defaultdict(float)
        # Track total minutes per fighter to enable per-15 normalization
        minutes_map: dict[str, float] = defaultdict(float)
        # For adjusted path, accumulate raw differentials (own - opp) separately
        raw_diff_map: dict[str, float] = defaultdict(float)
        fights: dict[str, int] = defaultdict(int)
        for p in parts:
            b = bouts_map.get(p.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if since_year is not None and (d is None or d.year < since_year):
                continue
            if division is not None and (not b or getattr(b, 'weight_class_code', None) != division):
                continue
            # Compute minutes if needed
            mins = 0.0
            if rate == 'per15' or adjusted:
                try:
                    is_decision = bool(getattr(b, 'method', '') and 'Decision' in getattr(b, 'method', ''))
                    if is_decision:
                        mins = (
                            25.0
                            if bool(
                                getattr(b, 'is_title_fight', False)
                                or (getattr(b, 'time_format', '') and '5 Rnd' in getattr(b, 'time_format', ''))
                            )
                            else 15.0
                        )
                    else:
                        rnum = int(getattr(b, 'round_num', 1) or 1)
                        tsec = int(getattr(b, 'time_sec', 0) or 0)
                        mins = ((rnum - 1) * 300 + tsec) / 60.0
                except Exception:
                    mins = 0.0
            try:
                v = float(getattr(p, metric, 0) or 0)
            except Exception:
                v = 0.0
            if adjusted:
                # Differential against opponent in the same bout
                opp: BoutParticipant | None = None
                for op in (bp for bp in parts if bp.bout_id == p.bout_id and bp.fighter_id != p.fighter_id):
                    opp = op
                    break
                diff_raw = 0.0
                if opp is not None:
                    try:
                        ov = float(getattr(opp, metric, 0) or 0)
                    except Exception:
                        ov = 0.0
                    diff_raw = v - ov
                # Accumulate raw differential; normalization handled after loop
                raw_diff_map[p.fighter_id] += diff_raw
            else:
                # Unadjusted path: accumulate raw metric totals
                totals[p.fighter_id] += v
            fights[p.fighter_id] += 1
            if mins > 0:
                minutes_map[p.fighter_id] += mins
        # Post-process totals depending on flags
        if adjusted:
            if rate == 'per15':
                for fid, diff in list(raw_diff_map.items()):
                    mins = minutes_map.get(fid, 0.0)
                    totals[fid] = (diff / mins * 15.0) if mins > 0 else 0.0
            else:
                # rate == 'total': return raw differential sums
                totals = raw_diff_map
        # Unadjusted path: optionally convert raw totals to per-15
        elif rate == 'per15':
            for fid, tot in list(totals.items()):
                mins = minutes_map.get(fid, 0.0)
                totals[fid] = (tot / mins * 15.0) if mins > 0 else 0.0
        # Sort by value desc
        items = sorted(totals.items(), key=lambda kv: -kv[1])[: max(1, int(limit))]
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}
        out: list[dict[str, Any]] = []
        for fid, val in items:
            f = fighters_map.get(fid)
            out.append(
                {'fighter_id': fid, 'name': getattr(f, 'name', None), 'value': val, 'fights': fights.get(fid, 0)}
            )
        return out

    @with_uow
    async def plus_minus(  # noqa: PLR0912, PLR0915
        self,
        uow: UnitOfWork,
        fighter_id: str,
        *,
        metric: str = 'sig_strikes',
        since_year: int | None = None,
        opp_window_months: int = 18,
    ) -> dict[str, Any]:
        """Opponent-adjusted plus/minus for a fighter: own per-minute rate minus opponent's typical allowed rate."""
        allowed_metrics = {
            'kd',
            'td',
            'td_attempts',
            'sub_attempts',
            'control_time_sec',
            'sig_strikes',
            'total_strikes',
        }
        if metric not in allowed_metrics:
            raise ValueError('Unsupported metric for plus/minus')
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)
        num = 0.0
        denom = 0
        for p in parts:
            b = bouts_map.get(p.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if d is None:
                continue
            if since_year is not None and d.year < int(since_year):
                continue
            # compute minutes for this fight
            minutes = 0.0
            with contextlib.suppress(Exception):
                is_decision = bool(getattr(b, 'method', '') and 'Decision' in getattr(b, 'method', ''))
                if is_decision:
                    scheduled5 = bool(
                        getattr(b, 'is_title_fight', False)
                        or (getattr(b, 'time_format', '') and '5 Rnd' in getattr(b, 'time_format', ''))
                    )
                    minutes = 25.0 if scheduled5 else 15.0
                else:
                    rnum = int(getattr(b, 'round_num', 1) or 1)
                    tsec = int(getattr(b, 'time_sec', 0) or 0)
                    minutes = ((rnum - 1) * 300 + tsec) / 60.0
            if minutes <= 0:
                continue
            own = float(getattr(p, metric, 0) or 0) / minutes
            # opponent typical allowed per-minute around this date
            opp: BoutParticipant | None = None
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id:
                    opp = op
                    break
            if not opp:
                continue
            opp_parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(opp.fighter_id)
            # collect opponent's other bouts within window
            wstart = d - timedelta(days=30 * max(1, opp_window_months))
            vals: list[float] = []
            for op2 in opp_parts:
                if op2.bout_id == p.bout_id:
                    continue
                b2 = bouts_map.get(op2.bout_id)
                e2 = events_map.get(b2.event_id) if b2 and b2.event_id else None
                d2 = getattr(e2, 'event_date', None)
                if d2 is None or d2 < wstart or d2 > d:
                    continue
            with contextlib.suppress(Exception):
                is_dec2 = bool(getattr(b2, 'method', '') and 'Decision' in getattr(b2, 'method', ''))
                if is_dec2:
                    scheduled5b = bool(
                        getattr(b2, 'is_title_fight', False)
                        or (getattr(b2, 'time_format', '') and '5 Rnd' in getattr(b2, 'time_format', ''))
                    )
                    mins2 = 25.0 if scheduled5b else 15.0
                else:
                    rnum2 = int(getattr(b2, 'round_num', 1) or 1)
                    tsec2 = int(getattr(b2, 'time_sec', 0) or 0)
                    mins2 = ((rnum2 - 1) * 300 + tsec2) / 60.0
                v2 = float(getattr(op2, metric, 0) or 0) / max(1e-6, mins2)
                vals.append(v2)
            opp_allowed = (sum(vals) / len(vals)) if vals else 0.0
            num += own - opp_allowed
            denom += 1
        return {
            'fighter_id': fighter_id,
            'metric': metric,
            'plus_minus_per_min': (num / denom) if denom > 0 else 0.0,
            'samples': denom,
        }

    @with_uow
    async def consistency_versatility(  # noqa: PLR0915
        self, uow: UnitOfWork, fighter_id: str, k: int = 6
    ) -> dict[str, Any]:
        """Return SD of ELO deltas (last k), coefficient of variation for basic per-minute rates, and method entropy."""
        parts: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        bps = sorted(parts, key=lambda p: (p.bout_id or ''))[-max(2, int(k)) :]
        deltas: list[float] = []
        for p in bps:
            if p.elo_before is None or p.elo_after is None:
                continue
            deltas.append(float(p.elo_after) - float(p.elo_before))
        min_points = 2

        def stdev(arr: list[float]) -> float | None:
            if len(arr) < min_points:
                return None
            m = sum(arr) / len(arr)
            v = sum((x - m) ** 2 for x in arr) / (len(arr) - 1)
            return float(v**0.5)

        sd_delta = stdev(deltas)

        # crude per-minute stats variation
        def fight_minutes(b: Bout) -> float:
            try:
                is_dec = bool(getattr(b, 'method', '') and 'Decision' in getattr(b, 'method', ''))
                if is_dec:
                    return (
                        25.0
                        if bool(
                            getattr(b, 'is_title_fight', False)
                            or (getattr(b, 'time_format', '') and '5 Rnd' in getattr(b, 'time_format', ''))
                        )
                        else 15.0
                    )
                rnum = int(getattr(b, 'round_num', 1) or 1)
                tsec = int(getattr(b, 'time_sec', 0) or 0)
                return ((rnum - 1) * 300 + tsec) / 60.0
            except Exception:
                return 0.0

        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        rates = []
        for p in bps:
            b = bouts_map.get(p.bout_id)
            if not b:
                continue
            mins = fight_minutes(b)
            if mins <= 0:
                continue
            sigpm = float(p.sig_strikes or 0) / mins
            tdpm = float(p.td or 0) / mins
            ctrlpm = float(p.control_time_sec or 0) / mins
            rates.append((sigpm, tdpm, ctrlpm))

        def cv(values: list[float]) -> float | None:
            vs = [v for v in values if v is not None]
            if len(vs) < min_points:
                return None
            m = sum(vs) / len(vs)
            if m == 0:
                return None
            s = stdev(vs)
            return (s / m) if s is not None else None

        sig_cv = cv([r[0] for r in rates])
        td_cv = cv([r[1] for r in rates])
        ctrl_cv = cv([r[2] for r in rates])
        # method entropy for wins
        bouts_map2: dict[str, Bout] = bouts_map
        # events_map not required here
        counts = {'KO_TKO': 0, 'TKO_DS': 0, 'SUB': 0, 'DEC': 0}
        total = 0
        for p in parts:
            if p.outcome != FightOutcome.WIN:
                continue
            b = bouts_map2.get(p.bout_id)
            mclass = method_class(getattr(b, 'method', '') if b else '')
            if mclass in ('KO_TKO', 'TKO_DS', 'SUB'):
                counts[mclass] += 1
            else:
                counts['DEC'] += 1
            total += 1
        entropy = None
        if total > 0:
            probs = [c / total for c in counts.values()]
            entropy = -sum(p * log2(p) for p in probs if p > 0) / log2(4)
        return {
            'fighter_id': fighter_id,
            'sd_elo_delta': sd_delta,
            'cv_sig_per_min': sig_cv,
            'cv_td_per_min': td_cv,
            'cv_ctrl_per_min': ctrl_cv,
            'versatility': entropy,
        }

    @with_uow
    async def divisions(self, uow: UnitOfWork) -> list[dict[str, Any]]:
        """Available divisions observed in bouts, with labels."""
        bouts = await uow.bouts.get_all()
        code_set: set[int] = set()
        for b in bouts:
            c = getattr(b, 'weight_class_code', None)
            if isinstance(c, int):
                code_set.add(c)
        codes: list[int] = sorted(code_set)

        def label(code: int) -> str:
            try:
                wc = WeightClassCode(code)
                name = wc.name.replace('MEN_', '').replace('WOMEN_', 'Women ')
                return name.replace('_', ' ').title()
            except Exception:
                return str(code)

        return [{'code': c, 'label': label(c)} for c in codes]

    @with_uow
    async def division_rankings(  # noqa: PLR0912, PLR0915
        self,
        uow: UnitOfWork,
        division: int,
        *,
        metric: str = 'current',  # 'current' | 'peak' | 'gains'
        year: int | None = None,
        active_only: bool = False,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Rank fighters by division under different metrics.

        - metric='current': order by Fighter.current_elo among fighters who fought in division
        - metric='peak': order by Fighter.peak_elo among fighters who fought in division
        - metric='gains': sum of ELO deltas in given year within that division (requires year)

        year: when provided for 'current'/'peak', requires at least one bout in div that year.
        active_only: requires at least one bout in this division within last 24 months.
        """
        # Build indexes
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}

        # Filter participations to this division
        div_parts: list[tuple[BoutParticipant, Bout, Event | None]] = []
        for p in bps:
            b = bouts_map.get(p.bout_id)
            if not b or int(b.weight_class_code or 0) != int(division):
                continue
            ev = events_map.get(b.event_id) if b.event_id else None
            if year is not None and (
                not ev or getattr(ev, 'event_date', None) is None or ev.event_date.year != int(year)
            ):
                continue
            div_parts.append((p, b, ev))

        # Do not prune parts here; active_only will be applied with nuanced rules later

        # Count fights per fighter within the filtered set (division-only)
        fights_in_div: dict[str, int] = defaultdict(int)
        for p, _b, _ev in div_parts:
            fights_in_div[p.fighter_id] += 1

        # Build global per-fighter fight dates across all divisions to evaluate activity accurately
        dates_by_fid_any: dict[str, list[date_type]] = defaultdict(list)
        for bp in bps:
            b = bouts_map.get(bp.bout_id)
            if not b or not b.event_id:
                continue
            ev = events_map.get(b.event_id)
            d = getattr(ev, 'event_date', None)
            if d is not None:
                dates_by_fid_any[bp.fighter_id].append(d)

        def _is_active(fid: str) -> bool:
            today = date.today()
            cutoff12 = today - timedelta(days=365)
            cutoff24 = today - timedelta(days=730)
            dts = sorted(dates_by_fid_any.get(fid, []))
            if not dts:
                return False
            last = dts[-1]
            fights_12 = sum(1 for d in dts if d >= cutoff12)
            fights_24 = sum(1 for d in dts if d >= cutoff24)
            days_since = (today - last).days
            if fights_24 == 0 and days_since > RETIRED_LAST_DAYS:
                return False
            # Active rules
            return fights_12 >= ACTIVE_F12_MIN or fights_24 >= ACTIVE_F24_MIN or days_since <= ACTIVE_LAST_DAYS

        # For 'gains', aggregate deltas per fighter within filtered parts (year should be provided)
        if metric == 'gains':
            sums: dict[str, float] = defaultdict(float)
            fights: dict[str, int] = defaultdict(int)
            for p, _b, _ev in div_parts:
                before = float(p.elo_before) if p.elo_before is not None else None
                after = float(p.elo_after) if p.elo_after is not None else None
                if before is None or after is None:
                    continue
                sums[p.fighter_id] += after - before
                fights[p.fighter_id] += 1
            # For all-time (no year), require at least 3 divisional fights; for yearly allow >=1
            min_required = 1 if year is not None else 3
            filtered = [(fid, val) for fid, val in sums.items() if fights.get(fid, 0) >= min_required]
            # Apply activity filter if requested (use global activity, not division-only)
            if active_only:
                filtered = [(fid, val) for fid, val in filtered if _is_active(fid)]
            items = sorted(filtered, key=lambda kv: kv[1], reverse=True)[: max(1, int(limit))]
            out: list[dict[str, Any]] = []
            for fid, val in items:
                f = fighters_map.get(fid)
                out.append(
                    {'fighter_id': fid, 'name': getattr(f, 'name', None), 'value': val, 'fights': fights.get(fid, 0)}
                )
            return out

        # For 'current'/'peak', collect eligible fighters and sort by attribute
        eligible_ids = {p.fighter_id for (p, _b, _ev) in div_parts}

        # Apply active filter using 18/24/36 month rules (global activity, not division-restricted)
        if active_only:
            eligible_ids = {fid for fid in eligible_ids if _is_active(fid)}
        # Apply minimum fights threshold: for all-time (no year) require >=3; for year-filter allow >=1.
        # When asking for current with active_only, do not enforce min fights beyond activity recency.
        if not (metric == 'current' and active_only):
            min_required = 1 if year is not None else 3
            eligible_ids = {fid for fid in eligible_ids if fights_in_div.get(fid, 0) >= min_required}
        rows: list[tuple[str, float]] = []
        for fid in eligible_ids:
            f = fighters_map.get(fid)
            if not f:
                continue
            raw_val: Any = getattr(f, 'current_elo' if metric == 'current' else 'peak_elo', None)
            v: float | None = float(raw_val) if isinstance(raw_val, (int, float)) else None
            if v is None:
                continue
            rows.append((fid, v))
        rows.sort(key=lambda x: x[1], reverse=True)
        result_rows: list[dict[str, Any]] = []
        for fid, val in rows[: max(1, int(limit))]:
            f = fighters_map.get(fid)
            result_rows.append({'fighter_id': fid, 'name': getattr(f, 'name', None), 'value': val})
        return result_rows

    async def _build_fighter_points(self, uow: UnitOfWork, fighter_id: str) -> list[EloPoint]:
        participations: list[BoutParticipant] = await uow.bout_participants.get_by_fighter_id(fighter_id)
        points: list[EloPoint] = []
        for bp in participations:
            bout: Bout | None = await uow.bouts.get_by_bout_id(bp.bout_id)
            ev: Event | None = await uow.events.get_by_event_id(bout.event_id) if bout and bout.event_id else None

            opponent_id: str | None = None
            opponent_name: str | None = None
            try:
                all_in_bout = await uow.bout_participants.get_by_bout_id(bp.bout_id)
                for other in all_in_bout:
                    if other.fighter_id != bp.fighter_id:
                        opponent_id = other.fighter_id
                        opp = await uow.fighters.get_by_fighter_id(opponent_id)
                        opponent_name = opp.name if opp else None
                        break
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(f'Failed opponent resolution for bout {bp.bout_id}: {exc}')

            outcome_map = {
                FightOutcome.WIN: 'W',
                FightOutcome.LOSS: 'L',
                FightOutcome.DRAW: 'D',
                FightOutcome.NO_CONTEST: 'NC',
            }
            res_letter = outcome_map.get(bp.outcome)
            before = float(bp.elo_before) if bp.elo_before is not None else None
            after = float(bp.elo_after) if bp.elo_after is not None else None
            delta = (after - before) if (before is not None and after is not None) else None

            points.append(
                EloPoint(
                    bout_id=bp.bout_id,
                    event_id=ev.event_id if ev else None,
                    event_date=getattr(ev, 'event_date', None),
                    event_link=getattr(ev, 'event_link', None),
                    event_stats_link=getattr(ev, 'event_stats_link', None),
                    opponent_id=opponent_id,
                    opponent_name=opponent_name,
                    result=res_letter,
                    elo_before=before,
                    elo_after=after,
                    delta=delta,
                    event_name=getattr(ev, 'name', None),
                    rank_after=None,
                    is_title_fight=getattr(bout, 'is_title_fight', None) if bout else None,
                    weight_class_code=getattr(bout, 'weight_class_code', None) if bout else None,
                )
            )
        # Order by date, then by Elo before (earlier fights that night tend to have lower pre-fight Elo), then stable by bout_id
        points.sort(
            key=lambda p: (
                p.event_date or date_type.min,
                (p.elo_before if p.elo_before is not None else float('-inf')),
                p.bout_id,
            )
        )
        return points

    @with_uow
    async def rankings_history(  # noqa: PLR0912, PLR0915
        self,
        uow: UnitOfWork,
        interval: str = 'year',
        start_year: int | None = None,
        end_year: int | None = None,
        top: int = 15,
    ) -> list[dict[str, Any]]:
        """Compute snapshots of top ELO rankings over time.

        Currently supports yearly snapshots. Returns a list of snapshots:
        [{ 'label': 'YYYY', 'date': ISO-date, 'entries': [{ fighter_id, name, elo, rank }] }]
        """
        # Note: 'interval' reserved for future (e.g., 'month'). For now, only 'year' is supported.
        if interval != 'year':  # keep argument used to satisfy linter and provide guardrail
            logger.warning("rankings_history: unsupported interval '%s', defaulting to 'year'", interval)

        # Prepare per-fighter chronological elo_after values keyed by event date
        # Prefer Redis cache for cross-process reuse
        cache = CacheManager(ttl=900)
        ck = f'analytics:rank_hist:year:{start_year}:{end_year}:{top}'
        c = await cache.get_json(ck)
        if c is not None:
            return c  # type: ignore[return-value]
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}

        per_fighter: dict[str, list[tuple[date_type | None, float | None]]] = defaultdict(list)
        all_dates: set[date_type] = set()
        for abp in bps:
            b = bouts_map.get(abp.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            val = float(abp.elo_after) if abp.elo_after is not None else None
            per_fighter[abp.fighter_id].append((d, val))
            if d is not None:
                all_dates.add(d)
        for arr in per_fighter.values():
            arr.sort(key=lambda t: (t[0] or date_type.min))

        if not all_dates:
            return []

        years = sorted({d.year for d in all_dates})
        if start_year is not None:
            years = [y for y in years if y >= start_year]
        if end_year is not None:
            years = [y for y in years if y <= end_year]

        def latest_elos_upto(snap_date: date_type) -> dict[str, float]:
            latest: dict[str, float] = {}
            for fid, arr in per_fighter.items():
                last: float | None = None
                for dd, val in arr:
                    if dd is None or dd <= snap_date:
                        if val is not None:
                            last = val
                    else:
                        break
                if last is not None:
                    latest[fid] = last
            return latest

        def top_entries(latest: dict[str, float], year: int) -> list[dict[str, Any]]:
            # Build per-fighter record within the given year
            record_wld: dict[str, tuple[int, int, int]] = defaultdict(lambda: (0, 0, 0))
            last_div: dict[str, tuple[Any | None, int | None]] = {}
            # Iterate all participants once and aggregate by fighter/year
            for abp in bps:
                b = bouts_map.get(abp.bout_id)
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                if d is None or d.year != year:
                    continue
                wins, losses, dcount = record_wld[abp.fighter_id]
                if abp.outcome == FightOutcome.WIN:
                    wins += 1
                elif abp.outcome == FightOutcome.LOSS:
                    losses += 1
                elif abp.outcome == FightOutcome.DRAW:
                    dcount += 1
                # Ignore NO_CONTEST for record
                record_wld[abp.fighter_id] = (wins, losses, dcount)
                # Track most recent division for that fighter within the year
                code = getattr(b, 'weight_class_code', None) if b else None
                prev = last_div.get(abp.fighter_id)
                if prev is None or ((prev[0] or date_type.min) <= (d or date_type.min)):
                    last_div[abp.fighter_id] = (d, int(code) if code is not None else None)

            top_items = sorted(latest.items(), key=lambda kv: kv[1], reverse=True)[: max(1, top)]
            prev_cutoff = date_type(year - 1, 12, 31) if year > date_type.min.year else None
            prev_elos: dict[str, float] = {}
            if prev_cutoff is not None:
                prev_elos = latest_elos_upto(prev_cutoff)
            entries: list[dict[str, Any]] = []
            for rank, (fid, elo_val) in enumerate(top_items, start=1):
                f = fighters_map.get(fid)
                wins, losses, dcount = record_wld.get(fid, (0, 0, 0))
                fights = wins + losses + dcount
                prev_elo = prev_elos.get(fid) if prev_elos else None
                delta_yoy = (elo_val - prev_elo) if (prev_elo is not None) else None
                entries.append(
                    {
                        'fighter_id': fid,
                        'name': f.name if f else None,
                        'elo': elo_val,
                        'rank': rank,
                        'wins': wins,
                        'losses': losses,
                        'draws': dcount,
                        'fights': fights,
                        'division': (last_div.get(fid) or (None, None))[1],
                        'delta_yoy': delta_yoy,
                    }
                )
            return entries

        # For each year, choose the last event date within that year
        snapshots: list[dict[str, Any]] = []
        for y in years:
            year_dates = sorted([d for d in all_dates if d.year == y])
            if not year_dates:
                continue
            snap_date = year_dates[-1]
            # Only include fighters who fought at least once in this calendar year
            eligible_fighters: set[str] = set()
            for fid in per_fighter:
                # Eligible only if fighter had at least one W/L/D in this year (exclude NC-only years)
                had_result = False
                for abp in bps:
                    if abp.fighter_id != fid:
                        continue
                    b = bouts_map.get(abp.bout_id)
                    e = events_map.get(b.event_id) if b and b.event_id else None
                    d = getattr(e, 'event_date', None)
                    if d is None or d.year != y:
                        continue
                    if abp.outcome in (FightOutcome.WIN, FightOutcome.LOSS, FightOutcome.DRAW):
                        had_result = True
                        break
                if had_result:
                    eligible_fighters.add(fid)
            latest_all = latest_elos_upto(snap_date)
            latest = {fid: val for fid, val in latest_all.items() if fid in eligible_fighters}
            entries = top_entries(latest, y)
            snapshots.append({'label': str(y), 'date': snap_date, 'entries': entries})

        await cache.set_json(ck, snapshots)
        return snapshots

    @with_uow
    async def yearly_elo_gains(  # noqa: PLR0912
        self, uow: UnitOfWork, year: int, limit: int = 10, *, offset: int = 0, page_size: int | None = None
    ) -> list[dict[str, Any]]:
        """Compute net ELO gain for each fighter within the calendar year.

        Net gain = (last elo_after in year) - (first elo_before in year), ignoring NC participations.
        Returns sorted list of { fighter_id, name, delta, wins, losses, draws, fights }.
        """
        cache = CacheManager(ttl=900)
        ck = f'analytics:year_gains:{year}:{limit}:{offset}:{page_size}'
        c = await cache.get_json(ck)
        if c is not None:
            return c  # type: ignore[return-value]
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}

        per_fighter: dict[str, list[tuple[Any | None, float | None, float | None, FightOutcome | None]]] = defaultdict(
            list
        )
        record_wld: dict[str, tuple[int, int, int]] = defaultdict(lambda: (0, 0, 0))
        for abp in bps:
            b = bouts_map.get(abp.bout_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if d is None or d.year != year:
                continue
            per_fighter[abp.fighter_id].append(
                (
                    d,
                    float(abp.elo_before) if abp.elo_before is not None else None,
                    float(abp.elo_after) if abp.elo_after is not None else None,
                    abp.outcome,
                )
            )
            wins, losses, draws_count = record_wld[abp.fighter_id]
            if abp.outcome == FightOutcome.WIN:
                wins += 1
            elif abp.outcome == FightOutcome.LOSS:
                losses += 1
            elif abp.outcome == FightOutcome.DRAW:
                draws_count += 1
            record_wld[abp.fighter_id] = (wins, losses, draws_count)

        results: list[dict[str, Any]] = []
        for fid, arr in per_fighter.items():
            # Only consider fighters with at least one W/L/D (exclude NC-only)
            if not any(out in (FightOutcome.WIN, FightOutcome.LOSS, FightOutcome.DRAW) for _d, _eb, _ea, out in arr):
                continue
            arr.sort(key=lambda t: (t[0] or date_type.min))
            first_before = next(
                (eb for (_d, eb, _ea, out) in arr if eb is not None and out != FightOutcome.NO_CONTEST), None
            )
            last_after = None
            for _d, _eb, ea, out in arr:
                if ea is not None and out != FightOutcome.NO_CONTEST:
                    last_after = ea
            if first_before is None or last_after is None:
                continue
            delta = last_after - first_before
            wins, losses, draws_count = record_wld.get(fid, (0, 0, 0))
            fights = wins + losses + draws_count
            f = fighters_map.get(fid)
            results.append(
                {
                    'fighter_id': fid,
                    'name': (f.name if f else None),
                    'delta': delta,
                    'wins': wins,
                    'losses': losses,
                    'draws': draws_count,
                    'fights': fights,
                }
            )

        results.sort(key=lambda x: x['delta'], reverse=True)
        if page_size is not None:
            start = max(0, int(offset))
            end = start + max(1, int(page_size))
            page = results[start:end]
        else:
            page = results[: max(1, limit)]
        await cache.set_json(ck, page)
        return page

    # --- Optimized single-year views using cached snapshots ---
    _RANK_MAX_TOP = 1000

    @with_uow
    async def ranking_years(self, _uow: UnitOfWork) -> list[int]:
        """Return available years for rankings without recomputing per request."""
        snaps = await self.rankings_history(top=self._RANK_MAX_TOP)  # new UoW via decorator
        years: list[int] = []
        for s in snaps:
            try:
                label_val = s.get('label')
                if isinstance(label_val, (str, int)):
                    years.append(int(label_val))
            except Exception as exc:
                logger.warning('Failed to parse ranking year label: %r', exc)
                continue
        years.sort()
        return years

    @with_uow
    async def rankings_year(
        self,
        _uow: UnitOfWork,
        year: int,
        top: int | None = None,
        *,
        offset: int = 0,
        page_size: int | None = None,
        division: int | None = None,
    ) -> dict[str, Any]:
        """Return a single year's snapshot with pagination support."""
        need = (top or 0) if top is not None else (max(1, int(offset)) + max(1, int(page_size or 10)))
        snaps = await self.rankings_history(top=max(need, self._RANK_MAX_TOP))  # new UoW via decorator
        target = None
        for s in snaps:
            try:
                label_val = s.get('label')
                if isinstance(label_val, (str, int)) and int(label_val) == int(year):
                    target = s
                    break
            except Exception as exc:
                logger.warning('Failed to scan snapshot year: %r', exc)
                continue
        if not target:
            return {'label': str(year), 'date': None, 'entries': []}
        entries_all = target.get('entries') or []
        # Optional division filter if snapshots include 'division' field
        if division is not None:
            try:
                div_code = int(division)
                entries_all = [e for e in entries_all if int(e.get('division') or -1) == div_code]
            except Exception:
                entries_all = []
        if top is not None:
            entries = entries_all[: max(1, top)]
        else:
            b = max(0, int(offset))
            e = b + max(1, int(page_size or 10))
            entries = entries_all[b:e]
        return {'label': target.get('label'), 'date': target.get('date'), 'entries': entries}

    async def _compute_ranks(self, uow: UnitOfWork, points: list[EloPoint]) -> None:
        try:
            all_bps = await uow.bout_participants.get_all()
            bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
            events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}

            per_fighter: dict[str, list[tuple[date_type | None, float | None]]] = defaultdict(list)
            for abp in all_bps:
                b = bouts_map.get(abp.bout_id)
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                per_fighter[abp.fighter_id].append((d, float(abp.elo_after) if abp.elo_after is not None else None))
            for _fid, arr in per_fighter.items():
                arr.sort(key=lambda t: (t[0] or date_type.min))

            def latest_elos_upto(d: date_type | None) -> dict[str, float]:
                out: dict[str, float] = {}
                if d is None:
                    return out
                for fid, arr in per_fighter.items():
                    last: float | None = None
                    for dd, val in arr:
                        if dd is None or dd <= d:
                            if val is not None:
                                last = val
                        else:
                            break
                    if last is not None:
                        out[fid] = last
                return out

            for p in points:
                if p.event_date and p.elo_after is not None:
                    latest = latest_elos_upto(p.event_date)
                    greater = sum(1 for v in latest.values() if v > p.elo_after)
                    p.rank_after = greater + 1
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception('Failed to compute historical ranks: %s', exc)

    @with_uow
    async def fighter_career_stats(self, uow: UnitOfWork, fighter_id: str) -> dict[str, Any]:
        """Return summed career stats and simple breakdowns for a fighter.

        Includes:
        - totals_for / totals_against (career sums)
        - averages_for / averages_against (per-fight averages)
        - by_year: { YYYY: { fights, totals_for, totals_against } }
        - by_opponent: [ { opponent_id, opponent_name, fights, totals_for, totals_against } ]
        """
        parts = await uow.bout_participants.get_by_fighter_id(fighter_id)
        # Group by bout for opponent lookup
        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for p in await uow.bout_participants.get_all():
            by_bout[p.bout_id].append(p)

        keys = [
            'kd',
            'sig_strikes',
            'sig_strikes_thrown',
            'total_strikes',
            'total_strikes_thrown',
            'td',
            'td_attempts',
            'sub_attempts',
            'reversals',
            'control_time_sec',
            'head_ss',
            'body_ss',
            'leg_ss',
            'distance_ss',
            'clinch_ss',
            'ground_ss',
        ]

        def zero_dict() -> dict[str, int]:
            return dict.fromkeys(keys, 0)

        totals_for = zero_dict()
        totals_against = zero_dict()
        fights_count = 0
        # Year and opponent breakdowns
        by_year: dict[int, dict[str, Any]] = {}
        by_opp: dict[str, dict[str, Any]] = {}

        def add(dst: dict[str, int], bp: BoutParticipant) -> None:
            for k in keys:
                # Some scraped/legacy fields may be missing or non-numeric; suppress
                # conversion errors rather than failing the whole aggregation.
                with contextlib.suppress(Exception):
                    dst[k] += int(getattr(bp, k) or 0)

        for p in parts:
            add(totals_for, p)
            fights_count += 1
            # Opponent
            opponent: BoutParticipant | None = None
            for op in by_bout.get(p.bout_id, []):
                if op.fighter_id != fighter_id:
                    opponent = op
                    break
            if opponent:
                add(totals_against, opponent)
            # Year grouping
            # Fetch event date
            bout = await uow.bouts.get_by_bout_id(p.bout_id)
            ev = await uow.events.get_by_event_id(bout.event_id) if bout and bout.event_id else None
            _ed = getattr(ev, 'event_date', None)
            yr = _ed.year if _ed is not None else None
            if yr is not None:
                bucket = by_year.setdefault(yr, {'fights': 0, 'totals_for': zero_dict(), 'totals_against': zero_dict()})
                bucket['fights'] += 1
                # Add stats
                add(bucket['totals_for'], p)
                if opponent:
                    add(bucket['totals_against'], opponent)
            # Opponent grouping
            if opponent:
                opp_id = opponent.fighter_id
                # Try to fetch name
                opp_ent = await uow.fighters.get_by_fighter_id(opp_id)
                oitem = by_opp.setdefault(
                    opp_id,
                    {
                        'opponent_id': opp_id,
                        'opponent_name': getattr(opp_ent, 'name', None),
                        'fights': 0,
                        'totals_for': zero_dict(),
                        'totals_against': zero_dict(),
                    },
                )
                oitem['fights'] += 1
                add(oitem['totals_for'], p)
                add(oitem['totals_against'], opponent)

        def averages(total: dict[str, int], denom: int) -> dict[str, float]:
            d = max(1, denom)
            return {k: float(v) / d for k, v in total.items()}

        return {
            'fighter_id': fighter_id,
            'fights': fights_count,
            'totals_for': totals_for,
            'totals_against': totals_against,
            'averages_for': averages(totals_for, fights_count),
            'averages_against': averages(totals_against, fights_count),
            'by_year': by_year,
            'by_opponent': list(by_opp.values()),
        }

    # --- Head-to-head probability ---
    @with_uow
    async def h2h_probability(  # noqa: PLR0912, PLR0913, PLR0915
        self,
        uow: UnitOfWork,
        fighter1_id: str,
        fighter2_id: str,
        *,
        mode1: str = 'current',
        mode2: str = 'current',
        year1: int | None = None,
        year2: int | None = None,
        adjust: str = 'base',
        explain: bool = False,
        ewma_hl: int | None = None,
        five_round: bool | None = None,
        title: bool | None = None,
    ) -> dict[str, Any]:
        """Compute expected win probabilities and implied odds for two fighters.

        mode: 'current' | 'peak' | 'year'. When 'year', uses last elo_after up to Dec 31 of that year.
        """

        async def elo_for(fid: str, mode: str, year: int | None) -> float | None:
            f = await uow.fighters.get_by_fighter_id(fid)
            if not f:
                return None
            m = (mode or 'current').lower()
            if m == 'peak':
                return (
                    float(f.peak_elo)
                    if f.peak_elo is not None
                    else (float(f.current_elo) if f.current_elo is not None else float(f.entry_elo or 1500.0))
                )
            if m == 'current':
                return (
                    float(f.current_elo)
                    if f.current_elo is not None
                    else (float(f.entry_elo) if f.entry_elo is not None else 1500.0)
                )
            # year mode
            if year is None:
                return float(f.current_elo) if f.current_elo is not None else float(f.entry_elo or 1500.0)
            # Build chronological elo_after points and take last within year
            parts = await uow.bout_participants.get_by_fighter_id(fid)
            bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
            events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
            vals: list[tuple[date_type | None, float | None]] = []
            for p in parts:
                b = bouts_map.get(p.bout_id)
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                vals.append((d, float(p.elo_after) if p.elo_after is not None else None))
            vals.sort(key=lambda t: (t[0] or date_type.min))
            last: float | None = None
            for d, v in vals:
                if d is None:
                    if v is not None:
                        last = v
                    continue
                if d.year <= year and v is not None:
                    last = v
                if d.year > year:
                    break
            if last is not None:
                return last
            return float(f.entry_elo or 1500.0)

        async def method_breakdown(fid: str, year: int | None = None) -> dict[str, float]:
            """Return normalized win-method distribution for the fighter up to optional year."""
            parts = await uow.bout_participants.get_by_fighter_id(fid)
            bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
            events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
            counts = {'KO/TKO': 0, 'TKO-DS': 0, 'SUB': 0, 'DEC': 0}
            total = 0
            for p in parts:
                if p.outcome != FightOutcome.WIN:
                    continue
                b = bouts_map.get(p.bout_id)
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                if year is not None and d is not None and d.year > year:
                    continue
                mclass = method_class(getattr(b, 'method', '') if b else '')
                if mclass == 'KO_TKO':
                    counts['KO/TKO'] += 1
                elif mclass == 'TKO_DS':
                    counts['TKO-DS'] += 1
                elif mclass == 'SUB':
                    counts['SUB'] += 1
                else:
                    counts['DEC'] += 1
                total += 1
            if total == 0:
                return {'KO/TKO': 0.25, 'TKO-DS': 0.25, 'SUB': 0.25, 'DEC': 0.25}
            return {k: round(v / total, 4) for k, v in counts.items()}

        async def loss_breakdown(fid: str, year: int | None = None) -> dict[str, float]:
            """Return normalized loss-method distribution for the fighter up to optional year."""
            parts = await uow.bout_participants.get_by_fighter_id(fid)
            bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
            events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
            counts = {'KO/TKO': 0, 'TKO-DS': 0, 'SUB': 0, 'DEC': 0}
            total = 0
            for p in parts:
                if p.outcome != FightOutcome.LOSS:
                    continue
                b = bouts_map.get(p.bout_id)
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                if year is not None and d is not None and d.year > year:
                    continue
                mclass = method_class(getattr(b, 'method', '') if b else '')
                if mclass == 'KO_TKO':
                    counts['KO/TKO'] += 1
                elif mclass == 'TKO_DS':
                    counts['TKO-DS'] += 1
                elif mclass == 'SUB':
                    counts['SUB'] += 1
                else:
                    counts['DEC'] += 1
                total += 1
            if total == 0:
                return {'KO/TKO': 0.25, 'TKO-DS': 0.25, 'SUB': 0.25, 'DEC': 0.25}
            return {k: round(v / total, 4) for k, v in counts.items()}

        async def feature_stats(fid: str, year: int | None = None) -> dict[str, float]:
            """Aggregate simple per-fight rates for KD, TD, SUB_ATT, CTRL and 'allowed' versions.

            Uses bouts up to optional year. 'Allowed' is computed from the opponent stats in the same bouts.
            """
            parts = await uow.bout_participants.get_by_fighter_id(fid)
            bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
            events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
            # Build by bout map for quick opponent lookup
            by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
            for p in await uow.bout_participants.get_all():
                by_bout[p.bout_id].append(p)
            count = 0
            kd = td = subs = ctrl = 0.0
            kd_allowed = td_allowed = subs_allowed = ctrl_allowed = 0.0
            for p in parts:
                b = bouts_map.get(p.bout_id)
                e = events_map.get(b.event_id) if b and b.event_id else None
                d = getattr(e, 'event_date', None)
                if year is not None and d is not None and d.year > year:
                    continue
                count += 1
                # Suppress any per-record parsing errors (missing fields, bad types)
                with contextlib.suppress(Exception):
                    kd += float(p.kd or 0)
                    td += float(p.td or 0)
                    subs += float(p.sub_attempts or 0)
                    ctrl += float(p.control_time_sec or 0)
                    # Opponent in same bout
                    for op in by_bout.get(p.bout_id, []):
                        if op.fighter_id != fid:
                            kd_allowed += float(op.kd or 0)
                            td_allowed += float(op.td or 0)
                            subs_allowed += float(op.sub_attempts or 0)
                            ctrl_allowed += float(op.control_time_sec or 0)
                            break
            denom = max(1, count)
            return {
                'kd_rate': kd / denom,
                'td_rate': td / denom,
                'sub_rate': subs / denom,
                'ctrl_rate': ctrl / denom,
                'kd_allowed': kd_allowed / denom,
                'td_allowed': td_allowed / denom,
                'sub_allowed': subs_allowed / denom,
                'ctrl_allowed': ctrl_allowed / denom,
            }

        r1 = await elo_for(fighter1_id, mode1, year1)
        r2 = await elo_for(fighter2_id, mode2, year2)
        if r1 is None or r2 is None:
            raise ValueError('Invalid fighter id(s)')
        p1 = logistic_expect(float(r1), float(r2), SCALE_S)
        p2 = 1.0 - p1

        # Get names and method distributions
        f1 = await uow.fighters.get_by_fighter_id(fighter1_id)
        f2 = await uow.fighters.get_by_fighter_id(fighter2_id)
        y1 = year1 if (mode1 or '').lower() == 'year' else None
        y2 = year2 if (mode2 or '').lower() == 'year' else None
        mb1 = await method_breakdown(fighter1_id, y1)
        mb2 = await method_breakdown(fighter2_id, y2)
        lb2 = await loss_breakdown(fighter2_id, y2)  # opponent vulnerabilities
        lb1 = await loss_breakdown(fighter1_id, y1)
        fs1 = await feature_stats(fighter1_id, y1)
        fs2 = await feature_stats(fighter2_id, y2)

        # Blend winner's method tendency with opponent's loss tendency
        alpha = 0.6  # weight on winner's own split

        # Stats-based heuristic scores (fighter A tendency + fighter B vulnerability)
        def stat_scores(fs_self: dict[str, float], fs_opp: dict[str, float]) -> dict[str, float]:
            sko = 0.6 * fs_self.get('kd_rate', 0.0) + 0.4 * fs_opp.get('kd_allowed', 0.0)
            ssub = 0.6 * fs_self.get('sub_rate', 0.0) + 0.4 * fs_opp.get('sub_allowed', 0.0)
            # Decision: control dominance proxy (more control but not necessarily finishes)
            sdec = max(0.0, 0.5 * fs_self.get('ctrl_rate', 0.0) + 0.5 * fs_opp.get('ctrl_allowed', 0.0))
            total = max(1e-6, (sko + ssub + sdec))
            # Stats do not differentiate regular KO/TKO vs doctor stoppage; keep TKO-DS at 0 here.
            return {'KO/TKO': sko / total, 'TKO-DS': 0.0, 'SUB': ssub / total, 'DEC': sdec / total}

        ss1 = stat_scores(fs1, fs2)
        ss2 = stat_scores(fs2, fs1)

        # Blend history, opponent losses, and current stats
        beta = 0.5  # weight on stats-based split versus historical blend
        blend_hist_1 = {
            k: alpha * mb1.get(k, 0) + (1 - alpha) * lb2.get(k, 0) for k in ('KO/TKO', 'TKO-DS', 'SUB', 'DEC')
        }
        blend_hist_2 = {
            k: alpha * mb2.get(k, 0) + (1 - alpha) * lb1.get(k, 0) for k in ('KO/TKO', 'TKO-DS', 'SUB', 'DEC')
        }

        def _norm(d: dict[str, float]) -> dict[str, float]:
            t = sum(d.values()) or 1.0
            return {k: v / t for k, v in d.items()}

        comb1_raw = {
            k: beta * ss1.get(k, 0) + (1 - beta) * blend_hist_1.get(k, 0) for k in ('KO/TKO', 'TKO-DS', 'SUB', 'DEC')
        }
        comb2_raw = {
            k: beta * ss2.get(k, 0) + (1 - beta) * blend_hist_2.get(k, 0) for k in ('KO/TKO', 'TKO-DS', 'SUB', 'DEC')
        }
        comb1 = _norm(comb1_raw)
        comb2 = _norm(comb2_raw)
        # Winner-centric method odds
        meth1 = {k: round(p1 * v, 4) for k, v in comb1.items()}
        meth2 = {k: round(p2 * v, 4) for k, v in comb2.items()}
        winner_pred = 'draw'
        if p1 > p2:
            winner_pred = f'{getattr(f1, "name", fighter1_id)} by {max(meth1.items(), key=lambda kv: kv[1])[0]}'
        elif p2 > p1:
            winner_pred = f'{getattr(f2, "name", fighter2_id)} by {max(meth2.items(), key=lambda kv: kv[1])[0]}'

        def to_odds(p: float) -> dict[str, float]:
            # Decimal and American odds (positive for underdog, negative for favorite)
            dec = float('inf') if p <= 0 else round(1.0 / p, 4)
            if p <= 0:
                american = 0.0
            elif p >= ODDS_FAVORITE_THRESHOLD:
                american = round(-100.0 * p / (1.0 - p))
            else:
                american = round(100.0 * (1.0 - p) / p)
            return {'decimal': float(dec), 'american': float(american)}

        result: dict[str, Any] = {
            'fighter1_id': fighter1_id,
            'fighter2_id': fighter2_id,
            'fighter1_name': getattr(f1, 'name', None),
            'fighter2_name': getattr(f2, 'name', None),
            'R1': float(r1),
            'R2': float(r2),
            'P1': float(p1),
            'P2': float(p2),
            'odds1': to_odds(p1),
            'odds2': to_odds(p2),
            'method_probs1': meth1,
            'method_probs2': meth2,
            'winner_pred': winner_pred,
        }

        def ensure_explain_payload() -> dict[str, Any]:
            existing = result.get('explain')
            if isinstance(existing, dict):
                return existing
            fresh: dict[str, Any] = {}
            result['explain'] = fresh
            return fresh

        # Apply optional adjustment modes
        if adjust in ('nudge', 'meta', 'window', 'best'):
            s = float(SCALE_S)
            # Prefer class-loaded artifacts, fallback to local file/no-op defaults
            artifacts = dict(self._ARTIFACTS or {})
            defaults = {
                'mu': {
                    'fi_diff': 0.0,
                    'sos_diff': 0.0,
                    'cons_diff': 0.0,
                    'rest_diff': 0.0,
                    'five_round': 0.0,
                    'title': 0.0,
                },
                'sigma': {
                    'fi_diff': 1.0,
                    'sos_diff': 1.0,
                    'cons_diff': 1.0,
                    'rest_diff': 1.0,
                    'five_round': 1.0,
                    'title': 1.0,
                },
                'platt': {'a': 1.0, 'b': 0.0},
            }

            try:
                if not artifacts:
                    here = os.path.dirname(__file__)
                    path = os.path.abspath(os.path.join(here, '..', '..', 'configs', 'elo_adjust.json'))
                    if os.path.exists(path):
                        with open(path, encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, dict):
                                artifacts.update(data)
            except Exception as exc:
                logger.debug('Failed to load calibration artifacts: %r', exc)

            # Overlay defaults for any missing keys
            for k, v in defaults.items():
                artifacts.setdefault(k, v)

            def _std(name: str, val: float) -> float:
                s = float(artifacts['sigma'].get(name, 1.0) or 1.0)
                if s == 0:
                    return 0.0
                return (float(val) - float(artifacts['mu'].get(name, 0.0))) / s

            # context features
            try:
                fi1 = await self.form_index(fighter_id=fighter1_id)
                fi2 = await self.form_index(fighter_id=fighter2_id)
                sos1 = await self.sos(fighter_id=fighter1_id)
                sos2 = await self.sos(fighter_id=fighter2_id)
                cv1 = await self.consistency_versatility(fighter_id=fighter1_id)
                cv2 = await self.consistency_versatility(fighter_id=fighter2_id)
            except Exception as exc:
                logger.exception('Exception in context features: %s', exc)
                fi1 = fi2 = sos1 = sos2 = cv1 = cv2 = {'fi': 0, 'mean': 0, 'sd_elo_delta': 0}
            feats = {
                'fi_diff': float((fi1.get('fi') or 0) - (fi2.get('fi') or 0)),
                'sos_diff': float((sos1.get('mean') or 0) - (sos2.get('mean') or 0)),
                'cons_diff': float((cv2.get('sd_elo_delta') or 0) - (cv1.get('sd_elo_delta') or 0)),
                'rest_diff': 0.0,
                'five_round': 1.0 if five_round else 0.0,
                'title': 1.0 if title else 0.0,
            }
            if adjust == 'window':
                try:
                    # Use EWMA of recent elo_after values
                    e1_recent = await self._ewma_recent_elo(uow, fighter1_id, half_life_days=int(ewma_hl or 180))
                    e2_recent = await self._ewma_recent_elo(uow, fighter2_id, half_life_days=int(ewma_hl or 180))
                    if e1_recent is None:
                        e1_recent = float(r1)
                    if e2_recent is None:
                        e2_recent = float(r2)
                except Exception:
                    e1_recent, e2_recent = float(r1), float(r2)
                alpha_w = float(artifacts.get('alpha_recent', 0.2))
                e1_eff = (1 - alpha_w) * float(r1) + alpha_w * e1_recent
                e2_eff = (1 - alpha_w) * float(r2) + alpha_w * e2_recent
                z_fin = (e1_eff - e2_eff) / float(artifacts.get('S', s))
                p_fin = 1.0 / (1.0 + math.exp(-z_fin))
                result['P1'] = p_fin
                result['P2'] = 1.0 - p_fin
                result['odds1'] = to_odds(p_fin)
                result['odds2'] = to_odds(1.0 - p_fin)
                explain_payload = ensure_explain_payload()
                explain_payload.update(
                    {
                        'adjust': 'window',
                        'alpha': alpha_w,
                        'effective_elo_gap': (e1_eff - e2_eff),
                        'flags': {'five_round': bool(five_round), 'title': bool(title)},
                        'calibration_version': (self._ARTIFACTS or {}).get('calibration_version'),
                    }
                )
                result['uncertainty'] = {'prob_ci_approx': [max(0.0, p_fin - 0.07), min(1.0, p_fin + 0.07)]}
                return result
            # For best/meta/nudge we need base features once
            # nudge/meta elo offset
            order = list(artifacts.get('feature_order', []))
            vals_std = [_std(k, float(feats.get(k, 0.0))) for k in order]
            beta = artifacts.get('beta', {})
            delta_z = sum(float(beta.get(k, 0.0)) * v for k, v in zip(order, vals_std, strict=True))
            cap_logit = float(artifacts.get('cap_logit', 0.25))
            delta_z = max(-cap_logit, min(cap_logit, delta_z))
            delta_e = float(artifacts.get('S', s)) * delta_z
            cap_elo = float(artifacts.get('cap_elo', 30.0))
            delta_e = max(-cap_elo, min(cap_elo, delta_e))
            e1_eff = float(r1) + delta_e / 2.0
            e2_eff = float(r2) - delta_e / 2.0
            z_fin = (e1_eff - e2_eff) / float(artifacts.get('S', s))
            p_fin = 1.0 / (1.0 + math.exp(-z_fin))
            if adjust == 'meta':
                pl = artifacts.get('platt', {})
                a = float(pl.get('a', 1.0))
                b = float(pl.get('b', 0.0))
                try:
                    logit = math.log(p_fin / max(1e-9, 1 - p_fin))
                    z2 = a * logit + b
                    p_fin = 1.0 / (1.0 + math.exp(-z2))
                except Exception as exc:
                    logger.exception('Exception in meta variant platt calibration: %s', exc)
            if adjust == 'best':
                # Compute window prob too
                try:
                    e1_recent = await self._ewma_recent_elo(uow, fighter1_id, half_life_days=int(ewma_hl or 180))
                    e2_recent = await self._ewma_recent_elo(uow, fighter2_id, half_life_days=int(ewma_hl or 180))
                    if e1_recent is None:
                        e1_recent = float(r1)
                    if e2_recent is None:
                        e2_recent = float(r2)
                except Exception:
                    e1_recent, e2_recent = float(r1), float(r2)
                alpha_w = float(artifacts.get('alpha_recent', 0.2))
                e1w = (1 - alpha_w) * float(r1) + alpha_w * e1_recent
                e2w = (1 - alpha_w) * float(r2) + alpha_w * e2_recent
                z_w = (e1w - e2w) / float(artifacts.get('S', s))
                p_w = 1.0 / (1.0 + math.exp(-z_w))
                # nudge (already p_fin, E1_eff/E2_eff)
                p_n = p_fin
                # meta variant (apply platt on same delta, alternative path)
                pl = artifacts.get('platt', {})
                a = float(pl.get('a', 1.0))
                b = float(pl.get('b', 0.0))
                try:
                    logit = math.log(p_n / max(1e-9, 1 - p_n))
                    z2 = a * logit + b
                    p_m = 1.0 / (1.0 + math.exp(-z2))
                except Exception:
                    p_m = p_n
                w_w, w_n, w_m = 0.2, 0.6, 0.2
                p_best = max(0.0, min(1.0, w_w * p_w + w_n * p_n + w_m * p_m))
                result['P1'] = p_best
                result['P2'] = 1.0 - p_best
                result['odds1'] = to_odds(p_best)
                result['odds2'] = to_odds(1.0 - p_best)
                contrib = {
                    k: float(artifacts.get('S', s)) * float(beta.get(k, 0.0)) * v
                    for k, v in zip(order, vals_std, strict=True)
                }
                explain_payload = ensure_explain_payload()
                explain_payload.update(
                    {
                        'adjust': 'best',
                        'p_base': float(p1),
                        'elo_gap': float(r1) - float(r2),
                        'effective_elo_gap': (e1_eff - e2_eff),
                        'elo_offset': delta_e,
                        'contrib': contrib,
                        'alpha': alpha_w,
                        'flags': {'five_round': bool(five_round), 'title': bool(title)},
                        'calibration_version': (self._ARTIFACTS or {}).get('calibration_version'),
                    }
                )
                result['uncertainty'] = {'prob_ci_approx': [max(0.0, p_best - 0.07), min(1.0, p_best + 0.07)]}
                return result
            result['P1'] = p_fin
            result['P2'] = 1.0 - p_fin
            result['odds1'] = to_odds(p_fin)
            result['odds2'] = to_odds(1.0 - p_fin)
            contrib = {
                k: float(artifacts.get('S', s)) * float(beta.get(k, 0.0)) * v
                for k, v in zip(order, vals_std, strict=True)
            }
            explain_payload = ensure_explain_payload()
            explain_payload.update(
                {
                    'adjust': adjust,
                    'p_base': float(p1),
                    'elo_gap': float(r1) - float(r2),
                    'effective_elo_gap': (e1_eff - e2_eff),
                    'elo_offset': delta_e,
                    'contrib': contrib,
                    'flags': {'five_round': bool(five_round), 'title': bool(title)},
                    'calibration_version': (self._ARTIFACTS or {}).get('calibration_version'),
                }
            )
            result['uncertainty'] = {'prob_ci_approx': [max(0.0, p_fin - 0.07), min(1.0, p_fin + 0.07)]}
            return result
        if explain:
            explain_ctx = ensure_explain_payload()
            # Pull quick context analytics (default params)
            fi_ctx1: dict[str, Any] | None = None
            fi_ctx2: dict[str, Any] | None = None
            sos_ctx1: dict[str, Any] | None = None
            sos_ctx2: dict[str, Any] | None = None
            cv_ctx1: dict[str, Any] | None = None
            cv_ctx2: dict[str, Any] | None = None
            try:
                fi_ctx1 = await self.form_index(fighter_id=fighter1_id)
                fi_ctx2 = await self.form_index(fighter_id=fighter2_id)
                sos_ctx1 = await self.sos(fighter_id=fighter1_id)
                sos_ctx2 = await self.sos(fighter_id=fighter2_id)
                cv_ctx1 = await self.consistency_versatility(fighter_id=fighter1_id)
                cv_ctx2 = await self.consistency_versatility(fighter_id=fighter2_id)
            except Exception:  # pragma: no cover -- non-critical extras
                logger.debug('Context analytics unavailable for H2H explain payload')
            explain_ctx.update(
                {
                    'adjust': adjust,
                    'elo_gap': float(r1) - float(r2),
                    'alpha': alpha,
                    'beta': beta,
                    'win_method_tendencies_1': mb1,
                    'win_method_tendencies_2': mb2,
                    'opp_loss_tendencies_1': lb1,
                    'opp_loss_tendencies_2': lb2,
                    'stat_scores_1': ss1,
                    'stat_scores_2': ss2,
                    'form_index_1': None if not fi_ctx1 else fi_ctx1.get('fi'),
                    'form_index_2': None if not fi_ctx2 else fi_ctx2.get('fi'),
                    'form_index_delta': (
                        None if not fi_ctx1 or not fi_ctx2 else (fi_ctx1.get('fi') or 0) - (fi_ctx2.get('fi') or 0)
                    ),
                    'sos_mean_1': None if not sos_ctx1 else sos_ctx1.get('mean'),
                    'sos_mean_2': None if not sos_ctx2 else sos_ctx2.get('mean'),
                    'sos_mean_delta': (
                        None
                        if not sos_ctx1 or not sos_ctx2
                        else (sos_ctx1.get('mean') or 0) - (sos_ctx2.get('mean') or 0)
                    ),
                    'consistency_1': None if not cv_ctx1 else cv_ctx1.get('sd_elo_delta'),
                    'consistency_2': None if not cv_ctx2 else cv_ctx2.get('sd_elo_delta'),
                    'consistency_delta': (
                        None
                        if not cv_ctx1 or not cv_ctx2
                        else (cv_ctx1.get('sd_elo_delta') or 0) - (cv_ctx2.get('sd_elo_delta') or 0)
                    ),
                    'versatility_1': None if not cv_ctx1 else cv_ctx1.get('versatility'),
                    'versatility_2': None if not cv_ctx2 else cv_ctx2.get('versatility'),
                    'flags': {'five_round': False, 'title': False},
                }
            )
        return result

    @with_uow
    async def h2h_calibration(  # noqa: PLR0912, PLR0915
        self,
        uow: UnitOfWork,
        since_year: int | None = None,
        *,
        bins: int | None = None,
        min_n: int | None = None,
        apply_cal: bool | None = None,
    ) -> dict[str, Any]:
        """Compute calibration with quantile bins and Wilson CIs using pre-fight ELOs.

        Uses fighter1 perspective per bout: p = P(f1 wins), y = 1 if f1 won else 0. Draws/NC skipped.
        """
        bps: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        pairs: list[tuple[float, int]] = []  # (p_f1, y)
        for b_id in {p.bout_id for p in bps}:
            parts = [p for p in bps if p.bout_id == b_id]
            if len(parts) < MIN_PARTICIPANTS:
                continue
            p1, p2 = parts[0], parts[1]
            b = bouts_map.get(b_id)
            e = events_map.get(b.event_id) if b and b.event_id else None
            d = getattr(e, 'event_date', None)
            if since_year is not None and (d is None or d.year < int(since_year)):
                continue
            if p1.elo_before is None or p2.elo_before is None:
                continue
            p_f1 = logistic_expect(float(p1.elo_before), float(p2.elo_before), SCALE_S)
            if p1.outcome == FightOutcome.WIN:
                pairs.append((p_f1, 1))
            elif p2.outcome == FightOutcome.WIN:
                pairs.append((p_f1, 0))
            else:
                continue
        if not pairs:
            return {'brier': None, 'bins': [], 'plot': []}

        # Optional Platt calibration
        def _sigmoid(z: float) -> float:
            return 1.0 / (1.0 + exp(-z))

        def _logit(p: float) -> float:
            p = min(max(p, 1e-6), 1 - 1e-6)
            return log(p / (1 - p))

        if apply_cal:
            a = float(((self._ARTIFACTS or {}).get('platt', {}) or {}).get('a', 1.0))
            b_platt = float(((self._ARTIFACTS or {}).get('platt', {}) or {}).get('b', 0.0))
            pairs = [(_sigmoid(a * _logit(p) + b_platt), y) for p, y in pairs]

        # Metrics
        brier = sum((p - y) ** 2 for p, y in pairs) / len(pairs)
        logloss = sum(-(y * math.log(max(1e-12, p)) + (1 - y) * math.log(max(1e-12, 1 - p))) for p, y in pairs) / len(
            pairs
        )

        def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
            if n <= 0:
                return (0.0, 0.0)
            p = k / n
            denom = 1.0 + (z * z) / n
            center = p + (z * z) / (2 * n)
            adj = z * math.sqrt((p * (1 - p) + (z * z) / (4 * n)) / n)
            lo = max(0.0, (center - adj) / denom)
            hi = min(1.0, (center + adj) / denom)
            return (lo, hi)

        # Quantile edges
        ps_sorted = sorted(p for p, _ in pairs)
        nbins = int(bins or 12)
        edges: list[float] = [0.0]
        for i in range(1, nbins):
            idx = int(i * (len(ps_sorted) - 1) / nbins)
            edges.append(ps_sorted[idx])
        edges.append(1.0)
        for i in range(1, len(edges)):
            edges[i] = max(edges[i], edges[i - 1])

        all_bins: list[dict[str, Any]] = []
        for i in range(len(edges) - 1):
            lo, hi = edges[i], edges[i + 1]
            inbin = [(p, y) for (p, y) in pairs if (p >= lo and (p < hi or (i == len(edges) - 2 and p <= hi)))]
            n = len(inbin)
            if n == 0:
                continue
            k = sum(y for _, y in inbin)
            avg_p = sum(p for p, _ in inbin) / n
            obs = k / n
            lo_ci, hi_ci = wilson_ci(k, n)
            all_bins.append({'lo': lo, 'hi': hi, 'n': n, 'avg_p': avg_p, 'obs': obs, 'ci_lo': lo_ci, 'ci_hi': hi_ci})

        min_n_val = int(min_n or 50)
        plot_bins = [b for b in all_bins if b['n'] >= min_n_val]
        return {
            'brier': brier,
            'logloss': logloss,
            'bins': all_bins,
            'plot': plot_bins,
            'applied_calibration': bool(apply_cal),
        }

    # --- Division roster (top by current elo) ---

    @with_uow
    async def division_roster(  # noqa: PLR0912, PLR0915
        self, uow: UnitOfWork, code: int, top: int = 10
    ) -> dict[str, Any]:
        target_codes = set(self._normalize_division_codes(int(code)))
        parts = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        fighters_map: dict[str, Fighter] = {f.fighter_id: f for f in await uow.fighters.get_all()}

        stats: dict[str, dict[str, Any]] = {}
        for abp in parts:
            b = bouts_map.get(abp.bout_id)
            if not b:
                continue
            wc = getattr(b, 'weight_class_code', None)
            if wc is None:
                continue
            try:
                wc_int = int(wc)
            except (TypeError, ValueError):
                continue
            if wc_int not in target_codes:
                continue
            ev = events_map.get(b.event_id) if b and b.event_id else None
            ev_date = getattr(ev, 'event_date', None)
            delta_recent = None
            if abp.elo_before is not None and abp.elo_after is not None:
                delta_recent = float(abp.elo_after) - float(abp.elo_before)
            entry = stats.setdefault(
                abp.fighter_id,
                {
                    'fights': 0,
                    'last_event_date': None,
                    'delta_recent': None,
                    'last_event_id': None,
                    'last_event_name': None,
                },
            )
            entry['fights'] += 1
            if ev_date is not None:
                prev_date = entry['last_event_date']
                if prev_date is None or ev_date > prev_date:
                    entry['last_event_date'] = ev_date
                    entry['delta_recent'] = delta_recent
                    entry['last_event_id'] = getattr(ev, 'event_id', None)
                    entry['last_event_name'] = getattr(ev, 'name', None)

        recent_cutoff = date_type.today() - timedelta(days=ACTIVE_LAST_DAYS)

        rows: list[dict[str, Any]] = []
        for fid, meta in stats.items():
            fighter = fighters_map.get(fid)
            if not fighter:
                continue
            last_event_date = meta.get('last_event_date')
            last_date_norm = last_event_date
            if isinstance(last_date_norm, str):
                try:
                    last_date_norm = date_type.fromisoformat(last_date_norm)
                except Exception:  # pragma: no cover - defensive parsing
                    last_date_norm = None
            elif isinstance(last_date_norm, datetime):
                last_date_norm = last_date_norm.date()
            elif isinstance(last_date_norm, date):
                # already a date instance; leave as-is
                pass
            else:
                last_date_norm = None
            if last_date_norm is None or last_date_norm < recent_cutoff:
                continue
            current_elo_raw = fighter.current_elo if fighter.current_elo is not None else fighter.entry_elo
            if current_elo_raw is None:
                continue
            current_elo = float(current_elo_raw)
            entry_elo_val = float(fighter.entry_elo) if fighter.entry_elo is not None else current_elo
            progress = current_elo - entry_elo_val
            rows.append(
                {
                    'fighter_id': fid,
                    'fighter_name': fighter.name,
                    'elo': current_elo,
                    'entry_elo': entry_elo_val,
                    'elo_progress': progress,
                    'peak_elo': float(fighter.peak_elo) if fighter.peak_elo is not None else None,
                    'delta_recent': meta.get('delta_recent'),
                    'last_event_date': meta.get('last_event_date'),
                    'last_event_id': meta.get('last_event_id'),
                    'last_event_name': meta.get('last_event_name'),
                    'fights': meta.get('fights', 0),
                }
            )

        weight_current = 0.7
        weight_progress = 0.3
        rows.sort(
            key=lambda r: (weight_current * r['elo'] + weight_progress * (r['elo'] - r.get('entry_elo', r['elo']))),
            reverse=True,
        )

        # Identify display name and primary code (first of set)
        primary_code = next(iter(target_codes)) if target_codes else int(code)
        division_name = DIVISION_LABELS.get(primary_code)
        if not division_name:
            try:
                wc_enum = WeightClassCode(primary_code)
            except Exception:
                wc_enum = None
            lbs = WEIGHT_CLASS_MAX_LBS.get(wc_enum) if wc_enum else None
            division_name = f'Division {primary_code}{f" ({lbs})" if lbs else ""}'

        return {
            'division_code': int(code),
            'division_name': division_name,
            'rows': rows[: max(1, int(top))],
            'active_count': len(rows),
        }

    # --- Form top (small utility to support spotlight) ---
    @with_uow
    async def form_top(
        self,
        uow: UnitOfWork,
        *,
        window: str = 'fights',
        n: int = 6,
        half_life_days: int = 180,
        top: int = 3,
        min_recent_fights: int = 0,
        recent_days: int | None = None,
    ) -> list[dict[str, Any]]:
        fighters = await uow.fighters.get_all()
        min_recent = max(0, int(min_recent_fights))
        if recent_days is not None:
            recent_window = max(0, int(recent_days))
        elif min_recent > 0:
            recent_window = 730
        else:
            recent_window = 0
        cutoff = date_type.today() - timedelta(days=recent_window) if recent_window > 0 else None
        parts_all: list[BoutParticipant] = await uow.bout_participants.get_all()
        bouts_map: dict[str, Bout] = {b.bout_id: b for b in await uow.bouts.get_all()}
        events_map: dict[Any, Event] = {e.event_id: e for e in await uow.events.get_all()}
        recent_counts, recent_last, last_any = self._collect_recent_activity(parts_all, bouts_map, events_map, cutoff)

        by_bout: dict[str, list[BoutParticipant]] = defaultdict(list)
        for bp in parts_all:
            by_bout[bp.bout_id].append(bp)

        form_entries: dict[str, list[tuple[date_type, float]]] = defaultdict(list)
        for bp in parts_all:
            bout = bouts_map.get(bp.bout_id)
            if not bout:
                continue
            ev = events_map.get(bout.event_id) if bout and bout.event_id else None
            event_date = getattr(ev, 'event_date', None)
            if event_date is None:
                continue
            if bp.outcome == FightOutcome.WIN:
                target = 1.0
            elif bp.outcome == FightOutcome.DRAW:
                target = 0.5
            else:
                target = 0.0
            opponents = by_bout.get(bp.bout_id, [])
            opponent = next((op for op in opponents if op.fighter_id != bp.fighter_id), None)
            if opponent is None or bp.elo_before is None or opponent.elo_before is None:
                continue
            try:
                f_before = float(bp.elo_before)
                opp_before = float(opponent.elo_before)
            except (TypeError, ValueError):
                continue
            residual = float(target) - float(logistic_expect(f_before, opp_before, SCALE_S))
            form_entries[bp.fighter_id].append((event_date, residual))

        for entries in form_entries.values():
            entries.sort(key=lambda item: item[0])

        weight_lambda = log(2.0) / max(1.0, float(half_life_days)) if half_life_days > 0 else None

        def compute_fi(entry_list: list[tuple[date_type, float]]) -> tuple[float, int] | None:
            if not entry_list:
                return None
            items = entry_list
            if window == 'fights':
                items = items[-max(1, int(n)) :]
            else:
                try:
                    last_date_local = items[-1][0]
                    start = last_date_local - timedelta(days=max(1, int(n)))
                    items = [it for it in items if it[0] and it[0] >= start]
                except Exception:
                    items = items[-max(1, int(n)) :]
            if not items:
                return None
            try:
                last_date_local = items[-1][0]
                weighted_sum = 0.0
                weight_total = 0.0
                if weight_lambda is not None:
                    for dt, residual in items:
                        age_days = (last_date_local - dt).days if (last_date_local and dt) else 0
                        weight = exp(-weight_lambda * float(max(0, age_days)))
                        weighted_sum += weight * float(residual)
                        weight_total += weight
                if weight_total <= 0.0:
                    raise ZeroDivisionError
                fi_val = weighted_sum / weight_total
            except Exception:
                fi_val = sum(float(residual) for _, residual in items) / float(len(items))
            return fi_val, len(items)

        results: list[dict[str, Any]] = []
        for f in fighters:
            if min_recent > 0 and cutoff is not None and recent_counts.get(f.fighter_id, 0) < min_recent:
                continue
            try:
                entries = form_entries.get(f.fighter_id)
                if not entries:
                    continue
                fi_calc = compute_fi(entries)
                if not fi_calc:
                    continue
                fi_value, fi_count = fi_calc
                last_date = recent_last.get(f.fighter_id) or last_any.get(f.fighter_id)
                last_label = self._date_to_label(last_date)
                results.append(
                    {
                        'fighter_id': f.fighter_id,
                        'fighter_name': f.name,
                        'fi': float(fi_value),
                        'count': fi_count,
                        'recent_fights': recent_counts.get(f.fighter_id, fi_count),
                        'last_event_date': last_label,
                    }
                )
            except Exception as exc:
                logger.debug('form_top skipping %s: %r', f.fighter_id, exc)
                continue
        results.sort(key=lambda x: x['fi'], reverse=True)
        return results[: max(1, int(top))]

    # --- Finishing hazard & durability ---
    @with_uow
    async def hazard(  # noqa: PLR0912
        self, uow: UnitOfWork, *, fighter_id: str, five_round: str = 'auto'
    ) -> dict[str, Any]:
        """Compute simple finishing hazard histogram and durability for a fighter.

        - Bins are 5-minute intervals up to 25min; extended to 45min if five_round is 'auto' or 'true'.
        - KO/TKO and SUB finishes for the fighter are counted; finishes against count when the fighter lost by finish.
        - Durability is 1 - (finishes received per 15 minutes).
        """
        parts = await uow.bout_participants.get_by_fighter_id(fighter_id)
        bouts_map = {b.bout_id: b for b in await uow.bouts.get_all()}
        bins: list[tuple[int, int]] = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25)]
        if five_round in ('auto', 'true'):
            bins.extend([(25, 30), (30, 35), (35, 40), (40, 45)])

        ko: dict[int, int] = defaultdict(int)
        sub: dict[int, int] = defaultdict(int)
        got_finished: dict[int, int] = defaultdict(int)
        total_time = 0.0
        for p in parts:
            b = bouts_map.get(p.bout_id)
            if not b:
                continue
            # Duration minutes
            if getattr(b, 'method', None) and 'decision' in str(b.method).lower():
                dur = 25.0 if bool(getattr(b, 'is_title_fight', False)) else 15.0
            else:
                rounds = int(getattr(b, 'round_num', 1) or 1)
                tsec = int(getattr(b, 'time_sec', 0) or 0)
                dur = float((rounds - 1) * 5) + (tsec / 60.0)
            if dur < 0:
                dur = 0.0
            total_time += dur
            # Bucket finishes for
            m = str(getattr(b, 'method', '') or '')
            ml = m.lower()
            outc = str(getattr(p, 'outcome', '') or '').lower()
            if m and outc == 'win':
                end = dur
                for i, (lo, hi) in enumerate(bins):
                    if end > lo and end <= hi:
                        if ('ko' in ml) or ('tko' in ml):
                            ko[i] += 1
                        elif ('sub' in ml) or ('submission' in ml):
                            sub[i] += 1
                        break
            # Bucket finishes against
            if m and outc == 'loss' and 'decision' not in ml:
                end = dur
                for i, (lo, hi) in enumerate(bins):
                    if end > lo and end <= hi:
                        got_finished[i] += 1
                        break
        # Prepare output
        bins_out = [
            {'lo': lo, 'hi': hi, 'ko': ko[i], 'sub': sub[i], 'finished_against': got_finished[i]}
            for i, (lo, hi) in enumerate(bins)
        ]
        # Normalize durability per 15 minutes exposure
        denom_bouts15 = max(1.0, total_time / 15.0)
        durability = 1.0 - (sum(got_finished.values()) / denom_bouts15)
        return {'bins': bins_out, 'durability': durability}
