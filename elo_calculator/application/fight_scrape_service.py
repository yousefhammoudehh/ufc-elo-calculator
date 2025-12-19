from __future__ import annotations

# ruff: noqa

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import update as sa_update

from elo_calculator.application.base_service import BaseService
from elo_calculator.application.elo_calculator import compute_elo_from_row
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.entities import Bout, BoutParticipant, Event
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.external_services.scrapers.ufcstats_event_scraper import UFCStatsEventScraper
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow

logger = get_logger()


@dataclass
class FightEloTrace:
    fight_index: int
    bout_id: str
    link: str
    fighter1_id: str
    fighter2_id: str
    fighter1_result: str
    fighter2_result: str
    # Fighter 1 parsed stats
    fighter1_kd: int
    fighter1_sig_strikes: int
    fighter1_sig_strikes_thrown: int
    fighter1_sig_strike_percent: float
    fighter1_total_strikes: int
    fighter1_total_strikes_thrown: int
    fighter1_strike_accuracy: float
    fighter1_td: int
    fighter1_td_attempts: int
    fighter1_td_percent: float
    fighter1_sub_attempts: int
    fighter1_rev: int
    fighter1_control_time_sec: int
    fighter1_head_ss: int
    fighter1_body_ss: int
    fighter1_leg_ss: int
    fighter1_distance_ss: int
    fighter1_clinch_ss: int
    fighter1_ground_ss: int
    # Fighter 2 parsed stats
    fighter2_kd: int
    fighter2_sig_strikes: int
    fighter2_sig_strikes_thrown: int
    fighter2_sig_strike_percent: float
    fighter2_total_strikes: int
    fighter2_total_strikes_thrown: int
    fighter2_strike_accuracy: float
    fighter2_td: int
    fighter2_td_attempts: int
    fighter2_td_percent: float
    fighter2_sub_attempts: int
    fighter2_rev: int
    fighter2_control_time_sec: int
    fighter2_head_ss: int
    fighter2_body_ss: int
    fighter2_leg_ss: int
    fighter2_distance_ss: int
    fighter2_clinch_ss: int
    fighter2_ground_ss: int
    R1_before: float
    R1_after: float
    E1: float
    Y1: float
    K1: float
    R2_before: float
    R2_after: float
    E2: float
    Y2: float
    K2: float


@dataclass
class FightScrapeResult:
    event_id: UUID
    fight_links_processed: int
    bouts_created: int
    participants_created: int
    fighters_updated: int
    fights: list[FightEloTrace]


class FightScrapeService(BaseService):
    def __init__(self, scraper: UFCStatsEventScraper | None = None) -> None:
        self._scraper = scraper or UFCStatsEventScraper()

    @with_uow
    async def scrape_event_fights_and_seed(
        self, uow: UnitOfWork, event_id: UUID, *, strict: bool = True
    ) -> FightScrapeResult:
        evt = await uow.events.get_by_event_id(event_id)
        if not evt or not evt.event_stats_link:
            raise ValueError('Event missing or has no UFCStats link')
        # Enforce precondition: fighters must be seeded to avoid duplications or mismatches
        if not getattr(evt, 'fighters_seeded', False):
            raise ValueError(
                f'Event fighters not seeded; ingest fighters first: event_id={evt.event_id} link={evt.event_link}'
            )

        fight_links = self._scraper.get_event_fight_links(evt.event_stats_link)
        bouts_created = 0
        participants_created = 0
        fighters_updated_set: set[str] = set()
        traces: list[FightEloTrace] = []
        # Ensure intra-event sequential Elo propagation regardless of DB transaction visibility
        elo_overrides: dict[str, float] = {}

        # Process from the first fight on the card to the main event
        processed = 0
        for idx, link in enumerate(reversed(fight_links), start=1):
            try:
                scraped = self._scraper.get_fight(link, evt.event_date)
            except Exception as exc:
                logger.warning(f'Failed to scrape fight: link={link} err={exc!r}')
                if strict:
                    raise
                continue

            # Ensure fighters exist in DB; tolerate cases where DB PK != UFCStats token by resolving via stats_link or name
            f1_token = scraped.fighter1.fighter_id or ''
            f2_token = scraped.fighter2.fighter_id or ''
            if not f1_token or not f2_token:
                logger.debug(f'Missing fighter ids on fight page; skipping link={link}')
                if strict:
                    raise ValueError(f'Missing fighter ids on fight page: {link}')
                continue

            async def _resolve_fighter(token: str, name: str | None) -> tuple[Any | None, str | None]:
                # 1) Direct by fighter_id (when PK equals UFCStats token)
                f = await uow.fighters.get_by_fighter_id(token)
                if f:
                    return f, f.fighter_id
                # 2) By stats_link (handle http/https canonical variants)
                https = f'https://www.ufcstats.com/fighter-details/{token}'
                http = f'http://www.ufcstats.com/fighter-details/{token}'
                f = await uow.fighters.get_by_stats_link(https)
                if not f:
                    f = await uow.fighters.get_by_stats_link(http)
                if f:
                    # If existing fighter lacks stats_link, try to patch to canonical https link (best-effort)
                    try:
                        if not getattr(f, 'stats_link', None):
                            cmd = (
                                sa_update(uow.fighters.table)
                                .where(uow.fighters.table.c.fighter_id == f.fighter_id)
                                .values(stats_link=https)
                            )
                            await uow.connection.execute(cmd)
                    except Exception:
                        pass
                    return f, f.fighter_id
                # Do NOT fallback to fuzzy/name matching — this risks misattribution.
                return None, None

            f1, f1_db_id = await _resolve_fighter(f1_token, getattr(scraped.fighter1, 'name', None))
            f2, f2_db_id = await _resolve_fighter(f2_token, getattr(scraped.fighter2, 'name', None))
            if not f1 or not f1_db_id or not f2 or not f2_db_id:
                missing: list[str] = []
                if not f1 or not f1_db_id:
                    missing.append(
                        f'fighter (id={f1_token}, name={getattr(scraped.fighter1, "name", None)}) not found in database'
                    )
                if not f2 or not f2_db_id:
                    missing.append(
                        f'fighter (id={f2_token}, name={getattr(scraped.fighter2, "name", None)}) not found in database'
                    )
                msg = f'; '.join(missing) + f'; event_id={evt.event_id} link={link}'
                logger.error(msg)
                if strict:
                    raise ValueError(msg)
                continue

            # Upsert bout
            bout = await uow.bouts.get_by_bout_id(scraped.fight_id)
            if not bout:
                bout = Bout(
                    bout_id=scraped.fight_id,
                    event_id=evt.event_id,
                    is_title_fight=scraped.is_title_fight,
                    weight_class_code=scraped.weight_class_code,
                    method=scraped.method,
                    round_num=scraped.round_num,
                    time_sec=scraped.time_sec,
                    time_format=scraped.time_format,
                )
                await uow.bouts.add(bout)
                bouts_created += 1
            else:
                # Patch metadata if missing
                cmd = (
                    sa_update(uow.bouts.table)
                    .where(uow.bouts.table.c.bout_id == bout.bout_id)
                    .values(
                        is_title_fight=bool(scraped.is_title_fight),
                        weight_class_code=scraped.weight_class_code,
                        method=scraped.method,
                        round_num=scraped.round_num,
                        time_sec=scraped.time_sec,
                        time_format=scraped.time_format,
                    )
                )
                await uow.connection.execute(cmd)

            # Upsert participants
            # Normalize results to consistent pair (W/L, D, NC)
            r1_letter, r2_letter = _normalize_results(scraped.fighter1.result, scraped.fighter2.result, scraped.method)
            o1 = _map_result(r1_letter)
            o2 = _map_result(r2_letter)

            # Helper to coerce numeric None to 0
            def _i(val: int | None) -> int:
                try:
                    return int(val or 0)
                except Exception:
                    return 0

            def _acc(total_l: int | None, total_a: int | None, provided: float | None) -> Decimal:
                # strike_accuracy stored as 0..1; derive when possible else fallback to provided or 0
                l = _i(total_l)
                a = _i(total_a)
                if a > 0:
                    return Decimal(str(round(l / a, 4)))
                try:
                    if provided is not None:
                        return Decimal(str(round(float(provided), 4)))
                except Exception:
                    pass
                return Decimal('0')

            p1 = await uow.bout_participants.get_by_bout_and_fighter(scraped.fight_id, f1_db_id)
            if not p1:
                p1 = BoutParticipant(
                    bout_id=scraped.fight_id,
                    fighter_id=f1_db_id,
                    outcome=o1,
                    kd=_i(scraped.fighter1.kd),
                    sig_strikes=_i(scraped.fighter1.sig_strikes),
                    sig_strikes_thrown=_i(scraped.fighter1.sig_strikes_thrown),
                    total_strikes=_i(scraped.fighter1.total_strikes),
                    total_strikes_thrown=_i(scraped.fighter1.total_strikes_thrown),
                    td=_i(scraped.fighter1.td),
                    td_attempts=_i(scraped.fighter1.td_attempts),
                    sub_attempts=_i(scraped.fighter1.sub_attempts),
                    reversals=_i(scraped.fighter1.rev),
                    control_time_sec=_i(scraped.fighter1.ctrl),
                    head_ss=_i(scraped.fighter1.head_ss),
                    body_ss=_i(scraped.fighter1.body_ss),
                    leg_ss=_i(scraped.fighter1.leg_ss),
                    distance_ss=_i(scraped.fighter1.distance_ss),
                    clinch_ss=_i(scraped.fighter1.clinch_ss),
                    ground_ss=_i(scraped.fighter1.ground_ss),
                    strike_accuracy=_acc(
                        scraped.fighter1.total_strikes,
                        scraped.fighter1.total_strikes_thrown,
                        scraped.fighter1.strike_accuracy,
                    ),
                )
                await uow.bout_participants.add(p1)
                participants_created += 1
            else:
                # Update in-memory values so ELO/PS use the freshly scraped stats even in dry-run
                p1.outcome = o1
                p1.kd = _i(scraped.fighter1.kd)
                p1.sig_strikes = _i(scraped.fighter1.sig_strikes)
                p1.sig_strikes_thrown = _i(scraped.fighter1.sig_strikes_thrown)
                p1.total_strikes = _i(scraped.fighter1.total_strikes)
                p1.total_strikes_thrown = _i(scraped.fighter1.total_strikes_thrown)
                p1.td = _i(scraped.fighter1.td)
                p1.td_attempts = _i(scraped.fighter1.td_attempts)
                p1.sub_attempts = _i(scraped.fighter1.sub_attempts)
                p1.reversals = _i(scraped.fighter1.rev)
                p1.control_time_sec = _i(scraped.fighter1.ctrl)
                p1.head_ss = _i(scraped.fighter1.head_ss)
                p1.body_ss = _i(scraped.fighter1.body_ss)
                p1.leg_ss = _i(scraped.fighter1.leg_ss)
                p1.distance_ss = _i(scraped.fighter1.distance_ss)
                p1.clinch_ss = _i(scraped.fighter1.clinch_ss)
                p1.ground_ss = _i(scraped.fighter1.ground_ss)
                p1.strike_accuracy = _acc(
                    scraped.fighter1.total_strikes,
                    scraped.fighter1.total_strikes_thrown,
                    scraped.fighter1.strike_accuracy,
                )

            p2 = await uow.bout_participants.get_by_bout_and_fighter(scraped.fight_id, f2_db_id)
            if not p2:
                p2 = BoutParticipant(
                    bout_id=scraped.fight_id,
                    fighter_id=f2_db_id,
                    outcome=o2,
                    kd=_i(scraped.fighter2.kd),
                    sig_strikes=_i(scraped.fighter2.sig_strikes),
                    sig_strikes_thrown=_i(scraped.fighter2.sig_strikes_thrown),
                    total_strikes=_i(scraped.fighter2.total_strikes),
                    total_strikes_thrown=_i(scraped.fighter2.total_strikes_thrown),
                    td=_i(scraped.fighter2.td),
                    td_attempts=_i(scraped.fighter2.td_attempts),
                    sub_attempts=_i(scraped.fighter2.sub_attempts),
                    reversals=_i(scraped.fighter2.rev),
                    control_time_sec=_i(scraped.fighter2.ctrl),
                    head_ss=_i(scraped.fighter2.head_ss),
                    body_ss=_i(scraped.fighter2.body_ss),
                    leg_ss=_i(scraped.fighter2.leg_ss),
                    distance_ss=_i(scraped.fighter2.distance_ss),
                    clinch_ss=_i(scraped.fighter2.clinch_ss),
                    ground_ss=_i(scraped.fighter2.ground_ss),
                    strike_accuracy=_acc(
                        scraped.fighter2.total_strikes,
                        scraped.fighter2.total_strikes_thrown,
                        scraped.fighter2.strike_accuracy,
                    ),
                )
                await uow.bout_participants.add(p2)
                participants_created += 1
            else:
                # Update in-memory values so ELO/PS use the freshly scraped stats even in dry-run
                p2.outcome = o2
                p2.kd = _i(scraped.fighter2.kd)
                p2.sig_strikes = _i(scraped.fighter2.sig_strikes)
                p2.sig_strikes_thrown = _i(scraped.fighter2.sig_strikes_thrown)
                p2.total_strikes = _i(scraped.fighter2.total_strikes)
                p2.total_strikes_thrown = _i(scraped.fighter2.total_strikes_thrown)
                p2.td = _i(scraped.fighter2.td)
                p2.td_attempts = _i(scraped.fighter2.td_attempts)
                p2.sub_attempts = _i(scraped.fighter2.sub_attempts)
                p2.reversals = _i(scraped.fighter2.rev)
                p2.control_time_sec = _i(scraped.fighter2.ctrl)
                p2.head_ss = _i(scraped.fighter2.head_ss)
                p2.body_ss = _i(scraped.fighter2.body_ss)
                p2.leg_ss = _i(scraped.fighter2.leg_ss)
                p2.distance_ss = _i(scraped.fighter2.distance_ss)
                p2.clinch_ss = _i(scraped.fighter2.clinch_ss)
                p2.ground_ss = _i(scraped.fighter2.ground_ss)
                p2.strike_accuracy = _acc(
                    scraped.fighter2.total_strikes,
                    scraped.fighter2.total_strikes_thrown,
                    scraped.fighter2.strike_accuracy,
                )

            # Judges scores intentionally ignored

            # Compute ELO for this fight and persist immediately (sequential updates)
            extras = await self._build_extras(uow, evt, p1, p2)
            row = self._build_row(bout, p1, p2)
            # Use latest in-memory Elo if present, else fall back to DB values
            r1_before = float(elo_overrides.get(f1_db_id, float(f1.current_elo or f1.entry_elo or 1500.0)))
            r2_before = float(elo_overrides.get(f2_db_id, float(f2.current_elo or f2.entry_elo or 1500.0)))
            outputs = compute_elo_from_row(
                row,
                extras={
                    'R1_before': r1_before,
                    'R2_before': r2_before,
                    'ufc_fights_before_1': extras['ufc_fights_before_1'],
                    'ufc_fights_before_2': extras['ufc_fights_before_2'],
                    'days_since_last_fight_1': extras['days_since_last_fight_1'],
                    'days_since_last_fight_2': extras['days_since_last_fight_2'],
                },
            )

            processed += 1

            # Record trace regardless of dry_run
            # Prepare convenience getters
            def _i(val: int | None) -> int:
                try:
                    return int(val or 0)
                except Exception:
                    return 0

            def _pct(n: int | None, d: int | None) -> float:
                ni = _i(n)
                di = _i(d)
                return round(100.0 * ni / max(1, di), 2)

            traces.append(
                FightEloTrace(
                    fight_index=idx,
                    bout_id=bout.bout_id,
                    link=link,
                    fighter1_id=f1_token,
                    fighter2_id=f2_token,
                    fighter1_result=r1_letter,
                    fighter2_result=r2_letter,
                    # Stats for fighter1
                    fighter1_kd=_i(p1.kd),
                    fighter1_sig_strikes=_i(p1.sig_strikes),
                    fighter1_sig_strikes_thrown=_i(p1.sig_strikes_thrown),
                    fighter1_sig_strike_percent=_pct(p1.sig_strikes, p1.sig_strikes_thrown),
                    fighter1_total_strikes=_i(p1.total_strikes),
                    fighter1_total_strikes_thrown=_i(p1.total_strikes_thrown),
                    fighter1_strike_accuracy=float(p1.strike_accuracy or Decimal('0')),
                    fighter1_td=_i(p1.td),
                    fighter1_td_attempts=_i(p1.td_attempts),
                    fighter1_td_percent=_pct(p1.td, p1.td_attempts),
                    fighter1_sub_attempts=_i(p1.sub_attempts),
                    fighter1_rev=_i(p1.reversals),
                    fighter1_control_time_sec=_i(p1.control_time_sec),
                    fighter1_head_ss=_i(p1.head_ss),
                    fighter1_body_ss=_i(p1.body_ss),
                    fighter1_leg_ss=_i(p1.leg_ss),
                    fighter1_distance_ss=_i(p1.distance_ss),
                    fighter1_clinch_ss=_i(p1.clinch_ss),
                    fighter1_ground_ss=_i(p1.ground_ss),
                    # Stats for fighter2
                    fighter2_kd=_i(p2.kd),
                    fighter2_sig_strikes=_i(p2.sig_strikes),
                    fighter2_sig_strikes_thrown=_i(p2.sig_strikes_thrown),
                    fighter2_sig_strike_percent=_pct(p2.sig_strikes, p2.sig_strikes_thrown),
                    fighter2_total_strikes=_i(p2.total_strikes),
                    fighter2_total_strikes_thrown=_i(p2.total_strikes_thrown),
                    fighter2_strike_accuracy=float(p2.strike_accuracy or Decimal('0')),
                    fighter2_td=_i(p2.td),
                    fighter2_td_attempts=_i(p2.td_attempts),
                    fighter2_td_percent=_pct(p2.td, p2.td_attempts),
                    fighter2_sub_attempts=_i(p2.sub_attempts),
                    fighter2_rev=_i(p2.reversals),
                    fighter2_control_time_sec=_i(p2.control_time_sec),
                    fighter2_head_ss=_i(p2.head_ss),
                    fighter2_body_ss=_i(p2.body_ss),
                    fighter2_leg_ss=_i(p2.leg_ss),
                    fighter2_distance_ss=_i(p2.distance_ss),
                    fighter2_clinch_ss=_i(p2.clinch_ss),
                    fighter2_ground_ss=_i(p2.ground_ss),
                    R1_before=r1_before,
                    R1_after=float(outputs.R1_after),
                    E1=float(outputs.E1),
                    Y1=float(outputs.Y1),
                    K1=float(outputs.K1),
                    R2_before=r2_before,
                    R2_after=float(outputs.R2_after),
                    E2=float(outputs.E2),
                    Y2=float(outputs.Y2),
                    K2=float(outputs.K2),
                )
            )

            # Update participants with elo_before/after and extras
            await self._update_participant_row(
                uow,
                p1,
                elo_before=r1_before,
                elo_after=outputs.R1_after,
                ufc_fights_before=extras['ufc_fights_before_1'],
                days_since_last_fight=extras['days_since_last_fight_1'],
            )
            await self._update_participant_row(
                uow,
                p2,
                elo_before=r2_before,
                elo_after=outputs.R2_after,
                ufc_fights_before=extras['ufc_fights_before_2'],
                days_since_last_fight=extras['days_since_last_fight_2'],
            )

            # Update fighters current and peak elo
            f1_updated = await uow.fighters.update_fighter_elo(f1.fighter_id, float(outputs.R1_after))
            f2_updated = await uow.fighters.update_fighter_elo(f2.fighter_id, float(outputs.R2_after))
            fighters_updated_set.update([f1_updated.fighter_id, f2_updated.fighter_id])
            # Update in-memory latest Elo to propagate within this event
            elo_overrides[f1_db_id] = float(outputs.R1_after)
            elo_overrides[f2_db_id] = float(outputs.R2_after)
            # Also patch local objects for completeness
            try:
                f1.current_elo = float(outputs.R1_after)
                f2.current_elo = float(outputs.R2_after)
            except Exception:
                pass

        # Mark event
        # Only mark fights_seeded when all fights were successfully processed
        if processed != len(fight_links):
            msg = (
                f'Event fights not fully seeded: event_id={evt.event_id} processed={processed} total={len(fight_links)}'
            )
            logger.error(msg)
            if strict:
                raise RuntimeError(msg)
        else:
            try:
                await uow.events.update(evt.event_id, {'fights_seeded': True})
            except Exception:
                logger.warning(f'Failed to mark event fights_seeded: event_id={evt.event_id}')

        # Persistence is enabled by default; no rollback-only mode

        return FightScrapeResult(
            event_id=evt.event_id,
            fight_links_processed=len(fight_links),
            bouts_created=bouts_created,
            participants_created=participants_created,
            fighters_updated=len(fighters_updated_set),
            fights=traces,
        )

    @with_uow
    async def seed_first_unseeded_events_fights(
        self, uow: UnitOfWork, limit: int, *, strict: bool = True
    ) -> list[FightScrapeResult]:
        """Seed fights (scrape + ELO) for the first N events not yet fights_seeded, oldest first.

        Strict mode: if any fight in an event fails or is missing required data, raise and abort the entire run.
        """
        page = 1
        page_size = 50
        selected: list[Event] = []
        while len(selected) < limit:
            events_page, total = await uow.events.get_paginated(page, page_size, sort_by='event_date', order='asc')
            if not events_page:
                break
            for evt in events_page:
                if len(selected) >= limit:
                    break
                if not getattr(evt, 'fights_seeded', False) and evt.event_stats_link:
                    # If fighters are not yet seeded for this event, fail-fast to preserve chronological order
                    if not getattr(evt, 'fighters_seeded', False):
                        raise ValueError(
                            f'Cannot seed fights before fighters are ingested: event_id={evt.event_id} link={evt.event_link}'
                        )
                    selected.append(evt)
            if page * page_size >= total:
                break
            page += 1

        results: list[FightScrapeResult] = []
        for evt in selected:
            # Process with strict behavior; bubble up any error to stop everything
            res = await self.scrape_event_fights_and_seed(evt.event_id, strict=strict)
            results.append(res)
        return results

    async def _build_extras(
        self, uow: UnitOfWork, evt: Event, p1: BoutParticipant, p2: BoutParticipant
    ) -> dict[str, int]:
        async def prior_stats(fid: str) -> tuple[int, int]:
            participations = await uow.bout_participants.get_by_fighter_id(fid)
            # Collect dates for prior bouts
            dates: list[Any] = []
            for bp in participations:
                bout = await uow.bouts.get_by_bout_id(bp.bout_id)
                if not bout or not bout.event_id:
                    continue
                ev = await uow.events.get_by_event_id(bout.event_id)
                if not ev or not ev.event_date:
                    continue
                if ev.event_date < evt.event_date:
                    dates.append(ev.event_date)
            dates.sort()
            count = len(dates)
            delta_days = (evt.event_date - dates[-1]).days if dates else 10_000
            return count, int(delta_days)

        c1, d1 = await prior_stats(p1.fighter_id)
        c2, d2 = await prior_stats(p2.fighter_id)
        return {
            'ufc_fights_before_1': c1,
            'ufc_fights_before_2': c2,
            'days_since_last_fight_1': d1,
            'days_since_last_fight_2': d2,
        }

    def _build_row(self, bout: Bout, p1: BoutParticipant, p2: BoutParticipant) -> dict[str, Any]:
        def res_letter(outcome: FightOutcome | None) -> str:
            if outcome == FightOutcome.WIN:
                return 'W'
            if outcome == FightOutcome.LOSS:
                return 'L'
            if outcome == FightOutcome.DRAW:
                return 'D'
            if outcome == FightOutcome.NO_CONTEST:
                return 'NC'
            return ''

        # Coerce round/time to ints with 0 default
        row: dict[str, Any] = {
            'method': bout.method or '',
            'time_format': bout.time_format or '',
            'is_title_fight': bool(bout.is_title_fight),
            'round_num': int(bout.round_num or 0) if bout.round_num is not None else 0,
            'time_sec': int(bout.time_sec or 0) if bout.time_sec is not None else 0,
            'fighter1_result': res_letter(p1.outcome),
            'fighter2_result': res_letter(p2.outcome),
        }

        def put_side(prefix: str, bp: BoutParticipant) -> None:
            def _i(val: int | None) -> int:
                try:
                    return int(val or 0)
                except Exception:
                    return 0

            ss_l = _i(bp.sig_strikes)
            ss_a = _i(bp.sig_strikes_thrown)
            td_l = _i(bp.td)
            td_a = _i(bp.td_attempts)

            row[f'{prefix}kd'] = _i(bp.kd)
            row[f'{prefix}sig_strikes'] = ss_l
            row[f'{prefix}sig_strikes_thrown'] = ss_a
            row[f'{prefix}total_strikes'] = _i(bp.total_strikes)
            row[f'{prefix}total_strikes_thrown'] = _i(bp.total_strikes_thrown)
            row[f'{prefix}td'] = td_l
            row[f'{prefix}td_attempts'] = td_a
            # Derived percents for PS (0.0 when attempts are 0 or missing)
            row[f'{prefix}sig_strike_percent'] = round(100.0 * ss_l / max(1, ss_a), 2)
            row[f'{prefix}td_percent'] = round(100.0 * td_l / max(1, td_a), 2)
            row[f'{prefix}sub_attempts'] = _i(bp.sub_attempts)
            # Use 'rev' as the canonical key (consumed by performance_score); 'reversals' omitted to avoid duplication
            row[f'{prefix}rev'] = _i(bp.reversals)
            row[f'{prefix}control_time_sec'] = _i(bp.control_time_sec)
            row[f'{prefix}ctrl'] = _i(bp.control_time_sec)
            row[f'{prefix}head_ss'] = _i(bp.head_ss)
            row[f'{prefix}body_ss'] = _i(bp.body_ss)
            row[f'{prefix}leg_ss'] = _i(bp.leg_ss)
            row[f'{prefix}distance_ss'] = _i(bp.distance_ss)
            row[f'{prefix}clinch_ss'] = _i(bp.clinch_ss)
            row[f'{prefix}ground_ss'] = _i(bp.ground_ss)

        put_side('fighter1_', p1)
        put_side('fighter2_', p2)
        return row

    async def _update_participant_row(
        self,
        uow: UnitOfWork,
        bp: BoutParticipant,
        *,
        elo_before: float,
        elo_after: float,
        ufc_fights_before: int,
        days_since_last_fight: int,
    ) -> None:
        cmd = (
            sa_update(uow.bout_participants.table)
            .where(
                (uow.bout_participants.table.c.bout_id == bp.bout_id)
                & (uow.bout_participants.table.c.fighter_id == bp.fighter_id)
            )
            .values(
                elo_before=float(elo_before),
                elo_after=float(elo_after),
                ufc_fights_before=int(ufc_fights_before),
                days_since_last_fight=int(days_since_last_fight),
            )
        )
        await uow.connection.execute(cmd)


def _map_result(result: str | None) -> FightOutcome:
    if not result:
        return FightOutcome.DRAW
    r = result.strip().upper()
    if r.startswith('W'):
        return FightOutcome.WIN
    if r.startswith('L'):
        return FightOutcome.LOSS
    if r.startswith('D'):
        return FightOutcome.DRAW
    if r.startswith('NC') or 'NO CONTEST' in r:
        return FightOutcome.NO_CONTEST
    return FightOutcome.DRAW


def _normalize_results(r1: str | None, r2: str | None, method: str | None) -> tuple[str, str]:
    """Return normalized result letters (W/L/D/NC) for fighter1 and fighter2.

    Applies symmetry (if one is W the other is L), and falls back to method text for draws/NC.
    Returns empty strings when unknown.
    """

    def _norm(x: str | None) -> str:
        s = (x or '').strip().upper()
        if s.startswith('W'):
            return 'W'
        if s.startswith('L'):
            return 'L'
        if s.startswith('D'):
            return 'D'
        if s.startswith('NC') or 'NO CONTEST' in s:
            return 'NC'
        return ''

    a = _norm(r1)
    b = _norm(r2)
    if a in ('W', 'L') and b == '':
        b = 'L' if a == 'W' else 'W'
    if b in ('W', 'L') and a == '':
        a = 'L' if b == 'W' else 'W'
    if a == '' and b == '':
        m = (method or '').lower()
        if 'draw' in m:
            a = b = 'D'
        elif 'no contest' in m or m.startswith('nc'):
            a = b = 'NC'
    return a, b
