from __future__ import annotations

# ruff: noqa

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import update as sa_update

from elo_calculator.application.base_service import BaseService
from elo_calculator.application.services.elo_calculator import compute_elo_from_row
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.entities import Bout, BoutParticipant, Event
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.external_services.scrapers.ufcstats_event_scraper import UFCStatsEventScraper
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow

logger = get_logger()


@dataclass
class FightScrapeResult:
    event_id: UUID
    fight_links_processed: int
    bouts_created: int
    participants_created: int
    fighters_updated: int


class FightScrapeService(BaseService):
    def __init__(self, scraper: UFCStatsEventScraper | None = None) -> None:
        self._scraper = scraper or UFCStatsEventScraper()

    @with_uow
    async def scrape_event_fights_and_seed(self, uow: UnitOfWork, event_id: UUID) -> FightScrapeResult:
        evt = await uow.events.get_by_event_id(event_id)
        if not evt or not evt.event_stats_link:
            raise ValueError('Event missing or has no UFCStats link')

        fight_links = self._scraper.get_event_fight_links(evt.event_stats_link)
        bouts_created = 0
        participants_created = 0
        fighters_updated_set: set[str] = set()

        # Process from the first fight on the card to the main event
        for link in reversed(fight_links):
            try:
                scraped = self._scraper.get_fight(link, evt.event_date)
            except Exception as exc:
                logger.warning('Failed to scrape fight: link=%s err=%r', link, exc)
                continue

            # Ensure fighters exist by UFCStats id (we seed fighters so fighter_id == stats token)
            f1id = scraped.fighter1.fighter_id or ''
            f2id = scraped.fighter2.fighter_id or ''
            if not f1id or not f2id:
                logger.debug('Missing fighter ids on fight page; skipping link=%s', link)
                continue
            f1 = await uow.fighters.get_by_fighter_id(f1id)
            f2 = await uow.fighters.get_by_fighter_id(f2id)
            if not f1 or not f2:
                logger.info('Fighter not found for fight; skipping link=%s f1=%s f2=%s', link, f1id, f2id)
                continue

            # Upsert bout
            bout = await uow.bouts.get_by_bout_id(scraped.fight_id)
            if not bout:
                bout = Bout(
                    bout_id=scraped.fight_id,
                    event_id=evt.event_id,
                    is_title_fight=scraped.is_title_fight,
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
                        method=scraped.method,
                        round_num=scraped.round_num,
                        time_sec=scraped.time_sec,
                        time_format=scraped.time_format,
                    )
                )
                await uow.connection.execute(cmd)

            # Upsert participants
            p1 = await uow.bout_participants.get_by_bout_and_fighter(scraped.fight_id, f1id)
            if not p1:
                p1 = BoutParticipant(
                    bout_id=scraped.fight_id,
                    fighter_id=f1id,
                    outcome=_map_result(scraped.fighter1.result),
                    kd=scraped.fighter1.kd,
                    sig_strikes=scraped.fighter1.sig_strikes,
                    sig_strikes_thrown=scraped.fighter1.sig_strikes_thrown,
                    total_strikes=scraped.fighter1.total_strikes,
                    total_strikes_thrown=scraped.fighter1.total_strikes_thrown,
                    td=scraped.fighter1.td,
                    td_attempts=scraped.fighter1.td_attempts,
                    sub_attempts=scraped.fighter1.sub_attempts,
                    reversals=scraped.fighter1.rev,
                    control_time_sec=scraped.fighter1.ctrl,
                    head_ss=scraped.fighter1.head_ss,
                    body_ss=scraped.fighter1.body_ss,
                    leg_ss=scraped.fighter1.leg_ss,
                    distance_ss=scraped.fighter1.distance_ss,
                    clinch_ss=scraped.fighter1.clinch_ss,
                    ground_ss=scraped.fighter1.ground_ss,
                    # strike_accuracy will be derived by service if None
                )
                await uow.bout_participants.add(p1)
                participants_created += 1
            p2 = await uow.bout_participants.get_by_bout_and_fighter(scraped.fight_id, f2id)
            if not p2:
                p2 = BoutParticipant(
                    bout_id=scraped.fight_id,
                    fighter_id=f2id,
                    outcome=_map_result(scraped.fighter2.result),
                    kd=scraped.fighter2.kd,
                    sig_strikes=scraped.fighter2.sig_strikes,
                    sig_strikes_thrown=scraped.fighter2.sig_strikes_thrown,
                    total_strikes=scraped.fighter2.total_strikes,
                    total_strikes_thrown=scraped.fighter2.total_strikes_thrown,
                    td=scraped.fighter2.td,
                    td_attempts=scraped.fighter2.td_attempts,
                    sub_attempts=scraped.fighter2.sub_attempts,
                    reversals=scraped.fighter2.rev,
                    control_time_sec=scraped.fighter2.ctrl,
                    head_ss=scraped.fighter2.head_ss,
                    body_ss=scraped.fighter2.body_ss,
                    leg_ss=scraped.fighter2.leg_ss,
                    distance_ss=scraped.fighter2.distance_ss,
                    clinch_ss=scraped.fighter2.clinch_ss,
                    ground_ss=scraped.fighter2.ground_ss,
                )
                await uow.bout_participants.add(p2)
                participants_created += 1

            # Judges scores intentionally ignored

            # Compute ELO for this fight and persist immediately (sequential updates)
            extras = await self._build_extras(uow, evt, p1, p2)
            row = self._build_row(bout, p1, p2)
            outputs = compute_elo_from_row(
                row,
                extras={
                    'R1_before': float(f1.current_elo or f1.entry_elo or 1500.0),
                    'R2_before': float(f2.current_elo or f2.entry_elo or 1500.0),
                    'ufc_fights_before_1': extras['ufc_fights_before_1'],
                    'ufc_fights_before_2': extras['ufc_fights_before_2'],
                    'days_since_last_fight_1': extras['days_since_last_fight_1'],
                    'days_since_last_fight_2': extras['days_since_last_fight_2'],
                },
            )

            # Update participants with elo_before/after and extras
            await self._update_participant_row(
                uow,
                p1,
                elo_before=float(f1.current_elo or f1.entry_elo or 1500.0),
                elo_after=outputs.R1_after,
                ufc_fights_before=extras['ufc_fights_before_1'],
                days_since_last_fight=extras['days_since_last_fight_1'],
            )
            await self._update_participant_row(
                uow,
                p2,
                elo_before=float(f2.current_elo or f2.entry_elo or 1500.0),
                elo_after=outputs.R2_after,
                ufc_fights_before=extras['ufc_fights_before_2'],
                days_since_last_fight=extras['days_since_last_fight_2'],
            )

            # Update fighters current and peak elo
            f1_updated = await uow.fighters.update_fighter_elo(f1.fighter_id, float(outputs.R1_after))
            f2_updated = await uow.fighters.update_fighter_elo(f2.fighter_id, float(outputs.R2_after))
            fighters_updated_set.update([f1_updated.fighter_id, f2_updated.fighter_id])

        # Mark event
        try:
            await uow.events.update(evt.event_id, {'fights_seeded': True})
        except Exception:
            logger.warning('Failed to mark event fights_seeded: event_id=%s', evt.event_id)

        return FightScrapeResult(
            event_id=evt.event_id,
            fight_links_processed=len(fight_links),
            bouts_created=bouts_created,
            participants_created=participants_created,
            fighters_updated=len(fighters_updated_set),
        )

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

        row: dict[str, Any] = {
            'method': bout.method or '',
            'time_format': bout.time_format or '',
            'is_title_fight': bool(bout.is_title_fight),
            'round_num': bout.round_num,
            'time_sec': bout.time_sec,
            'fighter1_result': res_letter(p1.outcome),
            'fighter2_result': res_letter(p2.outcome),
        }

        def put_side(prefix: str, bp: BoutParticipant) -> None:
            row[f'{prefix}kd'] = bp.kd
            row[f'{prefix}sig_strikes'] = bp.sig_strikes
            row[f'{prefix}sig_strikes_thrown'] = bp.sig_strikes_thrown
            row[f'{prefix}total_strikes'] = bp.total_strikes
            row[f'{prefix}total_strikes_thrown'] = bp.total_strikes_thrown
            row[f'{prefix}td'] = bp.td
            row[f'{prefix}td_attempts'] = bp.td_attempts
            # Derived percents for PS (ensure numeric or None)
            try:
                row[f'{prefix}sig_strike_percent'] = (
                    round(100.0 * float(bp.sig_strikes) / float(bp.sig_strikes_thrown), 2)
                    if (bp.sig_strikes is not None and bp.sig_strikes_thrown and bp.sig_strikes_thrown > 0)
                    else None
                )
            except Exception:
                row[f'{prefix}sig_strike_percent'] = None
            try:
                row[f'{prefix}td_percent'] = (
                    round(100.0 * float(bp.td) / float(bp.td_attempts), 2)
                    if (bp.td is not None and bp.td_attempts and bp.td_attempts > 0)
                    else None
                )
            except Exception:
                row[f'{prefix}td_percent'] = None
            row[f'{prefix}sub_attempts'] = bp.sub_attempts
            # Use 'rev' as the canonical key (consumed by performance_score); 'reversals' omitted to avoid duplication
            row[f'{prefix}rev'] = bp.reversals
            row[f'{prefix}control_time_sec'] = bp.control_time_sec
            row[f'{prefix}ctrl'] = bp.control_time_sec
            row[f'{prefix}head_ss'] = bp.head_ss
            row[f'{prefix}body_ss'] = bp.body_ss
            row[f'{prefix}leg_ss'] = bp.leg_ss
            row[f'{prefix}distance_ss'] = bp.distance_ss
            row[f'{prefix}clinch_ss'] = bp.clinch_ss
            row[f'{prefix}ground_ss'] = bp.ground_ss

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
    if r.startswith('NC'):
        return FightOutcome.NO_CONTEST
    return FightOutcome.DRAW
