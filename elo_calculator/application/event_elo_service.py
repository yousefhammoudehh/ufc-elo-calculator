from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy import update as sa_update

from elo_calculator.application.base_service import BaseService
from elo_calculator.application.elo_calculator import compute_elo_from_row
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.entities import Bout, BoutParticipant, Event
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow

logger = get_logger()


@dataclass
class EventEloResult:
    event_id: UUID
    bouts_processed: int
    participants_updated: int
    fighters_updated: int


class EventEloService(BaseService):
    @staticmethod
    async def _prior_title_fights(
        uow: UnitOfWork, fighter_id: str, before_event_date: date
    ) -> list[tuple[date, FightOutcome | None]]:
        out: list[tuple[date, FightOutcome | None]] = []
        if not before_event_date:
            return out
        parts = await uow.bout_participants.get_by_fighter_id(fighter_id)
        for bp in parts:
            b = await uow.bouts.get_by_bout_id(bp.bout_id)
            if not b or not b.is_title_fight or not b.event_id:
                continue
            ev = await uow.events.get_by_event_id(b.event_id)
            if not ev or not ev.event_date:
                continue
            if ev.event_date < before_event_date:
                out.append((ev.event_date, bp.outcome))
        out.sort(key=lambda t: t[0], reverse=True)
        return out

    @staticmethod
    def _entered_as_champion(priors_desc: list[tuple[date, FightOutcome | None]]) -> bool:
        # Most recent decisive prior result determines status; ignores DRAW/NC.
        for _d, outcome in priors_desc:
            if outcome == FightOutcome.WIN:
                return True
            if outcome == FightOutcome.LOSS:
                return False
        return False

    @staticmethod
    def _unification_winner_is_undisputed(
        winner_priors_desc: list[tuple[date, FightOutcome | None]],
        loser_priors_desc: list[tuple[date, FightOutcome | None]],
    ) -> bool:
        def count_wins(priors: list[tuple[date, FightOutcome | None]]) -> int:
            return sum(1 for _d, o in priors if o == FightOutcome.WIN)

        def first_win_date(priors: list[tuple[date, FightOutcome | None]]) -> date | None:
            wins: list[date] = [d for d, o in priors if o == FightOutcome.WIN]
            return min(wins) if wins else None

        cw = count_wins(winner_priors_desc)
        cl = count_wins(loser_priors_desc)
        if cw != cl:
            return cw > cl
        fw = first_win_date(winner_priors_desc)
        fl = first_win_date(loser_priors_desc)
        if fw and fl and fw != fl:
            return fw < fl
        return True

    @with_uow
    async def seed_events_by_ids(self, uow: UnitOfWork, event_ids: list[UUID]) -> list[EventEloResult]:
        results: list[EventEloResult] = []
        for eid in event_ids:
            evt = await uow.events.get_by_event_id(eid)
            if not evt:
                logger.warning(f'Event not found; skipping: event_id={eid!s}')
                continue
            res = await self._seed_single_event(uow, evt)
            results.append(res)
        return results

    async def _seed_single_event(self, uow: UnitOfWork, evt: Event) -> EventEloResult:
        bouts = await uow.bouts.get_by_event(evt.event_id)
        # Deterministic but arbitrary order; ELO within same event has negligible intra-event dependency
        bouts_sorted = sorted(bouts, key=lambda b: (b.bout_id or ''))

        bouts_processed = 0
        participants_updated = 0
        fighters_updated_set: set[str] = set()

        for bout in bouts_sorted:
            try:
                updated_count, fighter_ids = await self._process_bout(uow, evt, bout)
            except Exception as exc:
                logger.warning(
                    f'Failed processing bout; continuing. event_id={evt.event_id!s} bout_id={bout.bout_id} err={exc!r}'
                )
                continue
            if updated_count:
                bouts_processed += 1
                participants_updated += updated_count
                fighters_updated_set.update(fighter_ids)

        # Mark event fights seeded if we successfully processed any bouts
        try:
            await uow.events.update(evt.event_id, {'fights_seeded': True})
        except Exception:
            logger.warning(f'Failed to mark event fights_seeded: event_id={evt.event_id!s}')

        return EventEloResult(
            event_id=evt.event_id,
            bouts_processed=bouts_processed,
            participants_updated=participants_updated,
            fighters_updated=len(fighters_updated_set),
        )

    async def _process_bout(
        self, uow: UnitOfWork, evt: Event, bout: Bout
    ) -> tuple[int, list[str]]:  # returns (participants_updated_count, fighter_ids)
        participants = await uow.bout_participants.get_by_bout_id(bout.bout_id)
        expected_participants = 2
        if len(participants) != expected_participants:
            logger.debug(
                f'Bout has {len(participants)} participants; expected {expected_participants}. Skipping: {bout.bout_id}'
            )
            return 0, []

        p1, p2 = participants[0], participants[1]
        f1 = await uow.fighters.get_by_fighter_id(p1.fighter_id)
        f2 = await uow.fighters.get_by_fighter_id(p2.fighter_id)
        if not f1 or not f2:
            logger.debug(f'Missing fighter for bout participants; skipping bout_id={bout.bout_id}')
            return 0, []

        # Build extras per fighter
        extras = await self._build_extras(uow, evt, p1, p2)

        # Build row for compute_elo_from_row
        row = self._build_row(bout, p1, p2)

        # Attach title_bout_type for Elo per walkout status if title fight and clear winner
        if bool(bout.is_title_fight) and evt and getattr(evt, 'event_date', None):
            winner_pid: str | None = None
            loser_pid: str | None = None
            if p1.outcome == FightOutcome.WIN and p2.outcome != FightOutcome.WIN:
                winner_pid, loser_pid = p1.fighter_id, p2.fighter_id
            elif p2.outcome == FightOutcome.WIN and p1.outcome != FightOutcome.WIN:
                winner_pid, loser_pid = p2.fighter_id, p1.fighter_id
            if winner_pid and loser_pid:
                priors_w = await self._prior_title_fights(uow, winner_pid, evt.event_date)
                priors_l = await self._prior_title_fights(uow, loser_pid, evt.event_date)
                entered_w = self._entered_as_champion(priors_w)
                entered_l = self._entered_as_champion(priors_l)
                if entered_w and entered_l:
                    winner_is_undisputed = self._unification_winner_is_undisputed(priors_w, priors_l)
                    row['title_bout_type'] = 'defense' if winner_is_undisputed else 'capture'
                elif entered_w:
                    row['title_bout_type'] = 'defense'
                else:
                    row['title_bout_type'] = 'capture'

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

        # Update fighters' current_elo and peak_elo if needed
        f1_updated = await uow.fighters.update_fighter_elo(p1.fighter_id, float(outputs.R1_after))
        f2_updated = await uow.fighters.update_fighter_elo(p2.fighter_id, float(outputs.R2_after))

        return 2, [f1_updated.fighter_id, f2_updated.fighter_id]

    async def _build_extras(
        self, uow: UnitOfWork, evt: Event, p1: BoutParticipant, p2: BoutParticipant
    ) -> dict[str, int]:
        # Count prior UFC bouts and days since last fight per fighter based on event dates
        async def prior_stats(fid: str) -> tuple[int, int]:
            participations = await uow.bout_participants.get_by_fighter_id(fid)
            # Collect dates for those bouts
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
            if dates:
                last_date = dates[-1]
                delta_days = (evt.event_date - last_date).days
            else:
                delta_days = 10_000  # effectively "no recent activity"
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

        # Map participant stat fields to fighter1_/fighter2_ names expected by performance_score
        def put_side(prefix: str, bp: BoutParticipant) -> None:
            row[f'{prefix}kd'] = bp.kd
            row[f'{prefix}sig_strikes'] = bp.sig_strikes
            row[f'{prefix}sig_strikes_thrown'] = bp.sig_strikes_thrown
            row[f'{prefix}total_strikes'] = bp.total_strikes
            row[f'{prefix}total_strikes_thrown'] = bp.total_strikes_thrown
            row[f'{prefix}td'] = bp.td
            row[f'{prefix}td_attempts'] = bp.td_attempts
            row[f'{prefix}sub_attempts'] = bp.sub_attempts
            row[f'{prefix}reversals'] = bp.reversals
            row[f'{prefix}rev'] = bp.reversals  # alias expected by performance_score
            row[f'{prefix}control_time_sec'] = bp.control_time_sec
            row[f'{prefix}ctrl'] = bp.control_time_sec  # alias expected by performance_score
            row[f'{prefix}head_ss'] = bp.head_ss
            row[f'{prefix}body_ss'] = bp.body_ss
            row[f'{prefix}leg_ss'] = bp.leg_ss
            row[f'{prefix}distance_ss'] = bp.distance_ss
            row[f'{prefix}clinch_ss'] = bp.clinch_ss
            row[f'{prefix}ground_ss'] = bp.ground_ss
            row[f'{prefix}strike_accuracy'] = float(bp.strike_accuracy) if bp.strike_accuracy is not None else None
            row[f'{prefix}sig_strike_percent'] = (
                float(bp.strike_accuracy) if bp.strike_accuracy is not None else None
            )  # alias expected by performance_score

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
