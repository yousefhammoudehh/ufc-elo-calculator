from datetime import date
from typing import Any
from uuid import UUID

from elo_calculator.application.base_service import BaseService
from elo_calculator.application import elo_calculator as ec
from elo_calculator.application.performance_score import compute_ps_from_row
from elo_calculator.domain.entities import Bout
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.errors.app_exceptions import DataNotFoundException
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow
from elo_calculator.presentation.models.bout_models import BoutCalcSide, BoutDetailsResponse
from elo_calculator.presentation.models.fighter_models import FighterResponse


class BoutService(BaseService):
    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: UUID) -> Bout:
        bout = await uow.bouts.get_by_id(bout_id)
        if not bout:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return bout

    @with_uow
    async def get_by_bout_id(self, uow: UnitOfWork, bout_id: str) -> Bout | None:
        return await uow.bouts.get_by_bout_id(bout_id)

    @with_uow
    async def get_all(self, uow: UnitOfWork) -> list[Bout]:
        return await uow.bouts.get_all()

    @with_uow
    async def create(self, uow: UnitOfWork, bout: Bout) -> Bout:
        return await uow.bouts.add(bout)

    @with_uow
    async def update(self, uow: UnitOfWork, bout_id: UUID, data: dict[str, Any]) -> Bout:
        existing = await uow.bouts.get_by_id(bout_id)
        if not existing:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return await uow.bouts.update(bout_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, bout_id: UUID) -> Bout:
        existing = await uow.bouts.get_by_id(bout_id)
        if not existing:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return await uow.bouts.delete(bout_id)

    # --- Helpers to keep methods small ---
    @staticmethod
    def _res_letter(outcome: FightOutcome | None) -> str:
        if outcome == FightOutcome.WIN:
            return 'W'
        if outcome == FightOutcome.LOSS:
            return 'L'
        if outcome == FightOutcome.DRAW:
            return 'D'
        if outcome == FightOutcome.NO_CONTEST:
            return 'NC'
        return ''

    def _build_row_for_bout(self, bout: Bout, p1: Any, p2: Any) -> dict[str, Any]:
        row: dict[str, Any] = {
            'method': bout.method or '',
            'time_format': bout.time_format or '',
            'is_title_fight': bool(bout.is_title_fight),
            'round_num': bout.round_num,
            'time_sec': bout.time_sec,
            'fighter1_result': self._res_letter(p1.outcome),
            'fighter2_result': self._res_letter(p2.outcome),
        }

        def put_side(prefix: str, bp: Any) -> None:
            row[f'{prefix}kd'] = bp.kd
            row[f'{prefix}sig_strikes'] = bp.sig_strikes
            row[f'{prefix}sig_strikes_thrown'] = bp.sig_strikes_thrown
            row[f'{prefix}total_strikes'] = bp.total_strikes
            row[f'{prefix}total_strikes_thrown'] = bp.total_strikes_thrown
            row[f'{prefix}td'] = bp.td
            row[f'{prefix}td_attempts'] = bp.td_attempts
            row[f'{prefix}sub_attempts'] = bp.sub_attempts
            row[f'{prefix}reversals'] = bp.reversals
            row[f'{prefix}rev'] = bp.reversals
            row[f'{prefix}control_time_sec'] = bp.control_time_sec
            row[f'{prefix}ctrl'] = bp.control_time_sec
            row[f'{prefix}head_ss'] = bp.head_ss
            row[f'{prefix}body_ss'] = bp.body_ss
            row[f'{prefix}leg_ss'] = bp.leg_ss
            row[f'{prefix}distance_ss'] = bp.distance_ss
            row[f'{prefix}clinch_ss'] = bp.clinch_ss
            row[f'{prefix}ground_ss'] = bp.ground_ss
            row[f'{prefix}strike_accuracy'] = float(bp.strike_accuracy) if bp.strike_accuracy is not None else None
            row[f'{prefix}sig_strike_percent'] = float(bp.strike_accuracy) if bp.strike_accuracy is not None else None

        put_side('fighter1_', p1)
        put_side('fighter2_', p2)
        return row

    @staticmethod
    async def _prior_title_fights(
        uow: UnitOfWork, fighter_id: str, before_event_date: date
    ) -> list[tuple[date, FightOutcome | None]]:
        """Return list of (event_date, outcome) for prior title fights strictly before `before_event_date`.

        Sorted by event_date descending.
        """
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
        """Heuristic: champion at walkout if most recent decisive prior title fight is a WIN.

        We ignore DRAW/NC as non-decisive and look further back for a decisive result.
        """
        for _d, outcome in priors_desc:
            if outcome == FightOutcome.WIN:
                return True
            if outcome == FightOutcome.LOSS:
                return False
            # DRAW/NC: continue scanning further back
        return False

    @staticmethod
    def _unification_winner_is_undisputed(
        winner_priors_desc: list[tuple[date, FightOutcome | None]],
        loser_priors_desc: list[tuple[date, FightOutcome | None]],
    ) -> bool:
        """Approximate which champion is undisputed in a unification.

        Heuristic: the fighter with more prior title-fight wins is treated as undisputed.
        If tied, the fighter whose first title-fight win is earlier (longer tenure) is treated as undisputed.
        If still tied, default to winner being undisputed.
        """

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
            return fw < fl  # earlier first win -> longer tenure
        # fallback
        return True

    @staticmethod
    async def _compute_prior_stats(uow: UnitOfWork, evt: Any, fid: str) -> tuple[int, int]:
        if not evt:
            return 0, 10_000
        participations = await uow.bout_participants.get_by_fighter_id(fid)
        dates: list[Any] = []
        for bp in participations:
            b = await uow.bouts.get_by_bout_id(bp.bout_id)
            if not b or not b.event_id:
                continue
            ev = await uow.events.get_by_event_id(b.event_id)
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
            delta_days = 10_000
        return count, int(delta_days)

    @staticmethod
    def _build_calc_side(
        fighter: Any,
        participant: Any,
        elo_before: float,
        eyk: tuple[float, float, float],
        elo_after: float,
        ufc_fights_before: int,
        days_since_last_fight: int,
        k_details: dict[str, Any] | None = None,
    ) -> BoutCalcSide:
        e_val, y_val, k_val = eyk
        return BoutCalcSide(
            fighter=FighterResponse.from_entity(fighter),
            outcome=participant.outcome,
            kd=participant.kd,
            sig_strikes=participant.sig_strikes,
            sig_strikes_thrown=participant.sig_strikes_thrown,
            total_strikes=participant.total_strikes,
            total_strikes_thrown=participant.total_strikes_thrown,
            td=participant.td,
            td_attempts=participant.td_attempts,
            sub_attempts=participant.sub_attempts,
            reversals=participant.reversals,
            control_time_sec=participant.control_time_sec,
            head_ss=participant.head_ss,
            body_ss=participant.body_ss,
            leg_ss=participant.leg_ss,
            distance_ss=participant.distance_ss,
            clinch_ss=participant.clinch_ss,
            ground_ss=participant.ground_ss,
            strike_accuracy=float(participant.strike_accuracy) if participant.strike_accuracy is not None else None,
            elo_before=elo_before,
            elo_after=elo_after,
            elo_delta=elo_after - elo_before,
            E=e_val,
            Y=y_val,
            K=k_val,
            ufc_fights_before=ufc_fights_before,
            days_since_last_fight=days_since_last_fight,
            k_breakdown=k_details,
        )

    @with_uow
    async def get_details_by_bout_id(self, uow: UnitOfWork, bout_id: str) -> BoutDetailsResponse | None:  # noqa: PLR0915
        bout = await uow.bouts.get_by_bout_id(bout_id)
        if not bout:
            return None

        evt = await uow.events.get_by_event_id(bout.event_id) if bout.event_id else None
        participants = await uow.bout_participants.get_by_bout_id(bout.bout_id)
        expected_participants = 2
        if len(participants) < expected_participants:
            return None
        p1, p2 = participants[0], participants[1]

        f1 = await uow.fighters.get_by_fighter_id(p1.fighter_id)
        f2 = await uow.fighters.get_by_fighter_id(p2.fighter_id)
        if not f1 or not f2:
            return None

        row = self._build_row_for_bout(bout, p1, p2)

        # Infer title_bout_type per walkout status criteria
        if bool(bout.is_title_fight) and evt and getattr(evt, 'event_date', None):
            # identify winner and loser
            winner_pid: str | None = None
            loser_pid: str | None = None
            if p1.outcome == FightOutcome.WIN and p2.outcome != FightOutcome.WIN:
                winner_pid, loser_pid = p1.fighter_id, p2.fighter_id
            elif p2.outcome == FightOutcome.WIN and p1.outcome != FightOutcome.WIN:
                winner_pid, loser_pid = p2.fighter_id, p1.fighter_id
            if winner_pid and loser_pid:
                priors_w = await self._prior_title_fights(uow, winner_pid, evt.event_date)
                priors_l = await self._prior_title_fights(uow, loser_pid, evt.event_date)
                entered_w_champ = self._entered_as_champion(priors_w)
                entered_l_champ = self._entered_as_champion(priors_l)
                if entered_w_champ and entered_l_champ:
                    # Unification heuristic
                    winner_is_undisputed = self._unification_winner_is_undisputed(priors_w, priors_l)
                    row['title_bout_type'] = 'defense' if winner_is_undisputed else 'capture'
                elif entered_w_champ:
                    row['title_bout_type'] = 'defense'
                else:
                    # Includes vacant or challenger vs champ where challenger wins
                    row['title_bout_type'] = 'capture'

        c1 = (
            p1.ufc_fights_before
            if p1.ufc_fights_before is not None
            else (await self._compute_prior_stats(uow, evt, p1.fighter_id))[0]
        )
        c2 = (
            p2.ufc_fights_before
            if p2.ufc_fights_before is not None
            else (await self._compute_prior_stats(uow, evt, p2.fighter_id))[0]
        )
        d1 = (
            p1.days_since_last_fight
            if p1.days_since_last_fight is not None
            else (await self._compute_prior_stats(uow, evt, p1.fighter_id))[1]
        )
        d2 = (
            p2.days_since_last_fight
            if p2.days_since_last_fight is not None
            else (await self._compute_prior_stats(uow, evt, p2.fighter_id))[1]
        )

        r1_before = float(p1.elo_before if p1.elo_before is not None else (f1.current_elo or f1.entry_elo or 1500.0))
        r2_before = float(p2.elo_before if p2.elo_before is not None else (f2.current_elo or f2.entry_elo or 1500.0))

        outputs = ec.compute_elo_from_row(
            row,
            extras={
                'R1_before': r1_before,
                'R2_before': r2_before,
                'ufc_fights_before_1': c1,
                'ufc_fights_before_2': c2,
                'days_since_last_fight_1': d1,
                'days_since_last_fight_2': d2,
            },
        )

        ps = compute_ps_from_row(row)
        ps1 = float(ps.get('PS1', 0.0))
        ps2 = float(ps.get('PS2', 0.0))
        shares = ps.get('shares', {})

        rounds_scheduled = ec._rounds_scheduled_from_row(row)
        # Detailed schedule for K breakdown
        _n, per_round_seconds, total_scheduled_seconds = ec._parse_schedule_from_time_format(bout.time_format or '')

        kb1 = ec.k_breakdown(
            ec.KContext(
                rounds_scheduled=rounds_scheduled,
                method=bout.method or '',
                ufc_fights_before=int(c1),
                days_since_last_fight=int(d1),
                round_num=bout.round_num,
                time_sec=bout.time_sec,
                total_scheduled_seconds=total_scheduled_seconds,
                per_round_seconds=per_round_seconds,
            )
        )
        kb2 = ec.k_breakdown(
            ec.KContext(
                rounds_scheduled=rounds_scheduled,
                method=bout.method or '',
                ufc_fights_before=int(c2),
                days_since_last_fight=int(d2),
                round_num=bout.round_num,
                time_sec=bout.time_sec,
                total_scheduled_seconds=total_scheduled_seconds,
                per_round_seconds=per_round_seconds,
            )
        )

        side1 = self._build_calc_side(
            f1, p1, r1_before, (outputs.E1, outputs.Y1, outputs.K1), outputs.R1_after, c1, d1, kb1
        )
        side1.ps = ps1
        side1.k_breakdown = side1.k_breakdown or {}
        side1.k_breakdown['PS_SHARES'] = shares  # attach for UI
        side2 = self._build_calc_side(
            f2, p2, r2_before, (outputs.E2, outputs.Y2, outputs.K2), outputs.R2_after, c2, d2, kb2
        )
        side2.ps = ps2
        side2.k_breakdown = side2.k_breakdown or {}
        side2.k_breakdown['PS_SHARES'] = shares

        # Build human-readable explanations
        def _fmt_pct(x: float | None) -> str:
            return f'{100.0 * x:.1f}%' if x is not None else '—'

        # PS explanation (shared)
        sigp1 = None if p1.strike_accuracy is None else float(p1.strike_accuracy)
        sigp2 = None if p2.strike_accuracy is None else float(p2.strike_accuracy)
        ctrl1 = p1.control_time_sec or 0
        ctrl2 = p2.control_time_sec or 0
        td1 = p1.td or 0
        tda1 = p1.td_attempts or 0
        td2 = p2.td or 0
        tda2 = p2.td_attempts or 0
        clinch1 = p1.clinch_ss or 0
        clinch2 = p2.clinch_ss or 0
        _nseg, per_round_seconds, total_sched_seconds = ec._parse_schedule_from_time_format(bout.time_format or '')
        sched_desc = (
            f'{rounds_scheduled} segment(s), total {total_sched_seconds}s'
            if total_sched_seconds is not None
            else f'{rounds_scheduled} segment(s), unspecified duration'
        )
        ps_expl = (
            'Performance Score (PS v2.0) blends striking efficiency, control (incl. clinch), and takedowns with a '
            'continuous duration gate. Sig% is recomputed from landed/thrown; control includes clinch and ground; '
            'takedown impact gates continuously by attempts and success. Duration is normalized to the scheduled '
            f'time ({sched_desc}). For this bout: Fighter 1 Sig%={_fmt_pct(sigp1)}, control={ctrl1}s, TD {td1}/{tda1}, '
            f'clinch SS={clinch1}; Fighter 2 Sig%={_fmt_pct(sigp2)}, control={ctrl2}s, TD {td2}/{tda2}, clinch SS={clinch2}. '
            f'Computed PS: F1={ps1:.3f}, F2={ps2:.3f}. Weights remain unchanged vs v1.'
        )

        # K explanations per side
        def _k_explanation(kb: dict[str, Any], final_k: float) -> str:
            eps = 1e-3
            base = kb.get('base_K0')
            mult_r = kb.get('mult_rounds')
            mclass = kb.get('method_class')
            mult_m = kb.get('mult_method')
            mult_e = kb.get('mult_experience')
            mult_rn = kb.get('mult_recency')
            mult_f = kb.get('mult_finish')
            total_sched = kb.get('schedule_total_seconds')
            rs = kb.get('rounds_scheduled')
            parts: list[str] = []
            parts.append(
                f'Base K0={base}. Rounds multiplier={mult_r:.3f} based on schedule '
                f'({rs} segments, total {total_sched}s) vs 3x5 baseline.'
            )
            parts.append(f'Method class={mclass} → method multiplier={mult_m:.3f}.')
            parts.append(f'Experience multiplier={mult_e:.3f} from UFC fights before={kb.get("ufc_fights_before")}.')
            parts.append(
                f'Recency multiplier={mult_rn:.3f} from days since last fight={kb.get("days_since_last_fight")}.'
            )
            if isinstance(mult_f, float) and mult_f > 1.0 + eps:
                finish_u = kb.get('finish_u')
                parts.append(
                    f'Finish-time boost={mult_f:.3f} (earlier finish increases K; normalized time u={finish_u:.3f} if available).'
                )
            # Title stakes + safety cap notes inferred from final K vs pre-title K
            pre_title_k = kb.get('K_final')
            try:
                pre_title_k_f = float(pre_title_k) if pre_title_k is not None else None
            except Exception:
                pre_title_k_f = None
            cap_val = 1.5 * ec.K0
            if pre_title_k_f is not None:
                ratio = final_k / pre_title_k_f if pre_title_k_f > 0 else 1.0
                if final_k >= cap_val - 1e-6:
                    parts.append(f'Safety cap applied: final K capped at 1.5xK0 = {cap_val:.2f}.')
                elif ratio > 1.0 + eps:
                    parts.append(
                        f'Title stakes multiplier applied after base factors: x{ratio:.3f} (winner-centric, bounded).'
                    )
            parts.append(f'Final K used={final_k:.3f}.')
            return ' '.join(parts)

        side1.k_explanation = _k_explanation(kb1, side1.K or 0.0)
        side2.k_explanation = _k_explanation(kb2, side2.K or 0.0)

        return BoutDetailsResponse(
            bout_id=bout.bout_id,
            event_id=bout.event_id,
            event_date=str(evt.event_date) if evt and evt.event_date else None,
            is_title_fight=bout.is_title_fight,
            method=bout.method,
            round_num=bout.round_num,
            time_sec=bout.time_sec,
            time_format=bout.time_format,
            rounds_scheduled=rounds_scheduled,
            inputs={'R1_before': r1_before, 'R2_before': r2_before},
            side1=side1,
            side2=side2,
            ps_explanation=ps_expl,
        )
