from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from elo_calculator.application.fight_scrape_service import FightEloTrace, FightScrapeResult


class EloTransitionResponse(BaseModel):
    R_before: float
    R_after: float
    E: float
    Y: float
    K: float


class FighterStatsResponse(BaseModel):
    kd: int
    sig_strikes: int
    sig_strikes_thrown: int
    sig_strike_percent: float
    total_strikes: int
    total_strikes_thrown: int
    strike_accuracy: float
    td: int
    td_attempts: int
    td_percent: float
    sub_attempts: int
    rev: int
    control_time_sec: int
    head_ss: int
    body_ss: int
    leg_ss: int
    distance_ss: int
    clinch_ss: int
    ground_ss: int


class FighterTraceResponse(BaseModel):
    fighter_id: str
    result: str
    stats: FighterStatsResponse
    elo: EloTransitionResponse


class FightScrapeResultResponse(BaseModel):
    event_id: UUID
    fight_links_processed: int
    bouts_created: int
    participants_created: int
    fighters_updated: int
    fights: list[FightEloTraceResponse] | None = None

    @staticmethod
    def from_service(r: FightScrapeResult) -> FightScrapeResultResponse:
        return FightScrapeResultResponse(
            event_id=r.event_id,
            fight_links_processed=r.fight_links_processed,
            bouts_created=r.bouts_created,
            participants_created=r.participants_created,
            fighters_updated=r.fighters_updated,
            fights=[FightEloTraceResponse.from_service(t) for t in r.fights],
        )


class FightScrapeListResponse(BaseModel):
    results: list[FightScrapeResultResponse]

    @staticmethod
    def from_service_list(items: list[FightScrapeResult]) -> FightScrapeListResponse:
        return FightScrapeListResponse(results=[FightScrapeResultResponse.from_service(x) for x in items])


class FightEloTraceResponse(BaseModel):
    fight_index: int
    bout_id: str
    link: str
    fighter1: FighterTraceResponse
    fighter2: FighterTraceResponse

    @staticmethod
    def from_service(t: FightEloTrace) -> FightEloTraceResponse:
        f1_stats = FighterStatsResponse(
            kd=t.fighter1_kd,
            sig_strikes=t.fighter1_sig_strikes,
            sig_strikes_thrown=t.fighter1_sig_strikes_thrown,
            sig_strike_percent=t.fighter1_sig_strike_percent,
            total_strikes=t.fighter1_total_strikes,
            total_strikes_thrown=t.fighter1_total_strikes_thrown,
            strike_accuracy=t.fighter1_strike_accuracy,
            td=t.fighter1_td,
            td_attempts=t.fighter1_td_attempts,
            td_percent=t.fighter1_td_percent,
            sub_attempts=t.fighter1_sub_attempts,
            rev=t.fighter1_rev,
            control_time_sec=t.fighter1_control_time_sec,
            head_ss=t.fighter1_head_ss,
            body_ss=t.fighter1_body_ss,
            leg_ss=t.fighter1_leg_ss,
            distance_ss=t.fighter1_distance_ss,
            clinch_ss=t.fighter1_clinch_ss,
            ground_ss=t.fighter1_ground_ss,
        )
        f2_stats = FighterStatsResponse(
            kd=t.fighter2_kd,
            sig_strikes=t.fighter2_sig_strikes,
            sig_strikes_thrown=t.fighter2_sig_strikes_thrown,
            sig_strike_percent=t.fighter2_sig_strike_percent,
            total_strikes=t.fighter2_total_strikes,
            total_strikes_thrown=t.fighter2_total_strikes_thrown,
            strike_accuracy=t.fighter2_strike_accuracy,
            td=t.fighter2_td,
            td_attempts=t.fighter2_td_attempts,
            td_percent=t.fighter2_td_percent,
            sub_attempts=t.fighter2_sub_attempts,
            rev=t.fighter2_rev,
            control_time_sec=t.fighter2_control_time_sec,
            head_ss=t.fighter2_head_ss,
            body_ss=t.fighter2_body_ss,
            leg_ss=t.fighter2_leg_ss,
            distance_ss=t.fighter2_distance_ss,
            clinch_ss=t.fighter2_clinch_ss,
            ground_ss=t.fighter2_ground_ss,
        )
        f1_elo = EloTransitionResponse(R_before=t.R1_before, R_after=t.R1_after, E=t.E1, Y=t.Y1, K=t.K1)
        f2_elo = EloTransitionResponse(R_before=t.R2_before, R_after=t.R2_after, E=t.E2, Y=t.Y2, K=t.K2)
        return FightEloTraceResponse(
            fight_index=t.fight_index,
            bout_id=t.bout_id,
            link=t.link,
            fighter1=FighterTraceResponse(
                fighter_id=t.fighter1_id, result=t.fighter1_result, stats=f1_stats, elo=f1_elo
            ),
            fighter2=FighterTraceResponse(
                fighter_id=t.fighter2_id, result=t.fighter2_result, stats=f2_stats, elo=f2_elo
            ),
        )
