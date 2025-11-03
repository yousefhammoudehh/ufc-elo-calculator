from __future__ import annotations

from datetime import date
from typing import Any

from elo_calculator.presentation.models.shared import DataModel


class TopFighterResponse(DataModel):
    fighter_id: str
    name: str
    current_elo: float | None = None
    peak_elo: float | None = None


class FighterEloPoint(DataModel):
    bout_id: str
    event_id: Any | None
    event_date: date | None
    event_link: str | None
    event_stats_link: str | None
    opponent_id: str | None
    opponent_name: str | None = None
    result: str | None
    elo_before: float | None
    elo_after: float | None
    delta: float | None
    event_name: str | None = None
    rank_after: int | None = None
    is_title_fight: bool | None = None
    weight_class_code: int | None = None


class FighterEloHistoryResponse(DataModel):
    fighter_id: str
    name: str
    entry_elo: float | None = None
    current_elo: float | None = None
    peak_elo: float | None = None
    points: list[FighterEloPoint]

    @staticmethod
    def from_service(fighter: Any, points: list[Any]) -> FighterEloHistoryResponse:
        return FighterEloHistoryResponse(
            fighter_id=fighter.fighter_id,
            name=fighter.name,
            entry_elo=fighter.entry_elo,
            current_elo=fighter.current_elo,
            peak_elo=fighter.peak_elo,
            points=[FighterEloPoint(**p.__dict__) for p in points],
        )


class EloChangeItem(DataModel):
    bout_id: str
    fighter_id: str
    fighter_name: str | None = None
    opponent_id: str | None = None
    opponent_name: str | None = None
    opponent_elo_before: float | None = None
    delta: float
    elo_before: float | None = None
    elo_after: float | None = None
    outcome: str | None = None
    event_id: Any | None = None
    event_name: str | None = None
    event_date: date | None = None

    @staticmethod
    def from_service(items: list[Any]) -> list[EloChangeItem]:
        out: list[EloChangeItem] = []
        for x in items:
            out.append(
                EloChangeItem(
                    bout_id=x.bout_id,
                    fighter_id=x.fighter_id,
                    fighter_name=x.fighter_name,
                    opponent_id=x.opponent_id,
                    opponent_name=x.opponent_name,
                    opponent_elo_before=x.opponent_elo_before,
                    delta=float(x.delta),
                    elo_before=x.elo_before,
                    elo_after=x.elo_after,
                    outcome=x.outcome,
                    event_id=x.event_id,
                    event_name=x.event_name,
                    event_date=x.event_date,
                )
            )
        return out


class EloMoverItem(DataModel):
    fighter_id: str
    fighter_name: str | None = None
    delta: float
    fights: int
    avg_opponent_elo: float | None = None
    last_event_id: Any | None = None
    last_event_name: str | None = None
    last_event_date: date | None = None
    last_bout_id: str | None = None


class RandomBoutItem(DataModel):
    bout_id: str
    event_name: str | None = None
    event_date: date | None = None
    method: str | None = None
    round_num: int | None = None
    fighters: list[dict[str, Any]]

    @staticmethod
    def from_service(items: list[dict[str, Any]]) -> list[RandomBoutItem]:
        return [RandomBoutItem(**x) for x in items]


class RankingEntry(DataModel):
    fighter_id: str
    name: str | None = None
    elo: float
    rank: int
    wins: int | None = None
    losses: int | None = None
    draws: int | None = None
    fights: int | None = None
    division: int | None = None
    delta_yoy: float | None = None


class RankingSnapshot(DataModel):
    label: str
    date: date
    entries: list[RankingEntry]

    @staticmethod
    def from_service(items: list[dict[str, Any]]) -> list[RankingSnapshot]:
        out: list[RankingSnapshot] = []
        for snap in items:
            entries = [RankingEntry(**e) for e in snap.get('entries', [])]
            # We trust the service to provide a proper date; cast for typing clarity
            d = snap.get('date')
            lbl = snap.get('label')
            out.append(RankingSnapshot(label=str(lbl), date=d, entries=entries))  # type: ignore[arg-type]
        return out


class YearlyEloGainItem(DataModel):
    fighter_id: str
    name: str | None = None
    delta: float
    wins: int | None = None
    losses: int | None = None
    draws: int | None = None
    fights: int | None = None
