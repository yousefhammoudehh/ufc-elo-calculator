from __future__ import annotations

from uuid import UUID

from elo_calculator.application.event_elo_service import EventEloResult
from elo_calculator.presentation.models.shared import DataModel


class EventEloSeedResult(DataModel):
    event_id: UUID
    bouts_processed: int
    participants_updated: int
    fighters_updated: int


class EventEloSeedListResponse(DataModel):
    results: list[EventEloSeedResult]
    events_count: int
    bouts_processed_total: int
    participants_updated_total: int
    fighters_updated_total: int

    @staticmethod
    def from_service_results(results: list[EventEloResult]) -> EventEloSeedListResponse:
        items = [
            EventEloSeedResult(
                event_id=r.event_id,
                bouts_processed=r.bouts_processed,
                participants_updated=r.participants_updated,
                fighters_updated=r.fighters_updated,
            )
            for r in results
        ]
        return EventEloSeedListResponse(
            results=items,
            events_count=len(items),
            bouts_processed_total=sum(r.bouts_processed for r in results),
            participants_updated_total=sum(r.participants_updated for r in results),
            fighters_updated_total=sum(r.fighters_updated for r in results),
        )
