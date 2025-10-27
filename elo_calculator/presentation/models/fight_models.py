from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from elo_calculator.application.services.fight_scrape_service import FightScrapeResult


class FightScrapeResultResponse(BaseModel):
    event_id: UUID
    fight_links_processed: int
    bouts_created: int
    participants_created: int
    fighters_updated: int

    @staticmethod
    def from_service(r: FightScrapeResult) -> FightScrapeResultResponse:
        return FightScrapeResultResponse(
            event_id=r.event_id,
            fight_links_processed=r.fight_links_processed,
            bouts_created=r.bouts_created,
            participants_created=r.participants_created,
            fighters_updated=r.fighters_updated,
        )


class FightScrapeListResponse(BaseModel):
    results: list[FightScrapeResultResponse]

    @staticmethod
    def from_service_list(items: list[FightScrapeResult]) -> FightScrapeListResponse:
        return FightScrapeListResponse(results=[FightScrapeResultResponse.from_service(x) for x in items])
