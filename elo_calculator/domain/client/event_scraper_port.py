from typing import Protocol

from .models import ScrapedEvent, ScrapedFighter


class EventScraperPort(Protocol):
    """Port describing what the application needs from an event scraper.

    Implementations live in infrastructure/external_services/scrapers and adapt specific sites.
    """

    def get_event_info(self, event_link: str) -> ScrapedEvent:  # pragma: no cover - IO boundary
        """Fetch and parse basic event details from a public page."""
        ...

    def get_event_fighters(self, event_link: str, limit: int | None = None) -> list[ScrapedFighter]:  # pragma: no cover
        """Return fighter links (and optionally names) appearing on a given event page."""
        ...
