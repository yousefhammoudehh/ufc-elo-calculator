from typing import Protocol

from .models import ScrapedFighterProfile


class FighterScraperPort(Protocol):
    """Port for scraping a fighter's profile page.

    Implementations live under infrastructure/external_services/scrapers.
    """

    def get_profile(self, fighter_link: str) -> ScrapedFighterProfile:  # pragma: no cover - IO boundary
        """Fetch and parse a fighter profile page returning links and pre-UFC bouts.

        Expected to extract, in a single pass:
        - tapology_link (canonical URL)
        - stats_link if present on the page
        - pre_ufc_bouts collection
        """
        ...
