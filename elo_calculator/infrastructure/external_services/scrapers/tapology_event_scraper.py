# isort: skip_file
from __future__ import annotations

import logging

from typing import Any

from elo_calculator.domain.client.event_scraper_port import EventScraperPort
from elo_calculator.domain.client.models import ScrapedEvent, ScrapedFighter
from elo_calculator.infrastructure.external_services.scrapers.base_scraper import fetch_html, parse_with_bs


logger = logging.getLogger(__name__)


class TapologyEventScraper(EventScraperPort):
    """Scraper for Tapology event pages.

    Strategy:
    - Fetch event page HTML using our shared HTTP client and polite headers.
    - Select all anchors that link to fighter profiles (href starts with "/fightcenter/fighters/") from both left/right
        columns; avoid brittle container class matching.
    - Build absolute links and collect unique fighters in document order.
    - Return minimal VOs (ScrapedFighter) for downstream ingestion.
    """

    _BASE_URL = 'https://www.tapology.com'
    _MIN_FIGHTER_LINKS_IN_CONTAINER = 2

    def get_event_fighters(self, event_link: str, limit: int | None = None) -> list[ScrapedFighter]:
        logger.info('TapologyEventScraper.get_event_fighters: start link=%s', event_link)
        html = fetch_html(event_link, referer=None)
        soup = parse_with_bs(html)

        # Robust selector: collect both red/blue fighter anchors across the page.
        # Tapology consistently uses /fightcenter/fighters/<slug> for fighter profile links.
        anchors = soup.select(
            'span.left a[href^="/fightcenter/fighters/"], span.right a[href^="/fightcenter/fighters/"]'
        )

        fighters: list[ScrapedFighter] = []
        seen: set[str] = set()

        for a in anchors:
            href = a.get('href')
            if not isinstance(href, str):
                continue
            abs_link = f'https://www.tapology.com{href}' if href.startswith('/') else href
            if abs_link in seen:
                continue
            seen.add(abs_link)
            fighters.append(ScrapedFighter(a.get_text(strip=True), abs_link))
            if limit is not None and len(fighters) >= limit:
                logger.info(
                    'Parsed fighters from event page (truncated by limit): count=%d unique=%d link=%s',
                    len(fighters),
                    len(seen),
                    event_link,
                )
                return fighters

        logger.info('Parsed fighters from event page: count=%d unique=%d link=%s', len(fighters), len(seen), event_link)
        return fighters

    def get_event_info(self, event_link: str) -> Any:
        """Minimal info: currently returns title only; extend as needed."""
        html = fetch_html(event_link, referer=None)
        soup = parse_with_bs(html)
        title_el = soup.find('title')
        title = title_el.get_text(strip=True) if title_el else None
        return ScrapedEvent(event_link=event_link, title=title)
