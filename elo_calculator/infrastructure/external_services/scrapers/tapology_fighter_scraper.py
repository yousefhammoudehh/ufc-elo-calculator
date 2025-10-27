# isort: skip_file
from __future__ import annotations


import re
from html import unescape
from collections.abc import Iterable

from bs4 import BeautifulSoup
from bs4.element import Tag

from elo_calculator.domain.client.fighter_scraper_port import FighterScraperPort
from elo_calculator.domain.client.models import ScrapedFighterProfile
from elo_calculator.domain.client.models import ScrapedPreUfcBout
from elo_calculator.domain.client.models import ScrapedPromotion
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.configs.log import get_logger
from elo_calculator.infrastructure.external_services.scrapers.base_scraper import fetch_html
from elo_calculator.infrastructure.external_services.scrapers.base_scraper import parse_with_bs


logger = get_logger()


class TapologyFighterScraper(FighterScraperPort):
    _BASE_URL = 'https://www.tapology.com'
    _UFC_PROMO_SLUG = '/fightcenter/promotions/1-'

    def get_profile(self, fighter_link: str) -> ScrapedFighterProfile:  # pragma: no cover - IO boundary
        html = fetch_html(fighter_link)
        soup = parse_with_bs(html)

        tapology_link = fighter_link
        name = self._extract_name(soup)
        stats_link = self._extract_stats_link(soup)
        pre_ufc_bouts = self._extract_pre_ufc_bouts(soup)

        # Basic summary log only
        logger.info(
            f'Scraped fighter profile: link={tapology_link} name={name} stats_link={stats_link} pre_ufc_count={len(pre_ufc_bouts)}'
        )

        return ScrapedFighterProfile(
            tapology_link=tapology_link, name=name, stats_link=stats_link, pre_ufc_bouts=pre_ufc_bouts
        )

    @staticmethod
    def _extract_name(soup: BeautifulSoup) -> str | None:
        title_el = soup.find('title')
        if not title_el or not title_el.text:
            return None
        left = title_el.text.split('|', 1)[0].strip()
        # Remove nickname in parentheses if present
        left = re.sub(r'\((?:\"|\u201c|\u201d)?[^\)]+(?:\"|\u201c|\u201d)?\)\s*$', '', left).strip()
        return unescape(left) or None

    def _extract_stats_link(self, soup: BeautifulSoup) -> str | None:
        # Tapology has an explicit onclick for UFCStats external link on some pages
        a = soup.find('a', onclick="gtag('event', 'external_click_fighter_ufc');")
        raw = a.get('href') if a else None
        return raw if isinstance(raw, str) else None

    def _extract_pre_ufc_bouts(self, soup: BeautifulSoup) -> list[ScrapedPreUfcBout]:
        """Extract pre-UFC bouts using the caller-provided scraping logic AS IS, with entity conversion.

        Scraping logic mirrors the provided function:
        - Find header h3#fighterRecordHeader, then its next sibling container
        - Within, select bout nodes by data-bout-id, exclude amateur, and ensure the pre-fight record span exists
        - Skip rows where status indicates "Cancelled Bout"
        - Extract result via two-level shallow find
        - Determine promotion via either the "mt-5 bg-white" box or a following meta div
        - Compute last UFC index by exact promotion href equality and slice accordingly
        """
        sibling = self._record_container(soup)
        if not sibling:
            logger.info('Pre-UFC: record container not found')
            return []

        fights = self._select_fights(sibling)
        logger.debug(f'Pre-UFC: fights selected={len(fights)}')

        pre: list[ScrapedPreUfcBout] = []
        promotions_raw: list[str] = []

        for fight in fights:
            status_text, status_node = self._status_text(fight)
            if not (status_text and status_text != 'Cancelled Bout'):
                continue

            result_text = self._result_text_shallow(fight)
            promotion_link_str, promotion_name_str = self._promotion_link_and_name(fight, status_node)

            # Convert to our entities
            outcome = self._to_outcome(result_text)
            promotion = ScrapedPromotion(name=promotion_name_str or 'Unknown', link=promotion_link_str or None)

            pre.append(ScrapedPreUfcBout(result=outcome, promotion=promotion))
            promotions_raw.append(promotion_link_str or (promotion_name_str or ''))

            # Keep logs minimal; no per-row prints

        # Identify index of last UFC bout; if never UFC, return all bouts (exact match as provided)
        last_ufc_index = self._last_ufc_index_exact(promotions_raw)
        logger.info(
            f'Pre-UFC: last_ufc_index={last_ufc_index} total_parsed={len(pre)} returned_count={len(pre[last_ufc_index + 1 :] if last_ufc_index != -1 else pre)}'
        )
        return pre[last_ufc_index + 1 :] if last_ufc_index != -1 else pre

    # --- helpers to reduce complexity without altering scraping logic ---
    @staticmethod
    def _record_container(soup: BeautifulSoup) -> Tag | None:
        header = soup.find('h3', id='fighterRecordHeader')
        if not header:
            return None
        return header.find_next_sibling()

    @staticmethod
    def _select_fights(container: Tag | BeautifulSoup) -> list[Tag]:
        return container.find_all(
            lambda tag: tag.has_attr('data-bout-id')
            and (not tag.has_attr('data-division') or tag.get('data-division') != 'amateur')
            and tag.find('span', title='Fighter Record Before Fight') is not None
        )

    @staticmethod
    def _status_text(fight: Tag) -> tuple[str, Tag | None]:
        status = fight.find('div', class_='div text-neutral-700 text-xs font-bold md:leading-[16px]')
        status_text = status.text.strip() if status and status.text else ''
        return status_text, status

    @staticmethod
    def _result_text_shallow(fight: Tag) -> str:
        try:
            result_el_lvl1 = fight.find(recursive=False)
            result_el_lvl2 = result_el_lvl1.find(recursive=False) if result_el_lvl1 else None
            return result_el_lvl2.get_text(strip=True) if result_el_lvl2 else ''
        except Exception:
            return ''

    @staticmethod
    def _promotion_link_and_name(fight: Tag, status_node: Tag | None) -> tuple[str, str | None]:
        promotion_link_str = ''
        promotion_name_str: str | None = None

        promotion_box = fight.find('div', class_='div mt-5 bg-white p-4 rounded')
        if promotion_box:
            promotion_link = promotion_box.find('a')
            href = promotion_link.get('href') if promotion_link else None
            if isinstance(href, str):
                promotion_link_str = href
                promotion_name_str = promotion_link.get_text(strip=True) if promotion_link else None
        else:
            meta = (
                status_node.find_next_sibling('div', class_='div text-xs10 text-neutral-600 leading-none')
                if status_node
                else None
            )
            if meta is not None:
                promotion_link = meta.find('a')
                if promotion_link and promotion_link.get('href'):
                    href = promotion_link.get('href')
                    if isinstance(href, str):
                        promotion_link_str = href
                        promotion_name_str = promotion_link.get_text(strip=True)
                else:
                    promotion_name = meta.get_text(' ', strip=True)
                    promotion_name_str = (promotion_name or '').strip('\n')

        # Normalize to absolute Tapology link if relative
        promotion_link_abs = TapologyFighterScraper._normalize_link(promotion_link_str)
        return promotion_link_abs or '', promotion_name_str

    @staticmethod
    def _normalize_link(href: str | None) -> str | None:
        if not href:
            return None
        if href.startswith('http://') or href.startswith('https://'):
            return href
        if href.startswith('/'):
            return f'https://www.tapology.com{href}'
        return href

    @staticmethod
    def _last_ufc_index_exact(promotions_raw: Iterable[str]) -> int:
        """Return the last index where the UFC promotion appears in the list.

        Handles both relative and absolute Tapology links. We purposefully pick the
        last index so that slicing [last+1:] yields bouts strictly BEFORE the first UFC bout
        in chronological order when the page lists most-recent fights first (Tapology default).
        """
        last_ufc_index = -1
        needle = '/fightcenter/promotions/1-ultimate-fighting-championship-ufc'
        for i, promo in enumerate(promotions_raw):
            if not promo:
                continue
            # Accept relative or absolute; match by substring to be resilient
            if needle in str(promo):
                last_ufc_index = i
        return last_ufc_index

    @staticmethod
    def _to_outcome(text: str) -> FightOutcome | None:
        t = (text or '').strip().lower()
        if not t:
            return None
        # Handle common full words and Tapology single-letter abbreviations
        if t.startswith('win') or t == 'w':
            return FightOutcome.WIN
        if t.startswith('loss') or t.startswith('defeat') or t == 'l':
            return FightOutcome.LOSS
        if 'draw' in t or t == 'd':
            return FightOutcome.DRAW
        if 'no contest' in t or 'no-contest' in t or t in {'nc', 'n/c'}:
            return FightOutcome.NO_CONTEST
        return None

    # Note: kept only _last_ufc_index_exact for clarity; this looser variant was unused and removed
