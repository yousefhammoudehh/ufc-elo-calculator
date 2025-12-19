from __future__ import annotations

import asyncio
import unicodedata
from typing import Any, cast

from sqlalchemy import update as sa_update

from elo_calculator.application.base_service import BaseService
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.entities import Event, Fighter
from elo_calculator.infrastructure.external_services.scrapers import stats_search_scraper as _sss
from elo_calculator.infrastructure.external_services.scrapers.stats_search_scraper import _get_ufcstats_name_parts
from elo_calculator.infrastructure.external_services.scrapers.tapology_fighter_scraper import TapologyFighterScraper
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow

logger = get_logger()


def _stats_link_for_id(fid: str) -> str:
    return f'http://www.ufcstats.com/fighter-details/{fid}'


class MaintenanceService(BaseService):
    def __init__(self, fighter_scraper: TapologyFighterScraper | None = None) -> None:
        self._fighter_scraper = fighter_scraper or TapologyFighterScraper()

    @with_uow
    async def sync_fighter_names_and_links(self, uow: UnitOfWork, *, throttle_ms: int = 250) -> dict[str, Any]:
        """Ensure stored fighter.name matches authoritative sources, without changing stats_link.

        - For each fighter with a Tapology link: scrape the profile name; if different, update DB name.
        - stats_link is never modified by this routine; it may be used by external tools to audit names,
          but this endpoint intentionally does not rewrite or canonicalize stats links.
        Returns a summary and per-fighter name changes.
        """
        fighters: list[Fighter] = await uow.fighters.get_all()
        changed: list[dict[str, Any]] = []
        name_updates = 0

        async def process(f: Fighter) -> None:
            nonlocal name_updates
            before_name = f.name
            before_stats = f.stats_link
            updated = False

            # Prefer UFCStats name when stats_link is available
            if getattr(f, 'stats_link', None):
                try:
                    # Grab the full display name exactly as UFCStats shows it in the header
                    full = None
                    try:
                        link = cast(str, f.stats_link)
                        page = _sss.parse_with_bs(_sss.fetch_html(link))  # type: ignore[attr-defined]
                        el = page.select_one('span.b-content__title-highlight')
                        if el:
                            # Keep diacritics and punctuation; collapse internal whitespace
                            full = ' '.join(el.get_text(strip=True).split())
                    except Exception:
                        # Fallback to parts if highlight missing
                        first, last, _nick = _get_ufcstats_name_parts(cast(str, f.stats_link))
                        parts = [p for p in [first, last] if p]
                        full = ' '.join(parts) if parts else None

                    def _norm_cmp(s: str) -> str:
                        # Accent-insensitive, case-insensitive, space-collapsed comparison key
                        s2 = ' '.join((s or '').strip().split()).casefold()
                        # Strip diacritics
                        return ''.join(ch for ch in unicodedata.normalize('NFKD', s2) if not unicodedata.combining(ch))

                    if full and _norm_cmp(full) != _norm_cmp(f.name or ''):
                        cmd = (
                            sa_update(uow.fighters.table)
                            .where(uow.fighters.table.c.fighter_id == f.fighter_id)
                            .values(name=full)
                            .returning(*uow.fighters.table.columns)
                        )
                        row = (await uow.connection.execute(cmd)).first()
                        if row:
                            f = Fighter.from_dict(row._asdict())
                            name_updates += 1
                            updated = True
                except Exception as exc:
                    logger.warning('UFCStats name fetch failed for fighter_id=%s: %r', f.fighter_id, exc)
            # Fallback to Tapology if stats_link is absent or provided no name
            elif getattr(f, 'tapology_link', None):
                try:
                    prof = await asyncio.to_thread(self._fighter_scraper.get_profile, cast(str, f.tapology_link))
                    if prof and prof.name and prof.name != f.name:
                        cmd = (
                            sa_update(uow.fighters.table)
                            .where(uow.fighters.table.c.fighter_id == f.fighter_id)
                            .values(name=prof.name)
                            .returning(*uow.fighters.table.columns)
                        )
                        row = (await uow.connection.execute(cmd)).first()
                        if row:
                            f = Fighter.from_dict(row._asdict())
                            name_updates += 1
                            updated = True
                except Exception as exc:
                    logger.warning('Tapology name fetch failed for fighter_id=%s: %r', f.fighter_id, exc)

            if updated:
                changed.append(
                    {
                        'fighter_id': f.fighter_id,
                        'old_name': before_name,
                        'new_name': f.name,
                        'stats_link': before_stats,
                    }
                )

        for fx in fighters:
            await process(fx)
            if throttle_ms > 0:
                await asyncio.sleep(throttle_ms / 1000.0)

        return {'total': len(fighters), 'name_updates': name_updates, 'changed': changed}

    @with_uow
    async def seed_event_names(self, uow: UnitOfWork, *, throttle_ms: int = 250) -> dict[str, Any]:
        """Populate or refresh event.name from UFCStats event pages.

        For each event with an event_stats_link, fetch the page and read
        `span.b-content__title-highlight` exactly as displayed. Store this
        in events.name without altering other fields.
        """
        events: list[Event] = await uow.events.get_all()
        updates = 0
        changed: list[dict[str, Any]] = []

        async def process(ev: Event) -> None:
            nonlocal updates
            link = getattr(ev, 'event_stats_link', None)
            if not link:
                return
            old = getattr(ev, 'name', None)
            new_name: str | None = None
            try:
                page = _sss.parse_with_bs(_sss.fetch_html(link))  # type: ignore[attr-defined]
                el = page.select_one('span.b-content__title-highlight')
                if el and el.get_text(strip=True):
                    new_name = ' '.join(el.get_text(strip=True).split())
            except Exception as exc:
                logger.warning('Failed to fetch event name: %s (%r)', link, exc)
                return
            if new_name and new_name != old:
                cmd = (
                    sa_update(uow.events.table)
                    .where(uow.events.table.c.event_id == ev.event_id)
                    .values(name=new_name)
                    .returning(*uow.events.table.columns)
                )
                row = (await uow.connection.execute(cmd)).first()
                if row:
                    updates += 1
                    changed.append({'event_id': str(ev.event_id), 'old_name': old, 'new_name': new_name, 'link': link})

        for ev in events:
            await process(ev)
            if throttle_ms > 0:
                await asyncio.sleep(throttle_ms / 1000.0)

        return {'total': len(events), 'updated': updates, 'changed': changed}
