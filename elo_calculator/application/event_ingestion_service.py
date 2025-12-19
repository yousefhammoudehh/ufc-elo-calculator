from __future__ import annotations

import asyncio
import hashlib
import secrets
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import update as sa_update

from elo_calculator.application.base_service import BaseService
from elo_calculator.application.elo_calculator import PromotionsRepoProtocol, compute_starting_elo
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.client.event_scraper_port import EventScraperPort
from elo_calculator.domain.client.fighter_scraper_port import FighterScraperPort
from elo_calculator.domain.client.models import ScrapedFighterProfile, ScrapedPreUfcBout, ScrapedPromotion
from elo_calculator.domain.entities import Event, Fighter, PreUfcBout, Promotion
from elo_calculator.domain.shared.enumerations import FightOutcome
from elo_calculator.infrastructure.external_services.scrapers.stats_search_scraper import StatsSearchScraper
from elo_calculator.infrastructure.external_services.scrapers.tapology_event_scraper import TapologyEventScraper
from elo_calculator.infrastructure.external_services.scrapers.tapology_fighter_scraper import TapologyFighterScraper
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow

logger = get_logger()


def _stable_id_from_link(link: str, length: int = 16) -> str:
    """Derive a stable short ID from a link as a fallback.

    Uses BLAKE2b for fast, deterministic hashing. The returned hex string length
    will be `length` characters to match legacy expectations (defaults to 16).
    """
    # Each byte produces two hex chars; target digest size accordingly
    digest_bytes = max(1, length // 2)
    h = hashlib.blake2b(link.encode(), digest_size=digest_bytes).hexdigest()
    # If caller requests an odd number of hex chars, trim accordingly
    return h[:length]


def _fighter_id_from_stats_link(link: str | None) -> str | None:
    """Extract UFCStats fighter id from a fighter-details URL.

    Example: http://www.ufcstats.com/fighter-details/63b65af1c5cb02cb -> 63b65af1c5cb02cb
    """
    if not link:
        return None
    try:
        after = link.split('/fighter-details/', 1)[1]
        token = after.split('/', 1)[0]
        token = token.split('?', 1)[0]
        return token or None
    except Exception:
        return None


def _normalize_tapology_link(link: str | None) -> str | None:
    """Ensure Tapology links are absolute (https://www.tapology.com/...)."""
    if not link:
        return None
    if link.startswith('http://') or link.startswith('https://'):
        return link
    if link.startswith('/'):
        return f'https://www.tapology.com{link}'
    return link


def _canonicalize_tapology_link(link: str | None) -> str | None:
    """Normalize link for consistent storage and comparisons.

    Rules:
      - Ensure absolute and prefer https scheme
      - Remove a single trailing slash if present
    """
    norm = _normalize_tapology_link(link)
    if not norm:
        return None
    if norm.startswith('http://'):
        norm = 'https://' + norm[len('http://') :]
    # strip exactly one trailing slash to avoid // issues
    if norm.endswith('/'):
        norm = norm[:-1]
    return norm


def _tapology_link_variants(link: str) -> list[str]:
    """Generate a few equivalent variants to tolerate historical storage diffs.

    Produces combinations of:
      - https vs http
      - with/without trailing slash
    """
    variants: set[str] = set()
    base = _normalize_tapology_link(link) or link
    schemes = ['https://', 'http://']
    for scheme in schemes:
        if base.startswith('http://'):
            tail = base[len('http://') :]
        elif base.startswith('https://'):
            tail = base[len('https://') :]
        else:
            # base is already absolute by _normalize; fallback
            tail = base
        full = scheme + tail
        # add with and without trailing slash
        variants.add(full.rstrip('/'))
        variants.add(full.rstrip('/') + '/')
    return list(variants)


class EventIngestionService(BaseService):
    def __init__(
        self,
        event_scraper: EventScraperPort | None = None,
        fighter_scraper: FighterScraperPort | None = None,
        stats_search: StatsSearchScraper | None = None,
        http_delay_min_seconds: float = 3.0,
        http_delay_max_seconds: float = 7.0,
    ) -> None:
        self._event_scraper = event_scraper or TapologyEventScraper()
        self._fighter_scraper = fighter_scraper or TapologyFighterScraper()
        self._stats_search = stats_search or StatsSearchScraper()

        # Randomized per-call delay to avoid rate limits and clumping (no global monotonic pacing)
        self._http_delay_min_seconds = max(0.0, float(http_delay_min_seconds))
        self._http_delay_max_seconds = max(self._http_delay_min_seconds, float(http_delay_max_seconds))

    async def _sleep_before_http(self) -> None:
        """Sleep a random duration between [min, max] seconds before an HTTP call."""
        if self._http_delay_max_seconds <= 0:
            return
        min_ms = int(self._http_delay_min_seconds * 1000)
        max_ms = int(self._http_delay_max_seconds * 1000)
        # Defensive check: max should not be less than min (constructor clamps, but keep guard)
        if max_ms < min_ms:
            logger.error('Invalid HTTP delay configuration: max_ms (%s) < min_ms (%s); skipping sleep', max_ms, min_ms)
            return
        # secrets.randbelow is inclusive of 0 and exclusive of the upper bound
        delay_ms = min_ms if max_ms == min_ms else min_ms + secrets.randbelow((max_ms - min_ms) + 1)
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

    @with_uow
    async def ingest_events_by_ids(self, uow: UnitOfWork, event_ids: list[UUID]) -> tuple[list[Fighter], list[Event]]:
        logger.info(f'Ingesting events by IDs: count={len(event_ids)}')
        fighters_seeded: dict[str, Fighter] = {}
        selected_events: list[Event] = []

        for eid in event_ids:
            evt = await uow.events.get_by_event_id(eid)
            if not evt or not evt.event_link:
                logger.warning('Event missing or has no link; skipping: event_id=%s', eid)
                continue
            selected_events.append(evt)
            seeded_for_event = await self._ingest_single_event(uow, evt)
            for f in seeded_for_event:
                fighters_seeded[f.fighter_id] = f

        logger.info(f'Ingestion complete (IDs). Seeded fighters total: count={len(fighters_seeded)}')
        return (list(fighters_seeded.values()), selected_events)

    @with_uow
    async def ingest_events_by_links(
        self, uow: UnitOfWork, event_links: list[str]
    ) -> tuple[list[Fighter], list[Event]]:
        logger.info(f'Ingesting events by direct links: count={len(event_links)}')
        fighters_seeded: dict[str, Fighter] = {}
        selected_events: list[Event] = []

        for link in event_links:
            evt = await uow.events.get_by_event_link(link)
            if evt and evt.event_link:
                selected_events.append(evt)
                seeded_for_event = await self._ingest_single_event(uow, evt)
                for f in seeded_for_event:
                    fighters_seeded[f.fighter_id] = f
            else:
                # Event not in DB; still ingest fighters from the link without marking
                await self._sleep_before_http()
                fighters = await asyncio.to_thread(self._event_scraper.get_event_fighters, link)
                unique_links = {sf.fighter_link for sf in fighters}
                logger.info(
                    f'Event link not found in DB; ingesting fighters only: link={link} unique={len(unique_links)}'
                )
                sem = asyncio.Semaphore(3)
                new_map: dict[str, Fighter] = {}

                async def process(
                    lnk: str,
                    _sem: asyncio.Semaphore = sem,
                    _fighters_seeded: dict[str, Fighter] = fighters_seeded,
                    _new_map: dict[str, Fighter] = new_map,
                ) -> None:
                    async with _sem:
                        await self._process_fighter_link(uow, lnk, _fighters_seeded, _new_map)

                await asyncio.gather(*(process(lnk) for lnk in unique_links))

        logger.info(f'Ingestion complete (links). Seeded fighters total: count={len(fighters_seeded)}')
        return (list(fighters_seeded.values()), selected_events)

    @with_uow
    async def ingest_fighters_by_links(self, uow: UnitOfWork, fighter_links: list[str]) -> list[Fighter]:
        """Seed fighters directly from Tapology links.

        For each link: scrape profile, compute starting ELO from pre-UFC bouts, persist fighter and bouts.
        If the fighter already exists, ensure links are filled but do not recompute ELO.
        """
        sem = asyncio.Semaphore(3)
        fighters_seeded: dict[str, Fighter] = {}
        new_fighters: dict[str, Fighter] = {}

        async def process(lnk: str) -> None:
            async with sem:
                await self._process_fighter_link(uow, lnk, fighters_seeded, new_fighters)

        await asyncio.gather(*(process(lnk) for lnk in fighter_links))
        return list(fighters_seeded.values())

    @with_uow
    async def ingest_fighter_by_link(  # noqa: PLR0912
        self, uow: UnitOfWork, tapology_link: str, *, stats_link: str | None = None
    ) -> Fighter:
        """Seed a single fighter by Tapology link. Optionally pass a UFCStats link to skip searching.

        - Scrapes profile and pre-UFC bouts
        - Resolves stats_link only if not provided
        - Computes starting ELO from pre-UFC bouts
        - Persists fighter and pre-UFC bouts (idempotent; will not recompute ELO for existing fighters)
        """
        # If fighter already exists, ensure links but don't recompute ELO
        existing = await self._get_existing_by_tapology_link(uow, tapology_link)
        if existing:
            # If caller supplies a stats_link, enforce consistency of IDs; do not silently change PKs
            if stats_link:
                token = _fighter_id_from_stats_link(stats_link)
                if token and token != existing.fighter_id:
                    raise ValueError(
                        f'fighter exists with id={existing.fighter_id} but provided stats_link id={token}; '
                        'refusing to change primary key. Delete/merge the fighter first.'
                    )
                # Update stored stats_link if missing or different from provided
                if stats_link != existing.stats_link:
                    cmd = (
                        sa_update(uow.fighters.table)
                        .where(uow.fighters.table.c.fighter_id == existing.fighter_id)
                        .values(stats_link=stats_link)
                        .returning(*uow.fighters.table.columns)
                    )
                    row = (await uow.connection.execute(cmd)).first()
                    if row:
                        existing = Fighter.from_dict(row._asdict())
            return existing

        # Scrape profile
        await self._sleep_before_http()
        profile: ScrapedFighterProfile = await asyncio.to_thread(self._fighter_scraper.get_profile, tapology_link)
        pre_list = list(profile.iter_pre_ufc_bouts())

        # Use provided stats_link if given, otherwise resolve
        eff_stats = stats_link or profile.stats_link
        if not eff_stats and isinstance(self._stats_search, StatsSearchScraper):
            await self._sleep_before_http()
            eff_stats = await asyncio.to_thread(self._stats_search.get_link_from_tapology, profile.tapology_link)
        if not eff_stats and profile.name:
            await self._sleep_before_http()
            eff_stats = await asyncio.to_thread(self._stats_search.search_fighter, profile.name)

        # Guard against duplicate/misspelled names: if a fighter already exists with the same name,
        # prefer updating that record's links rather than creating a new one. If the provided stats_link
        # token conflicts with the existing fighter's id, raise a clear error.
        if profile.name:
            try:
                existing_by_name = await uow.fighters.get_by_name(profile.name)
            except Exception:
                existing_by_name = None
            if existing_by_name:
                token = _fighter_id_from_stats_link(eff_stats) if eff_stats else None
                if token and token != existing_by_name.fighter_id:
                    raise ValueError(
                        'fighter with the same name already exists in database but with a different id. '
                        f'name={profile.name!r} existing_id={existing_by_name.fighter_id} provided_stats_id={token}. '
                        'Refusing to create a duplicate or change primary key. Merge or correct the record first.'
                    )
                # Patch links on the existing fighter (no ELO recompute) and persist pre-UFC bouts
                updates: dict[str, Any] = {}
                canon_tap = _canonicalize_tapology_link(profile.tapology_link) or profile.tapology_link
                if canon_tap and (not existing_by_name.tapology_link or existing_by_name.tapology_link != canon_tap):
                    updates['tapology_link'] = canon_tap
                if eff_stats and (not existing_by_name.stats_link or existing_by_name.stats_link != eff_stats):
                    updates['stats_link'] = eff_stats
                if updates:
                    cmd = (
                        sa_update(uow.fighters.table)
                        .where(uow.fighters.table.c.fighter_id == existing_by_name.fighter_id)
                        .values(**updates)
                        .returning(*uow.fighters.table.columns)
                    )
                    row = (await uow.connection.execute(cmd)).first()
                    if row:
                        existing_by_name = Fighter.from_dict(row._asdict())
                # Attach pre-UFC bouts to the existing fighter id
                await self._persist_pre_ufc_bouts(uow, existing_by_name.fighter_id, pre_list)
                return existing_by_name

        # Compute starting ELO and persist
        promo_repo = await self._build_promotions_repo_for_bouts(uow, pre_list)
        starting_elo = compute_starting_elo(pre_list, promotions_repo=promo_repo)
        name = profile.name or _stable_id_from_link(profile.tapology_link)
        fighter = await self._ensure_fighter(
            uow,
            name=name,
            tapology_link=_canonicalize_tapology_link(profile.tapology_link) or profile.tapology_link,
            stats_link=eff_stats,
            starting_elo=starting_elo,
        )
        await self._persist_pre_ufc_bouts(uow, fighter.fighter_id, pre_list)
        return fighter

    @with_uow
    async def ingest_first_unseeded_events(
        self, uow: UnitOfWork, limit: int
    ) -> tuple[list[Fighter], list[Fighter], list[Event]]:
        """Scan events by oldest-first and ingest the first N that are not yet ingested.

        Ingestion status rule:
            - An event is considered 'ingested' if all fighters on its Tapology page already exist in our DB,
                even if some fighters have zero pre-UFC bouts (some fighters debut directly in the UFC).
        """
        logger.info(f'Scanning for first unseeded events: limit={limit}')
        page = 1
        page_size = 50
        selected_links: list[str] = []
        selected_events: list[Event] = []

        while len(selected_links) < limit:
            events_page, total = await uow.events.get_paginated(page, page_size, sort_by='event_date', order='asc')
            if not events_page:
                break

            # Evaluate ingestion status for each event on this page
            for evt in events_page:
                if len(selected_links) >= limit:
                    break
                link = evt.event_link
                if not link:
                    continue
                if not await self._is_event_ingested(uow, link):
                    selected_links.append(link)
                    selected_events.append(evt)

            if page * page_size >= total:
                break
            page += 1

        if not selected_links:
            logger.info('No unseeded events found to ingest')
            return ([], [], [])

        # Ingest events one-by-one to ensure per-event marking only when all unique fighters are present
        fighters_seeded: dict[str, Fighter] = {}
        new_fighters: dict[str, Fighter] = {}
        for evt in selected_events:
            seeded_for_event = await self._ingest_single_event(uow, evt)
            for f in seeded_for_event:
                # Track all encountered
                fighters_seeded[f.fighter_id] = f
                # If this fighter appears for the first time in this run, mark as new
                if f.fighter_id not in new_fighters:
                    new_fighters[f.fighter_id] = f
        logger.info(
            f'Batch ingestion complete. Events={len(selected_links)} Fighters={len(fighters_seeded)} NewFighters={len(new_fighters)}'
        )
        return (list(fighters_seeded.values()), list(new_fighters.values()), selected_events)

    async def _is_event_ingested(self, uow: UnitOfWork, event_link: str) -> bool:
        """An event is ingested if all its fighters already exist (pre-UFC bouts may be zero)."""
        # Short-circuit if event is already marked seeded
        evt = await uow.events.get_by_event_link(event_link)
        if evt and getattr(evt, 'fighters_seeded', False):
            logger.debug(f'Event is already marked as fighters_seeded: link={event_link}')
            return True
        await self._sleep_before_http()
        try:
            fighters = await asyncio.to_thread(self._event_scraper.get_event_fighters, event_link)
        except Exception as exc:
            logger.warning(f'Failed to fetch event fighters due to {exc.__class__.__name__}: link={event_link}')
            return False
        if not fighters:
            logger.debug(f'Event has no parsed fighters; treating as not ingested: link={event_link}')
            return False
        missing: list[str] = []
        for sf in fighters:
            existing = await self._get_existing_by_tapology_link(uow, sf.fighter_link)
            if not existing:
                missing.append(sf.fighter_link)
        if missing:
            logger.info(
                f'Event not fully ingested yet: link={event_link} parsed={len(fighters)} missing={len(missing)} sample_missing={missing[:3]}'
            )
            return False
        # If we reach here, consider event ingested and mark it
        if evt and not getattr(evt, 'fighters_seeded', False):
            try:
                await uow.events.update(evt.event_id, {'fighters_seeded': True})
            except Exception:
                logger.warning(f'Failed to mark event as fighters_seeded: link={event_link}')
        return True

    async def _get_existing_by_tapology_link(self, uow: UnitOfWork, link: str) -> Fighter | None:
        """Fetch fighter by tapology link with canonicalization and common variants."""
        # Try canonical first
        canon = _canonicalize_tapology_link(link)
        if canon:
            f = await uow.fighters.get_by_tapology_link(canon)
            if f:
                return f
        # Try stored original
        f = await uow.fighters.get_by_tapology_link(link)
        if f:
            return f
        # Try variants (http/https, trailing slash toggles)
        for v in _tapology_link_variants(link):
            f = await uow.fighters.get_by_tapology_link(v)
            if f:
                return f
        return None

    async def _ingest_single_event(self, uow: UnitOfWork, evt: Event) -> list[Fighter]:
        """Ingest a single event: seed unique fighters, skip existing, and mark event when complete.

        Returns a list of fighters encountered (existing or newly created) for this event.
        """
        if not evt.event_link:
            logger.warning(f'Event has no link; skipping ingestion: event_id={evt.event_id!s}')
            return []

        await self._sleep_before_http()
        try:
            scraped = await asyncio.to_thread(self._event_scraper.get_event_fighters, evt.event_link)
        except Exception as exc:
            logger.warning(
                f'Failed to fetch fighters for event due to {exc.__class__.__name__}; not marking seeded: event_id={evt.event_id!s}'
            )
            return []
        if not scraped:
            logger.warning(f'No fighters parsed for event; not marking seeded: event_id={evt.event_id!s}')
            return []

        unique_links = {sf.fighter_link for sf in scraped}
        logger.info(
            f'Event unique fighters to process: event_id={evt.event_id!s} link={evt.event_link} unique={len(unique_links)}'
        )

        sem = asyncio.Semaphore(3)
        fighters_seeded: dict[str, Fighter] = {}
        new_fighters: dict[str, Fighter] = {}

        async def process(
            link: str,
            _sem: asyncio.Semaphore = sem,
            _fighters_seeded: dict[str, Fighter] = fighters_seeded,
            _new_fighters: dict[str, Fighter] = new_fighters,
        ) -> None:
            async with _sem:
                await self._process_fighter_link(uow, link, _fighters_seeded, _new_fighters)

        # Process all fighters for the event
        await asyncio.gather(*(process(link) for link in unique_links))

        # Verify all unique fighters exist now (either newly added or previously existing)
        all_exist = True
        for link in unique_links:
            existing = await self._get_existing_by_tapology_link(uow, link)
            if not existing:
                all_exist = False
                logger.warning(
                    f'Not marking event as seeded; missing fighter for link: event_id={evt.event_id!s} link={link}'
                )
                break

        if all_exist:
            try:
                await uow.events.update(evt.event_id, {'fighters_seeded': True})
                logger.info(f'Marked event as fighters_seeded: event_id={evt.event_id!s}')
            except Exception:
                logger.warning(f'Failed to mark event as fighters_seeded: event_id={evt.event_id!s}')

        return list(fighters_seeded.values())

    async def _process_fighter_link(
        self, uow: UnitOfWork, tapology_link: str, fighters_seeded: dict[str, Fighter], new_fighters: dict[str, Fighter]
    ) -> None:
        logger.debug(f'Processing fighter link: {tapology_link}')
        # If fighter already exists, consider it seeded and avoid re-scraping pre-UFC and recalculating ELO
        existing = await self._get_existing_by_tapology_link(uow, tapology_link)
        if existing:
            # Ensure stats_link if missing via a lightweight resolver; skip pre-UFC/elo work
            if not existing.stats_link and isinstance(self._stats_search, StatsSearchScraper):
                await self._sleep_before_http()
                resolved = await asyncio.to_thread(self._stats_search.get_link_from_tapology, tapology_link)
                if resolved:
                    # Patch only missing link fields; avoid ELO changes
                    await self._ensure_fighter(
                        uow, name=existing.name, tapology_link=tapology_link, stats_link=resolved
                    )
            fighters_seeded[existing.fighter_id] = existing
            logger.info(f'Fighter already exists; skipping pre-UFC and ELO recompute: fighter_id={existing.fighter_id}')
            return

        # Scrape fighter profile in one pass (no DB writes yet)
        await self._sleep_before_http()
        profile: ScrapedFighterProfile = await asyncio.to_thread(self._fighter_scraper.get_profile, tapology_link)
        pre_list = list(profile.iter_pre_ufc_bouts())
        logger.debug(
            f'Scraped profile summary: tapology={profile.tapology_link} name={profile.name} stats_link={profile.stats_link} pre_bouts={len(pre_list)}'
        )

        # Resolve stats link if missing (still no DB writes)
        stats_link = profile.stats_link
        if not stats_link and isinstance(self._stats_search, StatsSearchScraper):
            await self._sleep_before_http()
            stats_link = await asyncio.to_thread(self._stats_search.get_link_from_tapology, profile.tapology_link)
            logger.debug(
                f'Resolved stats link via Tapology-based search: tapology={profile.tapology_link} stats_link={stats_link}'
            )
        if not stats_link and profile.name:
            await self._sleep_before_http()
            stats_link = await asyncio.to_thread(self._stats_search.search_fighter, profile.name)
            logger.debug(f'Resolved stats link via name search: name={profile.name} stats_link={stats_link}')

        # Compute starting ELO from pre-UFC bouts and promotion strengths (no DB writes yet)
        promo_repo = await self._build_promotions_repo_for_bouts(uow, pre_list)
        starting_elo = compute_starting_elo(pre_list, promotions_repo=promo_repo)
        logger.info(f'Computed starting ELO (pre-persist): link={tapology_link} elo={starting_elo:.3f}')

        # Now persist fighter and bouts atomically within UoW
        # Create new fighter with computed ELOs on insert (we reach here only when no existing fighter)
        name = profile.name or _stable_id_from_link(profile.tapology_link)
        fighter = await self._ensure_fighter(
            uow,
            name=name,
            tapology_link=_canonicalize_tapology_link(profile.tapology_link) or profile.tapology_link,
            stats_link=stats_link,
            starting_elo=starting_elo,
        )
        logger.info(
            f'Created fighter with computed ELO: fighter_id={fighter.fighter_id} name={fighter.name} elo={starting_elo:.3f}'
        )

        # Persist promotions and pre-UFC bouts after fighter exists
        await self._persist_pre_ufc_bouts(uow, fighter.fighter_id, pre_list)
        logger.info(f'Persisted pre-UFC bouts: fighter_id={fighter.fighter_id} count={len(pre_list)}')

        fighters_seeded[fighter.fighter_id] = fighter
        new_fighters[fighter.fighter_id] = fighter

    async def _ensure_fighter(
        self, uow: UnitOfWork, name: str, tapology_link: str, stats_link: str | None, starting_elo: float | None = None
    ) -> Fighter:
        tapology_link = _canonicalize_tapology_link(tapology_link) or tapology_link
        fighter = await uow.fighters.get_by_tapology_link(tapology_link)
        if not fighter and stats_link:
            fighter = await uow.fighters.get_by_stats_link(stats_link)

        if fighter:
            # Update missing links if needed
            updates: dict[str, Any] = {}
            # Always prefer canonical stored link if db differs
            if not fighter.tapology_link or fighter.tapology_link != tapology_link:
                updates['tapology_link'] = tapology_link
            if stats_link and not fighter.stats_link:
                updates['stats_link'] = stats_link
            if updates:
                # Fighters repo expects update by string PK; we have helper there
                fighter = await uow.fighters.update_fighter_elo(fighter.fighter_id, fighter.current_elo or 0)
                # Note: update_fighter_elo updates only ELO, so we need an explicit column update path
                # Fallback: perform manual update for links
                cmd = (
                    sa_update(uow.fighters.table)
                    .where(uow.fighters.table.c.fighter_id == fighter.fighter_id)
                    .values(**updates)
                    .returning(*uow.fighters.table.columns)
                )
                result = await uow.connection.execute(cmd)
                row = result.first()
                if row:
                    fighter = Fighter.from_dict(row._asdict())
            return fighter

        # Create new fighter with deterministic fighter_id
        fid = _fighter_id_from_stats_link(stats_link) or _stable_id_from_link(tapology_link)
        entity = Fighter(
            fighter_id=fid,
            name=name,
            entry_elo=float(starting_elo or 0.0),
            current_elo=float(starting_elo or 0.0),
            peak_elo=float(starting_elo or 0.0),
            tapology_link=_canonicalize_tapology_link(tapology_link) or tapology_link,
            stats_link=stats_link,
        )
        return await uow.fighters.add(entity)

    async def _persist_pre_ufc_bouts(self, uow: UnitOfWork, fighter_id: str, bouts: list[ScrapedPreUfcBout]) -> None:
        for b in bouts:
            promotion_id: UUID | None = None
            if b.promotion:
                promotion = await self._ensure_promotion(uow, b.promotion)
                promotion_id = promotion.promotion_id

            # Dedup key could be implemented here if persistence grows
            entity = PreUfcBout(
                bout_id=uuid4(),
                fighter_id=fighter_id,
                promotion_id=promotion_id,
                result=b.result if isinstance(b.result, FightOutcome) else None,
            )
            await uow.pre_ufc_bouts.add(entity)

    async def _ensure_promotion(self, uow: UnitOfWork, promo: ScrapedPromotion) -> Promotion:
        # Try by link first if present
        existing: Promotion | None = None
        norm_link = _normalize_tapology_link(promo.link)
        if norm_link:
            existing = await uow.promotions.get_by_link(norm_link)
        if existing:
            return existing

        # Fallback by name
        existing = await uow.promotions.get_by_name(promo.name)
        if existing:
            return existing

        # Create new promotion with generated id
        entity = Promotion(promotion_id=uuid4(), name=promo.name, link=norm_link, strength=None)
        created = await uow.promotions.add(entity)
        logger.debug(f'Created promotion: name={created.name} link={created.link}')
        return created

    async def _build_promotions_repo_for_bouts(
        self, uow: UnitOfWork, bouts: list[ScrapedPreUfcBout]
    ) -> PromotionsRepoProtocol:
        # Build a simple sync adapter from promotion link -> strength
        raw_links = [b.promotion.link for b in bouts if b.promotion and b.promotion.link]
        links: set[str] = set()
        for raw in raw_links:
            norm = _normalize_tapology_link(raw)
            if norm:
                links.add(norm)
        strength_by_link: dict[str, float] = {}

        async def fetch(link: str) -> None:
            promo = await uow.promotions.get_by_link(link)
            if promo and promo.strength is not None:
                strength_by_link[link] = float(promo.strength)

        await asyncio.gather(*(fetch(link) for link in links))
        logger.debug(f'Promotion strengths resolved: requested={len(links)} found={len(strength_by_link)}')

        class _Repo(PromotionsRepoProtocol):
            def get_strength_by_link(self, link: str, default: float) -> float:  # pragma: no cover - simple adapter
                return float(strength_by_link.get(link, default))

        return _Repo()

    async def _update_fighter_elos(self, uow: UnitOfWork, fighter: Fighter, starting_elo: float) -> None:
        # Update current and entry ELO; update peak if higher
        new_current: float = starting_elo
        new_peak: float = max(float(fighter.peak_elo or 0.0), new_current)

        # Reuse repository's special update method for ELO then patch peak if needed
        await uow.fighters.update_fighter_elo(fighter.fighter_id, new_current)

        if new_peak != float(fighter.peak_elo or 0.0):
            cmd = (
                sa_update(uow.fighters.table)
                .where(uow.fighters.table.c.fighter_id == fighter.fighter_id)
                .values(peak_elo=new_peak, entry_elo=starting_elo)
            )
            await uow.connection.execute(cmd)
