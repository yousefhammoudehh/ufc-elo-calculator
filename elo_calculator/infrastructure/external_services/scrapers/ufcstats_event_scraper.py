from __future__ import annotations

# ruff: noqa

from dataclasses import dataclass
from datetime import date
import re

from bs4 import BeautifulSoup

from elo_calculator.configs.log import get_logger
from elo_calculator.domain.client.models import ScrapedFight, ScrapedFightFighter
from elo_calculator.infrastructure.external_services.scrapers.base_scraper import fetch_html, parse_with_bs

logger = get_logger()


# --- Light parsing helpers ---


def _parse_int(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        try:
            # Some cells contain '\xa0' or '-' when not applicable
            return int(s.strip().replace('\xa0', '').replace('-', ''))
        except Exception:
            return None


def _parse_percent(s: str | None) -> float | None:
    if not s:
        return None
    try:
        t = s.strip().replace('%', '')
        val = float(t)
        # Convert 0..1 to percentage points if needed
        return val * 100.0 if val <= 1.0 else val
    except Exception:
        return None


def _parse_of(s: str | None) -> tuple[int | None, int | None]:
    if not s:
        return None, None
    try:
        if 'of' in s:
            left, right = s.split('of', 1)
            return _parse_int(left.strip()), _parse_int(right.strip())
        return _parse_int(s.strip()), None
    except Exception:
        return None, None


def _time_to_seconds(s: str | None) -> int | None:
    if not s:
        return None
    try:
        parts = s.strip().split(':')
        if len(parts) == 2:
            m, sec = int(parts[0]), int(parts[1])
            return max(0, m * 60 + sec)
        if len(parts) == 3:
            h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
            return max(0, h * 3600 + m * 60 + sec)
        return _parse_int(s)
    except Exception:
        return None


@dataclass
class _FightMeta:
    method: str
    round_num: int | None
    time_str: str | None
    time_format: str | None
    is_title_fight: bool


class UFCStatsEventScraper:
    """Scraper for UFCStats event and fight pages."""

    def get_event_fight_links(self, event_stats_link: str) -> list[str]:  # pragma: no cover - IO boundary
        html = fetch_html(event_stats_link)
        soup = parse_with_bs(html)
        # Fights table links include '/fight-details/ID'
        links: list[str] = []
        for a in soup.find_all('a', href=True):
            href_val = a.get('href')
            if isinstance(href_val, str) and '/fight-details/' in href_val:
                links.append(href_val)
        # Preserve order as they appear (assumed card order)
        dedup: list[str] = []
        seen: set[str] = set()
        for href in links:
            if href not in seen:
                seen.add(href)
                dedup.append(href)
        # Log with concrete values (avoid % placeholders with our logger)
        logger.info(f'UFCStats fights detected: count={len(dedup)} link={event_stats_link}')
        return dedup

    def _parse_fight_meta(self, soup: BeautifulSoup) -> _FightMeta:
        fight_details = soup.find('div', class_='b-fight-details__content')
        # Some pages structure content children differently; guard accordingly
        method = ''
        round_num: int | None = None
        time_str: str | None = None
        time_format: str | None = None
        if fight_details:
            data_nodes = fight_details.find_all('i', class_=lambda x: x and 'b-fight-details__text-item' in x)
            try:
                method = data_nodes[0].find_all('i')[1].get_text(strip=True)
            except Exception:
                method = ''
            try:
                round_text = data_nodes[1].get_text(strip=True)
                # Extract last int token
                tokens = [t for t in round_text.split() if t.isdigit()]
                round_num = int(tokens[-1]) if tokens else None
            except Exception:
                round_num = None
            try:
                time_text = data_nodes[2].get_text(strip=True)
                time_str = time_text.split('Time:')[-1].strip() if 'Time:' in time_text else time_text
            except Exception:
                time_str = None
            try:
                tformat_text = data_nodes[3].get_text(strip=True)
                time_format = tformat_text[12:].strip()
            except Exception:
                time_format = None
        # Fallback parsing by scanning text if structured nodes failed (early events often differ)
        try:
            blob = fight_details.get_text(' ', strip=True) if fight_details else ''
            if not method and 'Method:' in blob:
                method = blob.split('Method:')[-1].split('Round:')[0].strip().split('  ')[0]
            if round_num is None and ('Round:' in blob or 'ROUND:' in blob):
                import re as _re  # local import to avoid top pollution

                m = _re.search(r'(?:Round:|ROUND:)\s*(\d+)', blob)
                if m:
                    round_num = int(m.group(1))
            if time_str is None and ('Time:' in blob or 'TIME:' in blob):
                t = blob.split('Time:')[-1] if 'Time:' in blob else blob.split('TIME:')[-1]
                time_str = t.split()[0].strip()
            if time_format is None and ('Time format:' in blob or 'TIME FORMAT:' in blob):
                tf = blob.split('Time format:')[-1] if 'Time format:' in blob else blob.split('TIME FORMAT:')[-1]
                time_format = tf.split('Referee:')[0].strip() if 'Referee:' in tf else tf.strip()
        except Exception:
            pass
        # Title fight detection
        is_title = False
        try:
            title_el = soup.find('i', class_='b-fight-details__fight-title')
            title_text = title_el.get_text(separator=' ', strip=True).lower() if title_el else ''
            if 'title' in title_text or 'championship' in title_text:
                is_title = True
        except Exception:
            is_title = False
        return _FightMeta(
            method=method, round_num=round_num, time_str=time_str, time_format=time_format, is_title_fight=is_title
        )

    def _parse_person_results(self, soup: BeautifulSoup) -> dict[str, str]:
        persons_container = soup.find('div', class_='b-fight-details__persons')
        results_by_id: dict[str, str] = {}
        if persons_container:
            persons = persons_container.find_all('div', class_='b-fight-details__person')
            for person in persons:
                # Try common status element
                status_el = person.find(
                    lambda tag: tag.name in ('i', 'span', 'div')
                    and any(
                        isinstance(tag.get('class'), list) and any('person-status' in c for c in tag.get('class'))
                        for _ in [0]
                    )
                )
                status = status_el.get_text(strip=True) if status_el else None
                link = person.find('a', href=True)
                href_val = link.get('href') if link else None
                fighter_id = href_val.rstrip('/').split('/')[-1] if isinstance(href_val, str) else None
                if fighter_id and status:
                    # Normalize to first char token
                    res = status.strip().upper()
                    if res.startswith('WIN'):
                        results_by_id[fighter_id] = 'W'
                    elif res.startswith('LOSS'):
                        results_by_id[fighter_id] = 'L'
                    elif res.startswith('DRAW'):
                        results_by_id[fighter_id] = 'D'
                    elif 'NO CONTEST' in res or res.startswith('NC'):
                        results_by_id[fighter_id] = 'NC'
                elif fighter_id and not status:
                    # Fallback: heuristic scan within person block
                    try:
                        txt = person.get_text(' ', strip=True).upper()
                        if ' WIN ' in f' {txt} ':
                            results_by_id[fighter_id] = 'W'
                        elif ' LOSS ' in f' {txt} ':
                            results_by_id[fighter_id] = 'L'
                        elif ' DRAW ' in f' {txt} ':
                            results_by_id[fighter_id] = 'D'
                        elif ' NO CONTEST ' in f' {txt} ' or ' NC ' in f' {txt} ':
                            results_by_id[fighter_id] = 'NC'
                    except Exception:
                        pass
        return results_by_id

    def _parse_general_stats(self, soup: BeautifulSoup) -> tuple[ScrapedFightFighter, ScrapedFightFighter]:
        sections = soup.find_all('section', class_='b-fight-details__section')
        # General stats table typically the second section (index 1)
        general_stats_table = sections[1] if len(sections) > 1 else None
        f1 = ScrapedFightFighter()
        f2 = ScrapedFightFighter()
        if not general_stats_table:
            return f1, f2
        stats_tbody = general_stats_table.find('tbody', class_='b-fight-details__table-body')
        row = stats_tbody.find('tr', class_='b-fight-details__table-row') if stats_tbody else None
        cols = row.find_all('td', class_='b-fight-details__table-col') if row else []
        if not cols:
            return f1, f2
        name_links = cols[0].find_all('a', href=True)
        if len(name_links) >= 2:
            f1.name = name_links[0].get_text(strip=True)
            href1 = name_links[0].get('href')
            f1.fighter_id = href1.rstrip('/').split('/')[-1] if isinstance(href1, str) else None
            f2.name = name_links[1].get_text(strip=True)
            href2 = name_links[1].get('href')
            f2.fighter_id = href2.rstrip('/').split('/')[-1] if isinstance(href2, str) else None
        stat_keys = [
            'kd',
            'sig_strikes',
            'sig_strike_percent',
            'total_strikes',
            'td',
            'td_percent',
            'sub_attempts',
            'rev',
            'ctrl',
        ]
        for i, key in enumerate(stat_keys):
            if i + 1 >= len(cols):
                break
            col = cols[i + 1]
            p_tags = col.find_all('p', class_='b-fight-details__table-text')
            v1 = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else ''
            v2 = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else ''
            if key == 'ctrl':
                f1.ctrl = _time_to_seconds(v1)
                f2.ctrl = _time_to_seconds(v2)
            elif key in ('sig_strikes', 'total_strikes', 'td'):
                l1, r1 = _parse_of(v1)
                l2, r2 = _parse_of(v2)
                setattr(f1, key, l1)
                # for td, attempts live under *_td_attempts; for strikes under *_thrown
                if key == 'td':
                    setattr(f1, key + '_attempts', r1)
                else:
                    setattr(f1, key + '_thrown', r1)
                setattr(f2, key, l2)
                if key == 'td':
                    setattr(f2, key + '_attempts', r2)
                else:
                    setattr(f2, key + '_thrown', r2)
            elif key.endswith('_percent'):
                setattr(f1, key, _parse_percent(v1))
                setattr(f2, key, _parse_percent(v2))
            else:
                setattr(f1, key, _parse_int(v1))
                setattr(f2, key, _parse_int(v2))
        # Derive strike_accuracy as landed/thrown for total strikes
        for f in (f1, f2):
            landed = getattr(f, 'total_strikes', None)
            thrown = getattr(f, 'total_strikes_thrown', None)
            try:
                f.strike_accuracy = (
                    round((landed / thrown), 2) if landed is not None and thrown and thrown > 0 else None
                )
            except Exception:
                f.strike_accuracy = None
        return f1, f2

    def _parse_sig_strikes(self, soup: BeautifulSoup, dst1: ScrapedFightFighter, dst2: ScrapedFightFighter) -> None:
        sections = soup.find_all('section', class_='b-fight-details__section')
        # Significant strikes table often comes later; find by header text
        table = None
        for sec in sections:
            if sec.find(string=lambda t: isinstance(t, str) and 'Significant Strikes' in t):
                # The table is sometimes the next sibling; fallback to find('table')
                table = sec.find_next_sibling('table') or sec.find('table')
                break
        if not table:
            return
        ss_tbody = table.find('tbody', class_='b-fight-details__table-body')
        ss_row = ss_tbody.find('tr', class_='b-fight-details__table-row') if ss_tbody else None
        ss_cols = ss_row.find_all('td', class_='b-fight-details__table-col') if ss_row else []
        ss_keys = ['head_ss', 'body_ss', 'leg_ss', 'distance_ss', 'clinch_ss', 'ground_ss']
        # The order: Fighter, Sig. str, Sig. str. %, Head, Body, Leg, Distance, Clinch, Ground
        for i, key in enumerate(ss_keys):
            idx = i + 3
            if idx >= len(ss_cols):
                break
            col = ss_cols[idx]
            p_tags = col.find_all('p', class_='b-fight-details__table-text')
            v1 = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else ''
            v2 = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else ''
            landed1 = v1.split(' of ')[0] if ' of ' in v1 else v1
            landed2 = v2.split(' of ')[0] if ' of ' in v2 else v2
            setattr(dst1, key, _parse_int(landed1))
            setattr(dst2, key, _parse_int(landed2))

    def get_fight(self, fight_link: str, event_date: date | None) -> ScrapedFight:  # pragma: no cover - IO boundary
        html = fetch_html(fight_link)
        soup = parse_with_bs(html)
        # Fight id
        fight_id = fight_link.rstrip('/').split('/')[-1]
        # Meta
        meta = self._parse_fight_meta(soup)
        # Per-person status
        results_by_id = self._parse_person_results(soup)
        # General stats and names/ids
        f1, f2 = self._parse_general_stats(soup)
        # Results mapping
        if f1.fighter_id in results_by_id:
            f1.result = results_by_id[f1.fighter_id]
        if f2.fighter_id in results_by_id:
            f2.result = results_by_id[f2.fighter_id]
        # Significant strikes breakdown
        self._parse_sig_strikes(soup, f1, f2)
        # Convert time string
        time_seconds = _time_to_seconds(meta.time_str)
        return ScrapedFight(
            fight_id=fight_id,
            method=meta.method,
            round_num=meta.round_num,
            time_sec=time_seconds,
            time_format=meta.time_format,
            event_date=event_date,
            fighter1=f1,
            fighter2=f2,
            is_title_fight=meta.is_title_fight,
        )
