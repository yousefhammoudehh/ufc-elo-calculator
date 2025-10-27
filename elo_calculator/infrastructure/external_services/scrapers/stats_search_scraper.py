from __future__ import annotations

# isort: skip_file

import logging
from functools import lru_cache
import re
from urllib.parse import quote_plus

from elo_calculator.domain.client.stats_search_port import StatsSearchPort
from elo_calculator.infrastructure.external_services.scrapers.base_scraper import fetch_html, parse_with_bs

# Tunable heuristics
MIN_ACCEPT_SCORE = 40
MIN_PARTS_FOR_NICKNAME = 3
SHORT_SURNAME_LEN = 3
MIN_NICKNAME_SURNAME_LEN = 3
AUTO_ACCEPT_SCORE = 70
AUTO_ACCEPT_MARGIN = 15
CANDIDATE_DETAILS_LIMIT = 8  # cap detailed page fetches per query to reduce HTTP


class StatsSearchScraper(StatsSearchPort):
    def search_fighter(self, name: str) -> str | None:  # pragma: no cover - IO boundary
        # 1) Derive expected name parts from the query
        q_first, q_last, q_nick = _expected_name_parts_from_query(name)

        # 2) Search UFCStats for candidates
        candidates = _search_ufcstats_candidates(name)
        if not candidates:
            return None

        best_href: str | None = None
        best_score = -1

        # 3) Score candidates by label first; then refine using the candidate page details
        for href, label in candidates:
            base_score = _score_name_match(label, q_first, q_last, q_nick)

            # Optionally refine score using the UFCStats fighter-details page
            f2, l2, n2 = _get_ufcstats_name_parts(href)
            if f2 or l2 or n2:
                refined_label = ' '.join([p for p in [f2, l2, n2] if p])
                refined_score = _score_name_match(refined_label, q_first, q_last, q_nick)
                score = max(base_score, refined_score)
            else:
                score = base_score

            if score > best_score:
                best_score = score
                best_href = href

        # 4) Require a modest threshold to avoid random mismatches
        return best_href if best_score >= MIN_ACCEPT_SCORE else None

    def get_link_from_tapology(self, url: str) -> str | None:  # pragma: no cover - IO boundary
        # Try explicit external link first
        direct = _tapology_external_link(url)
        if direct:
            return direct

        # Build queries from refined name parts
        queries, parts = _build_queries_from_tapology(url)
        if not queries:
            return None

        # Collect results by query
        by_query = _collect_results_by_query(queries)
        if not by_query:
            return None

        # Sort by fewest candidates (>0), earliest tie-breaker
        by_query.sort(key=lambda item: (len(item[1]), queries.index(item[0]) if item[0] in queries else 9999))

        # Try queries in order until one yields a confident result
        for _chosen_query, candidates in by_query:
            if len(candidates) == 1:
                return candidates[0][0]
            href = _auto_select_candidate(candidates, parts)
            if href:
                return href

        # No query produced a unique or confidently auto-selected candidate
        return None


def _name_parts_from_slug(url: str) -> tuple[str | None, str | None, str | None]:
    """Extract (first_name, last_name, nickname) from a Tapology fighter slug in the URL.

    Examples:
    - .../fighters/35757-kevin-holland -> ("Kevin", "Holland", None)
    - .../fighters/12345-max-blessed-holloway -> ("Max", "Holloway", "Blessed")
    - .../fighters/99999-robert-the-reaper-whittaker -> ("Robert", "Whittaker", "The Reaper")
    Fallback returns (None, None, None) if parsing fails.
    """
    try:
        slug = url.rstrip('/').split('/')[-1]
        parts = slug.split('-')
        if parts and parts[0].isdigit():
            parts = parts[1:]
        parts = [p for p in parts if p]
        if not parts:
            return None, None, None
        # Title-case each piece
        words = [re.sub(r'\s+', ' ', p).strip().title() for p in parts]
        if len(words) == 1:
            return words[0], None, None
        first = words[0]
        last = words[-1]
        nickname = ' '.join(words[1:-1]) if len(words) >= MIN_PARTS_FOR_NICKNAME else None
        nickname = nickname if nickname and nickname.strip() else None
        # Heuristic fix: if last is very short (<=3) and nickname looks like a surname, swap
        if nickname and last and len(last) <= SHORT_SURNAME_LEN and len(nickname) >= MIN_NICKNAME_SURNAME_LEN:
            last, nickname = nickname, last
        return first, last, nickname
    except Exception:
        return None, None, None


def _expected_name_parts_from_query(name: str) -> tuple[str | None, str | None, str | None]:
    """Parse a query string into (first, last, nickname).

    Handles nickname wrapped in quotes or parentheses; falls back to first/last tokens.
    """
    try:
        base = name.strip()
        # Extract nickname in quotes or parentheses; keep straight or curly quotes
        m = re.search(r'(?:\(|[\"\u201c\u201d])([^\)\"\u201c\u201d]+)(?:\)|[\"\u201c\u201d])', base)
        nickname = m.group(1).strip() if m else None
        if m:
            # Remove the matched nickname segment (including quotes/parens) from base
            start, end = m.span()
            base = (base[:start] + base[end:]).strip()

        parts = [p for p in re.split(r'\s+', base) if p]
        if not parts:
            return None, None, nickname
        if len(parts) == 1:
            return parts[0].title(), None, nickname
        first = parts[0].title()
        last = parts[-1].title()
        return first, last, nickname.title() if nickname else None
    except Exception:
        return None, None, None


@lru_cache(maxsize=256)
def _refine_expected_name_parts_from_tapology(
    url: str, first: str | None, last: str | None, nickname: str | None
) -> tuple[str | None, str | None, str | None]:
    """Refine name parts by parsing Tapology page title if available.

    Title commonly contains First 'Nickname' Last; we extract reliably when possible.
    """
    try:
        page = parse_with_bs(fetch_html(url))
        title_el = page.find('title')
        if title_el and title_el.text:
            title = title_el.text.strip()
            # Use only the left-most part before the first pipe to avoid trailing context
            base = title.split('|', 1)[0].strip()
            # Try to extract nickname in parentheses with straight or curly quotes: ("Nickname")
            m_nick = re.search(r'\((?:\"|\u201c|\u201d)?([^\"\u201c\u201d\)]+)(?:\"|\u201c|\u201d)?\)\s*$', base)
            nick_val = None
            if m_nick:
                nick_val = m_nick.group(1).strip()
                # Remove the nickname part from base to leave just the names
                base = base[: m_nick.start()].strip()
            # Now split base as First Last (use first token and last token)
            parts = [p for p in base.split() if p]
            if parts:
                f2 = parts[0]
                l2 = parts[-1] if len(parts) > 1 else None
            else:
                f2, l2 = None, None
            # Prefer refined parts when present
            first = f2 or first
            last = l2 or last
            if nick_val:
                nickname = nick_val
            return first, last, nickname
    except Exception as exc:
        logging.exception('Tapology refinement failed: %s', exc)
    return first, last, nickname


@lru_cache(maxsize=256)
def _search_ufcstats_candidates(query: str) -> list[tuple[str, str]]:
    """Search UFCStats for a name query and return list of (href, label).

    Looks for anchors with class 'b-link b-link_style_black' that contain fighter-details links.
    """
    base = 'http://www.ufcstats.com/statistics/fighters/search?query='
    url = f'{base}{quote_plus(query)}'
    page = parse_with_bs(fetch_html(url))
    results: list[tuple[str, str]] = []
    seen = set()
    for a in page.select('a.b-link.b-link_style_black[href]'):
        href_val = a.get('href', '')
        if not isinstance(href_val, str):
            # Skip non-string hrefs (e.g., attribute lists)
            continue
        href = href_val
        text = a.get_text(strip=True)
        if not href or '/fighter-details/' not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        results.append((href, text))
    return results


# Note: interactive helpers like timed input were removed as they aren't used in the service context


def _score_name_match(label: str, first: str | None, last: str | None, nickname: str | None) -> int:
    """Heuristic score for how well a UFCStats label matches expected name parts.

    No external deps; uses token containment and initials. Returns 0-100.
    """
    if not label:
        return 0
    tokens = [t for t in label.lower().split() if t]
    score = 0
    if last:
        last_l = last.lower()
        if last_l in tokens:
            score += 50
    if first:
        f = first.lower()
        if f in tokens:
            score += 40
        elif last and any(len(t) == 1 and t == f[:1] for t in tokens):
            # first initial match alongside last name
            score += 15
    if nickname:
        n = nickname.lower()
        if n in tokens:
            score += 10
    # small bonus for two-token exact order match
    if first and last:
        norm = f'{first} {last}'.lower()
        if norm == label.lower().strip():
            score += 10
    return min(score, 100)


@lru_cache(maxsize=256)
def _get_ufcstats_name_parts(href: str) -> tuple[str | None, str | None, str | None]:
    """Fetch a UFCStats fighter-details page and extract (first, last, nickname).

    Best-effort parsing: uses title-highlight for full name and title-secondary for nickname.
    Returns (None, None, None) on failure.
    """
    try:
        page = parse_with_bs(fetch_html(href))
        # Name
        name_el = page.select_one('span.b-content__title-highlight')
        full_name = name_el.get_text(strip=True) if name_el else None
        # Nickname (if present)
        nick_el = page.select_one('span.b-content__title-secondary')
        nickname = nick_el.get_text(strip=True) if nick_el else None
        # Normalize nickname (remove quotes if UFCStats wraps with quotes)
        if nickname:
            nickname = nickname.strip('"\'\u201c\u201d ').strip()
        if not full_name:
            return None, None, nickname
        parts = [p for p in full_name.split() if p]
        if len(parts) == 1:
            return parts[0], None, nickname
        return parts[0], parts[-1], nickname
    except Exception:
        return None, None, None


@lru_cache(maxsize=256)
def _tapology_external_link(url: str) -> str | None:
    try:
        page = parse_with_bs(fetch_html(url))
        a = page.find('a', onclick="gtag('event', 'external_click_fighter_ufc');")
        href = a.get('href') if a else None
        return href if isinstance(href, str) else None
    except Exception as exc:
        logging.exception('Tapology external link fetch failed: %s', exc)
        return None


def _build_queries_from_tapology(url: str) -> tuple[list[str], tuple[str | None, str | None, str | None]]:
    first, last, nickname = _name_parts_from_slug(url)
    first, last, nickname = _refine_expected_name_parts_from_tapology(url, first, last, nickname)
    queries: list[str] = []
    if first and nickname and last:
        queries.append(f'{first} {nickname} {last}')
    if first and last:
        queries.append(f'{first} {last}')
    if nickname and last:
        queries.append(f'{nickname} {last}')
    if first and nickname:
        queries.append(f'{first} {nickname}')
    if nickname:
        queries.append(nickname)
    if last:
        queries.append(last)
    if first:
        queries.append(first)
    return queries, (first, last, nickname)


def _collect_results_by_query(queries: list[str]) -> list[tuple[str, list[tuple[str, str]]]]:
    results_by_query: list[tuple[str, list[tuple[str, str]]]] = []
    for q in queries:
        try:
            found = _search_ufcstats_candidates(q)
        except Exception as exc:
            logging.exception('UFCStats search failed for query %s: %s', q, exc)
            found = []
        if found:
            results_by_query.append((q, found))
    return results_by_query


def _auto_select_candidate(
    candidates: list[tuple[str, str]], parts: tuple[str | None, str | None, str | None]
) -> str | None:
    first, last, nickname = parts
    # Pre-filter: compute base label score (no network), then fetch details for top-K only
    base_scored = [(href, label, _score_name_match(label, first, last, nickname)) for href, label in candidates]
    base_scored.sort(key=lambda t: t[2], reverse=True)
    top = base_scored[:CANDIDATE_DETAILS_LIMIT]
    # Fetch candidate details for the top-K
    cand_details: list[tuple[str, str, str | None, str | None, str | None]] = []
    for href, label, _ in top:
        c_first, c_last, c_nick = _get_ufcstats_name_parts(href)
        cand_details.append((href, label, c_first, c_last, c_nick))

    def _score_candidate(label: str, c_first: str | None, c_last: str | None, c_nick: str | None) -> int:
        base = _score_name_match(label, first, last, nickname)
        boost = 0
        try:
            if last and c_last and last.lower() == c_last.lower():
                boost += 60
            if first and c_first and first.lower() == c_first.lower():
                boost += 30
            if nickname and c_nick and nickname.lower() == c_nick.lower():
                boost += 10
            if (
                first
                and last
                and c_first
                and c_last
                and first.lower() == c_first.lower()
                and last.lower() == c_last.lower()
            ):
                boost += 10
        except Exception as exc:
            logging.debug('Scoring boost calc failed: %s', exc)
        return min(100, base + boost)

    best_href: str | None = None
    best_score = -1
    second_best = -1
    for href, label, c_first, c_last, c_nick in cand_details:
        s = _score_candidate(label, c_first, c_last, c_nick)
        if s > best_score:
            second_best = best_score
            best_score = s
            best_href = href
        elif s > second_best:
            second_best = s

    margin = best_score - max(0, second_best)
    if best_href and best_score >= AUTO_ACCEPT_SCORE and margin >= AUTO_ACCEPT_MARGIN:
        return best_href
    return None
