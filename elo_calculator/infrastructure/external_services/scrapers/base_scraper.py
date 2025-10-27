from __future__ import annotations

import random
import secrets
import time
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from httpx import HTTPStatusError

from elo_calculator.configs import env as app_env
from elo_calculator.configs.log import get_logger
from elo_calculator.infrastructure.utils.http_client import get_client


class _ScraperState:
    """Mutable state holder to avoid `global` statements."""

    def __init__(self) -> None:
        self.last_request_time: float = 0.0


_STATE = _ScraperState()
logger = get_logger()


def _random_user_agent() -> str:
    return random.choice(app_env.SCRAPER_USER_AGENTS)  # noqa: S311


def _random_accept_language() -> str:
    return random.choice(app_env.SCRAPER_ACCEPT_LANGUAGES)  # noqa: S311


def _default_referer_for(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f'{parsed.scheme}://{parsed.netloc}/'
    return 'https://www.google.com/'


def build_headers(url: str, referer: str | None = None, extra: dict[str, str] | None = None) -> dict[str, str]:
    """Build realistic request headers with rotation and optional overrides."""
    headers = {
        'User-Agent': _random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': _random_accept_language(),
        'Referer': referer or _default_referer_for(url),
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
    }
    if extra:
        headers.update(extra)
    return headers


# All configuration now comes from elo_calculator.configs.env


def fetch_html(
    url: str, timeout: float = 15.0, referer: str | None = None, extra_headers: dict[str, str] | None = None
) -> str:
    """Fetch raw HTML from a URL and return it as text with throttling.

    Retries are handled by the shared HTTP client (httpx with RetryTransport).
    This function enforces a minimum interval between requests, adds a small
    random jitter to timing, and sets realistic rotating headers.
    """
    # Throttle between requests
    now = time.monotonic()
    elapsed = now - _STATE.last_request_time
    min_interval = max(0.0, float(app_env.SCRAPER_MIN_INTERVAL_SECONDS))
    if elapsed < min_interval:
        # Add small jitter (0-800ms) to avoid uniform intervals during heavy runs
        # Use secrets for consistency with other jitter implementations
        jitter = secrets.randbelow(801) / 1000.0
        progress_sleep((min_interval - elapsed) + jitter, description='HTTP throttle')

    headers = build_headers(url, referer=referer, extra=extra_headers)
    try:
        with get_client(timeout=timeout) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            _STATE.last_request_time = time.monotonic()
            return resp.text
    except HTTPStatusError as exc:
        # If Tapology or UFCStats returns 503, pause 10-15 minutes and retry once
        status = getattr(exc.response, 'status_code', None)
        service_unavailable = 503
        if status == service_unavailable:
            # 10-15 minutes backoff window; use secrets to avoid predictable intervals
            wait_seconds = float(600 + secrets.randbelow(301))
            logger.error(
                f'503 Service Unavailable for {url}; cooling down for {wait_seconds:.0f} seconds before single retry'
            )
            progress_sleep(wait_seconds, description='503 cooldown')
            # Rebuild headers (rotate UA) before retry
            headers = build_headers(url, referer=referer, extra=extra_headers)
            with get_client(timeout=timeout) as client:
                resp = client.get(url, headers=headers)
                resp.raise_for_status()
                _STATE.last_request_time = time.monotonic()
                return resp.text
        raise


def parse_with_bs(html: str) -> BeautifulSoup:
    """Parse HTML with BeautifulSoup and return a soup object."""
    return BeautifulSoup(html, 'html.parser')


def progress_sleep(seconds: float, description: str | None = None) -> None:
    """Sleep helper with optional description for progress reporting.

    This stub integrates easily with a richer progress system later.
    """
    _ = description
    time.sleep(max(0.0, float(seconds)))
