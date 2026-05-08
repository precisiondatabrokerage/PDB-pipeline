from __future__ import annotations

from config.web_scraper_targets import (
    DEFAULT_YELLOWPAGES_INDUSTRIES,
    DEFAULT_YELLOWPAGES_LOCATIONS,
    WEB_TARGET_MATRIX,
    get_active_yellowpages_targets,
)
from scrapers.yellowpages_scraper import (
    __version__,
    fetch_yellowpages_scraper,
)


def fetch_yellowpages_playwright(search_term: str, location: str):
    return fetch_yellowpages_scraper(
        search_term=search_term,
        location=location,
        headless=True,
        max_pages=1,
        max_scrolls=2,
    )


__all__ = [
    "__version__",
    "fetch_yellowpages_playwright",
    "fetch_yellowpages_scraper",
    "WEB_TARGET_MATRIX",
    "get_active_yellowpages_targets",
    "DEFAULT_YELLOWPAGES_INDUSTRIES",
    "DEFAULT_YELLOWPAGES_LOCATIONS",
]