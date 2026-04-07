from __future__ import annotations

from scrapers.yellowpages_scraper import (
    __version__,
    fetch_yellowpages_scraper,
    DEFAULT_YELLOWPAGES_INDUSTRIES,
    DEFAULT_YELLOWPAGES_LOCATIONS,
)

def fetch_yellowpages_playwright(search_term: str, location: str):
    return fetch_yellowpages_scraper(
        search_term=search_term,
        location=location,
        headless=True,
        max_pages=2,
        max_scrolls=3,
    )

__all__ = [
    "__version__",
    "fetch_yellowpages_playwright",
    "fetch_yellowpages_scraper",
    "DEFAULT_YELLOWPAGES_INDUSTRIES",
    "DEFAULT_YELLOWPAGES_LOCATIONS",
]