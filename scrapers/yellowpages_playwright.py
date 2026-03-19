# scrapers/yellowpages_playwright.py
from __future__ import annotations

from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

try:
    # Newer / current package style
    from playwright_stealth import Stealth  # type: ignore
    _HAS_STEALTH_CLASS = True
except Exception:
    Stealth = None  # type: ignore
    _HAS_STEALTH_CLASS = False

try:
    # Older package style
    from playwright_stealth import stealth_sync  # type: ignore
    _HAS_STEALTH_SYNC = True
except Exception:
    stealth_sync = None  # type: ignore
    _HAS_STEALTH_SYNC = False

__version__ = "3.0.1"

BASE_URL = "https://www.yellowpages.com"
SEARCH_URL = "https://www.yellowpages.com/search"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _safe_text(el):
    try:
        return el.get_text(strip=True)
    except Exception:
        return None


def _apply_stealth(page) -> None:
    """
    Support both old and new playwright-stealth APIs.
    """
    if _HAS_STEALTH_CLASS and Stealth is not None:
        stealth = Stealth()
        # Current/common sync method used by many projects
        if hasattr(stealth, "apply_stealth_sync"):
            stealth.apply_stealth_sync(page)
            return
        # Fallback in case only async/context-based methods exist
        if hasattr(stealth, "apply_stealth_async"):
            # sync scraper cannot await; skip instead of crashing
            return

    if _HAS_STEALTH_SYNC and stealth_sync is not None:
        stealth_sync(page)
        return


def _parse_cards(html: str, industry_tag: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".result, .v-card")
    print("YP cards detected:", len(cards))

    results = []
    for card in cards:
        name_span = card.select_one("a.business-name span")
        name_a = card.select_one("a.business-name")

        if not name_span and name_a:
            name = _safe_text(name_a)
        else:
            name = _safe_text(name_span)

        if not name:
            continue

        href = name_a.get("href") if name_a else None
        source_id = href.split("?")[0] if href else None
        detail_url = (
            urljoin(BASE_URL, source_id)
            if source_id and source_id.startswith("/")
            else None
        )

        phone = _safe_text(card.select_one(".phones"))
        street = _safe_text(card.select_one(".street-address"))
        locality = _safe_text(card.select_one(".locality"))

        address = None
        if street and locality:
            address = f"{street}, {locality}"
        elif street:
            address = street
        elif locality:
            address = locality

        website = None
        website_a = card.select_one(
            'a.track-visit-website[href], a.website-link[href], a[href^="http"][rel*="nofollow"]'
        )
        if website_a:
            website = website_a.get("href")

        results.append(
            {
                "source": "yellowpages",
                "industry": industry_tag,
                "raw_company_name": name,
                "raw_address": address,
                "raw_phone": phone,
                "raw_website": website,
                "raw_email": None,
                "lat": None,
                "lng": None,
                "source_id": source_id,
                "detail_url": detail_url,
                "raw_json": None,
                "scraped_at": datetime.utcnow().isoformat(),
            }
        )

    return results


def fetch_yellowpages_playwright(search_term: str, location: str) -> list[dict]:
    """
    Pipeline-safe adapter.
    - Does NOT write to Mongo
    - Does NOT write CSV
    - Returns list of raw business dicts
    """
    results: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=UA,
        )
        page = context.new_page()

        _apply_stealth(page)

        params = (
            f"?search_terms={search_term.replace(' ', '+')}"
            f"&geo_location_terms={location.replace(' ', '+')}"
        )
        url = SEARCH_URL + params

        try:
            page.goto(url, timeout=45000)
            page.wait_for_selector("a.business-name", timeout=25000)

            page.mouse.wheel(0, 3500)
            page.wait_for_timeout(1500)
            page.mouse.wheel(0, 3500)
            page.wait_for_timeout(1500)

            html = page.content()
            print("YP HTML length:", len(html))

            if "captcha" in html.lower():
                return []

            results = _parse_cards(html, industry_tag=search_term)
            return results

        finally:
            browser.close()