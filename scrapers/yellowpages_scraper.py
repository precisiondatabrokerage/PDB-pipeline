from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

try:
    from playwright_stealth import Stealth  # type: ignore
    _HAS_STEALTH_CLASS = True
except Exception:
    Stealth = None  # type: ignore
    _HAS_STEALTH_CLASS = False

try:
    from playwright_stealth import stealth_sync  # type: ignore
    _HAS_STEALTH_SYNC = True
except Exception:
    stealth_sync = None  # type: ignore
    _HAS_STEALTH_SYNC = False

__version__ = "4.0.1"

BASE_URL = "https://www.yellowpages.com"
SEARCH_URL = "https://www.yellowpages.com/search"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_YELLOWPAGES_LOCATIONS = [
    "Knoxville, TN",
    "Maryville, TN",
]

DEFAULT_YELLOWPAGES_INDUSTRIES = [
    "Property Management",
    "HOA Management",
    "Commercial Real Estate",
    "Insurance Agencies",
    "Electricians",
    "Roofing",
    "Plumbers",
    "Contractors",
]


def _safe_text(el):
    try:
        return el.get_text(strip=True)
    except Exception:
        return None


def _apply_stealth(page) -> None:
    if _HAS_STEALTH_CLASS and Stealth is not None:
        stealth = Stealth()
        if hasattr(stealth, "apply_stealth_sync"):
            stealth.apply_stealth_sync(page)
            return

    if _HAS_STEALTH_SYNC and stealth_sync is not None:
        stealth_sync(page)


def _parse_cards(html: str, industry_tag: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".result, .v-card")
    print("YP cards detected:", len(cards))

    results = []
    seen = set()

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

        dedupe_key = (source_id, name, address)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

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


def fetch_yellowpages_scraper(
    search_term: str,
    location: str,
    *,
    headless: bool = True,
    max_pages: int = 2,
    max_scrolls: int = 3,
) -> List[Dict]:
    results: List[Dict] = []
    seen = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=UA,
        )
        page = context.new_page()
        _apply_stealth(page)

        try:
            for page_num in range(1, max_pages + 1):
                url = (
                    f"{SEARCH_URL}"
                    f"?search_terms={quote_plus(search_term)}"
                    f"&geo_location_terms={quote_plus(location)}"
                    f"&page={page_num}"
                )

                page.goto(url, timeout=45000)

                try:
                    page.wait_for_selector("a.business-name", timeout=25000)
                except Exception:
                    html = page.content()
                    if "captcha" in html.lower():
                        if page_num == 1 and not results:
                            return []
                        print(f"[yellowpages] captcha on page {page_num}; preserving prior rows and stopping pagination")
                        break

                    if page_num == 1 and not results:
                        return []

                    print(f"[yellowpages] no business-name selector on page {page_num}; preserving prior rows and stopping pagination")
                    break

                for _ in range(max_scrolls):
                    page.mouse.wheel(0, 3200)
                    page.wait_for_timeout(1200)

                html = page.content()
                print("YP HTML length:", len(html))

                if "captcha" in html.lower():
                    if page_num == 1 and not results:
                        return []
                    print(f"[yellowpages] captcha after render on page {page_num}; preserving prior rows and stopping pagination")
                    break

                parsed = _parse_cards(html, industry_tag=search_term)
                print(f"[yellowpages] page={page_num} parsed_rows={len(parsed)}")

                if not parsed:
                    if page_num == 1 and not results:
                        return []
                    print(f"[yellowpages] no parsed rows on page {page_num}; preserving prior rows and stopping pagination")
                    break

                for row in parsed:
                    key = (
                        row.get("source_id"),
                        row.get("raw_company_name"),
                        row.get("raw_address"),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(row)

            return results

        finally:
            browser.close()


def fetch_yellowpages_playwright(search_term: str, location: str) -> List[Dict]:
    return fetch_yellowpages_scraper(
        search_term=search_term,
        location=location,
        headless=True,
        max_pages=2,
        max_scrolls=3,
    )