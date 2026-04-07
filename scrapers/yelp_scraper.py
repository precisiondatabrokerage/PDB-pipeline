from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

__version__ = "v3-pipeline-safe"

BASE_URL = "https://www.yelp.com"

SEARCH_GROUPS = {
    "property_management": ["Property Management", "Property Manager"],
    "commercial_real_estate": ["Commercial Real Estate", "Commercial Real Estate Brokers"],
    "insurance_agents": ["Insurance Agents", "Insurance Agency", "Business Insurance"],
    "contractors": ["Contractors", "General Contractors"],
    "plumbing": ["Plumbers", "Plumbing Services"],
    "electrical": ["Electricians", "Electrical Contractors"],
    "roofing": ["Roofing", "Roofers"],
}

STAR_RE = re.compile(r"([0-9.]+)\s*star", re.I)
REVIEW_COUNT_RE = re.compile(r"(\d[\d,]*)\s+reviews?", re.I)
PHONE_RE = re.compile(r"(\+?1[\s\-\.]?)?(\(?\d{3}\)?)[\s\-\.]?(\d{3})[\s\-\.]?(\d{4})")


def _scroll(page, times=4):
    for _ in range(times):
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(1200)


def _normalize_yelp_website(href: Optional[str]) -> Optional[str]:
    if not href:
        return None

    if href.startswith("http"):
        return href

    if href.startswith("/biz_redir"):
        try:
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            if "url" in qs and qs["url"]:
                return unquote(qs["url"][0])
        except Exception:
            return None

    return None


def _safe_text(locator) -> Optional[str]:
    try:
        return locator.first.text_content().strip()
    except Exception:
        return None


def _safe_attr(locator, attr: str) -> Optional[str]:
    try:
        return locator.first.get_attribute(attr)
    except Exception:
        return None


def _extract_biz_urls(page) -> List[str]:
    urls = {}
    for link in page.locator('a[href^="/biz/"]').all():
        href = link.get_attribute("href")
        if not href:
            continue
        slug = href.split("?")[0]
        urls[slug] = BASE_URL + slug
    return list(urls.values())


def _extract_rating_from_page(page) -> Optional[float]:
    candidates = [
        _safe_attr(page.locator('[aria-label*="star rating"]'), "aria-label"),
        _safe_attr(page.locator('[aria-label*="stars"]'), "aria-label"),
        _safe_attr(page.locator('[role="img"][aria-label*="star"]'), "aria-label"),
    ]
    for c in candidates:
        if not c:
            continue
        m = STAR_RE.search(c)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None
    return None


def _extract_review_count_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    m = REVIEW_COUNT_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


def _extract_phone_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = PHONE_RE.search(text)
    if not m:
        return None
    return m.group(0).strip()


def _scrape_business(page, url: str) -> Optional[Dict]:
    page.goto(url, timeout=45000)
    page.wait_for_selector("h1", timeout=15000)

    name = _safe_text(page.locator("h1"))
    if not name:
        return None

    page_text = page.locator("body").inner_text(timeout=5000)

    website = (
        _normalize_yelp_website(_safe_attr(page.locator('a[href*="/biz_redir"]'), "href"))
        or _safe_attr(page.locator('a[rel="nofollow"][href^="http"]'), "href")
    )

    return {
        "source": "yelp",
        "source_confidence": 0.85,
        "raw_company_name": name,
        "raw_address": _safe_text(page.locator("address")) or _safe_text(page.locator('[data-testid="address"]')),
        "raw_phone": _safe_text(page.locator('a[href^="tel:"]')) or _extract_phone_from_text(page_text),
        "raw_website": website,
        "google_reviews_rating": _extract_rating_from_page(page),
        "google_reviews_count": _extract_review_count_from_text(page_text),
        "lat": None,
        "lng": None,
        "scraped_at": datetime.utcnow().isoformat(),
        "source_url": url,
    }


def fetch_yelp_data(
    location: str,
    *,
    search_groups: Optional[Dict[str, List[str]]] = None,
    headless: bool = True,
    max_results_per_term: int = 15,
) -> List[Dict]:
    results: List[Dict] = []
    seen = set()
    groups = search_groups or SEARCH_GROUPS

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()

            for industry, terms in groups.items():
                for term in terms:
                    search_url = f"{BASE_URL}/search?find_desc={term}&find_loc={location}"
                    page.goto(search_url, timeout=45000)
                    page.wait_for_selector("main", timeout=15000)
                    _scroll(page)

                    biz_urls = _extract_biz_urls(page)[:max_results_per_term]

                    for biz_url in biz_urls:
                        key = biz_url.split("?")[0]
                        if key in seen:
                            continue
                        seen.add(key)

                        data = _scrape_business(page, biz_url)
                        if not data:
                            continue

                        data["industry_type"] = industry
                        data["search_term"] = term
                        results.append(data)

            browser.close()

        deduped = {}
        for r in results:
            key = f"{r.get('raw_company_name')}|{r.get('raw_address')}"
            deduped[key] = r

        return list(deduped.values())

    except Exception as e:
        print(f"Yelp skipped: {e}")
        return []