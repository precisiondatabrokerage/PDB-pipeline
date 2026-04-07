from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright

__version__ = "v6-playwright-search-text"

BASE_URL = "https://www.yelp.com"
SEARCH_URL = f"{BASE_URL}/search"

SEARCH_GROUPS = {
    "property_management": ["Property Management", "Property Manager"],
    "commercial_real_estate": ["Commercial Real Estate", "Commercial Real Estate Brokers"],
    "insurance_agents": ["Insurance Agents", "Insurance Agency", "Business Insurance"],
    "contractors": ["Contractors", "General Contractors"],
    "plumbing": ["Plumbers", "Plumbing Services"],
    "electrical": ["Electricians", "Electrical Contractors"],
    "roofing": ["Roofing", "Roofers"],
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

STAR_LINE_RE = re.compile(r"^\s*(\d\.\d)\s*\((\d[\d,]*)\s+reviews?\)\s*$", re.I)
REVIEWS_ONLY_RE = re.compile(r"^\s*(\d[\d,]*)\s+reviews?\s*$", re.I)
ADDRESS_RE = re.compile(
    r"^\s*\d{1,6}\s+[A-Za-z0-9 .'\-#&]+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Ln|Lane|Blvd|Boulevard|Pike|Way|Ct|Court|Cir|Circle|Pl|Place|Hwy|Highway)\b.*$",
    re.I,
)

KNOWN_CATEGORY_LINES = {
    "Plumbing",
    "Water Heater Installation/Repair",
    "Water Purification Services",
    "Electricians",
    "General Contractors",
    "Roofing",
    "Property Management",
    "Commercial Real Estate",
    "Insurance",
    "Insurance Agency",
    "Insurance Agencies",
    "Real Estate Services",
    "Real Estate Agents",
}

BAD_NAME_PREFIXES = (
    "top 10 best",
    "sort:",
    "filters",
    "get pricing",
    "see portfolio",
    "yelp guaranteed",
    "response time",
    "excellent",
    "featured",
    "open now",
    "virtual consultations",
    "request a quote",
    "start a project",
    "write a review",
)

BAD_EXACT_LINES = {
    "Yelp",
    "Yelp for Business",
    "Plumbers",
    "Electricians",
    "Roofing",
    "General Contractors",
    "Home Services",
    "Get pricing & availability",
    "Emergency services",
    "Free estimates",
    "Locally owned & operated",
    "Certified professionals",
    "Family-owned & operated",
    "Veteran-owned & operated",
    "New on Yelp",
    "See Portfolio",
}


def _norm(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    value = re.sub(r"\s+", " ", text).strip()
    return value or None


def _looks_like_business_name(text: Optional[str]) -> bool:
    value = _norm(text)
    if not value:
        return False

    if value in BAD_EXACT_LINES:
        return False

    lower = value.lower()
    if lower.startswith(BAD_NAME_PREFIXES):
        return False

    if len(value) < 3:
        return False

    if ADDRESS_RE.match(value):
        return False

    if STAR_LINE_RE.match(value) or REVIEWS_ONLY_RE.match(value):
        return False

    if value.isdigit():
        return False

    return True


def _extract_rows_from_text_lines(
    text_lines: List[str],
    *,
    industry_type: str,
    search_term: str,
    max_results_per_term: int,
) -> List[Dict]:
    rows: List[Dict] = []
    seen = set()

    i = 0
    while i < len(text_lines):
        line = text_lines[i]

        if not _looks_like_business_name(line):
            i += 1
            continue

        rating = None
        review_count = None
        address = None
        categories: List[str] = []

        j = i + 1
        while j < min(i + 14, len(text_lines)):
            probe = text_lines[j]

            m = STAR_LINE_RE.match(probe)
            if m:
                try:
                    rating = float(m.group(1))
                except Exception:
                    rating = None
                try:
                    review_count = int(m.group(2).replace(",", ""))
                except Exception:
                    review_count = None
                j += 1
                continue

            m2 = REVIEWS_ONLY_RE.match(probe)
            if m2 and review_count is None:
                try:
                    review_count = int(m2.group(1).replace(",", ""))
                except Exception:
                    review_count = None
                j += 1
                continue

            if ADDRESS_RE.match(probe) and address is None:
                address = probe
                j += 1
                continue

            if probe in KNOWN_CATEGORY_LINES:
                categories.append(probe)
                j += 1
                continue

            if _looks_like_business_name(probe) and j > i + 1:
                break

            j += 1

        if rating is not None or address is not None or categories:
            key = (line, address, industry_type)
            if key not in seen:
                seen.add(key)
                rows.append(
                    {
                        "source": "yelp",
                        "source_confidence": 0.80,
                        "raw_company_name": line,
                        "raw_address": address,
                        "raw_phone": None,
                        "raw_website": None,
                        "google_reviews_rating": rating,
                        "google_reviews_count": review_count,
                        "lat": None,
                        "lng": None,
                        "scraped_at": datetime.utcnow().isoformat(),
                        "source_url": None,
                        "industry_type": industry_type,
                        "search_term": search_term,
                        "yelp_categories": categories,
                    }
                )
                if len(rows) >= max_results_per_term:
                    break

        i = j if j > i else i + 1

    return rows


def _parse_search_page(page, *, industry_type: str, search_term: str, max_results_per_term: int) -> List[Dict]:
    body_text = page.locator("body").inner_text(timeout=10000)
    text_lines = []
    for line in body_text.splitlines():
        value = _norm(line)
        if value:
            text_lines.append(value)

    return _extract_rows_from_text_lines(
        text_lines,
        industry_type=industry_type,
        search_term=search_term,
        max_results_per_term=max_results_per_term,
    )


def fetch_yelp_data(
    location: str,
    *,
    search_groups: Optional[Dict[str, List[str]]] = None,
    max_results_per_term: int = 10,
    headless: bool = True,
) -> List[Dict]:
    print(f"Yelp scrape for location: {location}")
    groups = search_groups or SEARCH_GROUPS
    results: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        try:
            for industry_type, terms in groups.items():
                for term in terms:
                    url = f"{SEARCH_URL}?find_desc={quote_plus(term)}&find_loc={quote_plus(location)}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=45000)
                        page.wait_for_timeout(3500)
                        rows = _parse_search_page(
                            page,
                            industry_type=industry_type,
                            search_term=term,
                            max_results_per_term=max_results_per_term,
                        )
                        if rows:
                            results.extend(rows)
                    except Exception as e:
                        print(f"Yelp term skipped [{term}] [{location}]: {type(e).__name__}")
                        continue
        finally:
            browser.close()

    deduped: Dict[str, Dict] = {}
    for row in results:
        key = f"{row.get('raw_company_name')}|{row.get('raw_address')}|{row.get('industry_type')}"
        deduped[key] = row

    return list(deduped.values())


if __name__ == "__main__":
    rows = fetch_yelp_data("Maryville, TN", max_results_per_term=5, headless=False)
    print(f"rows={len(rows)}")
    print(rows[:5])