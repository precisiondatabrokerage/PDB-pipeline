from __future__ import annotations

import re
from typing import Dict, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

SEARCH_URL = "https://tnbear.tn.gov/Ecommerce/FilingSearch.aspx"

LABEL_MAP = {
    "status": "business_status",
    "current status": "business_status",
    "filing date": "business_start_date",
    "formation date": "business_start_date",
    "initial filing date": "business_start_date",
    "entity type": "entity_type",
    "registered agent": "registered_agent",
}

TEXT_LABEL_PATTERNS = {
    "business_status": [
        r"current status\s*:?\s*([^\n]+)",
        r"status\s*:?\s*([^\n]+)",
    ],
    "business_start_date": [
        r"initial filing date\s*:?\s*([^\n]+)",
        r"formation date\s*:?\s*([^\n]+)",
        r"filing date\s*:?\s*([^\n]+)",
    ],
    "registered_agent": [
        r"registered agent\s*:?\s*([^\n]+)",
    ],
    "entity_type": [
        r"entity type\s*:?\s*([^\n]+)",
    ],
}


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _first_visible(page, selectors):
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                return loc.first
        except Exception:
            continue
    return None


def _extract_table_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}

    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue

        label = _normalize_space(cells[0].get_text(" ", strip=True)).lower().rstrip(":")
        value = _normalize_space(cells[1].get_text(" ", strip=True))
        if not label or not value:
            continue

        if label in LABEL_MAP:
            out[LABEL_MAP[label]] = value

    return out


def _extract_text_patterns(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    for key, patterns in TEXT_LABEL_PATTERNS.items():
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.I)
            if m:
                value = _normalize_space(m.group(1))
                if value:
                    out[key] = value
                    break

    return out


def _parse_detail_html(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    data = {
        "business_status": None,
        "business_start_date": None,
        "registered_agent": None,
        "entity_type": None,
        "industry_attributes": {},
        "source_url": SEARCH_URL,
    }

    data.update(_extract_table_pairs(soup))
    data.update(_extract_text_patterns(page_text))

    return data


def fetch_business_registration(company_name: str) -> dict:
    company_name = (company_name or "").strip()
    if not company_name:
        return {
            "business_status": None,
            "business_start_date": None,
            "registered_agent": None,
            "entity_type": None,
            "industry_attributes": {},
            "source_url": SEARCH_URL,
        }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(SEARCH_URL, timeout=45000)

            search_input = _first_visible(
                page,
                [
                    'input[type="text"]',
                    'input[name*="Search"]',
                    'input[id*="Search"]',
                    'input[name*="Name"]',
                    'input[id*="Name"]',
                ],
            )
            if search_input is None:
                browser.close()
                return {
                    "business_status": None,
                    "business_start_date": None,
                    "registered_agent": None,
                    "entity_type": None,
                    "industry_attributes": {},
                    "source_url": SEARCH_URL,
                }

            search_input.fill(company_name)

            submit = _first_visible(
                page,
                [
                    'input[type="submit"]',
                    'button[type="submit"]',
                    'input[value*="Search"]',
                    'button:has-text("Search")',
                    'a:has-text("Search")',
                ],
            )

            if submit is not None:
                submit.click()
            else:
                search_input.press("Enter")

            page.wait_for_timeout(2500)

            result_link = _first_visible(
                page,
                [
                    'a[href*="BusinessInformation"]',
                    'a[href*="BusinessInfo"]',
                    'a[href*="BusinessDetail"]',
                    'a[href*="FilingDetail"]',
                    "table a",
                    "gridview a",
                ],
            )

            if result_link is not None:
                try:
                    result_link.click()
                    page.wait_for_timeout(2500)
                except Exception:
                    pass

            html = page.content()
            browser.close()
            return _parse_detail_html(html)

    except Exception:
        return {
            "business_status": None,
            "business_start_date": None,
            "registered_agent": None,
            "entity_type": None,
            "industry_attributes": {},
            "source_url": SEARCH_URL,
        }