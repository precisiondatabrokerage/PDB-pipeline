from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

SEARCH_URL = "https://tncab.tnsos.gov/business-entity-search"

RETURN_TEMPLATE = {
    "business_status": None,
    "business_start_date": None,
    "registered_agent": None,
    "entity_type": None,
    "industry_attributes": {},
    "source_url": SEARCH_URL,
    "matched_name": None,
    "control_number": None,
    "principal_address": None,
    "source_status": "unknown",
}

FIELD_PATTERNS = {
    "entity_type": re.compile(r"^Entity Type:\s*(.+)$", re.I),
    "business_status": re.compile(r"^Status:\s*(.+)$", re.I),
    "control_number": re.compile(r"^Control Number:\s*(.+)$", re.I),
    "business_start_date": re.compile(r"^Initial Filing Date:\s*(.+)$", re.I),
}


def _normalize_space(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _empty_result(status: str) -> dict:
    out = dict(RETURN_TEMPLATE)
    out["source_status"] = status
    return out


def _find_suffix(page) -> Optional[str]:
    ids = page.eval_on_selector_all(
        "input[id^='Name_']",
        "els => els.map(e => e.id)",
    )
    for val in ids:
        if isinstance(val, str) and val.startswith("Name_"):
            return val.split("Name_", 1)[1]
    return None


def _parse_detail_html(html: str, current_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    out = dict(RETURN_TEMPLATE)
    out["source_url"] = current_url or SEARCH_URL
    out["source_status"] = "parsed"

    details_root = soup.select_one("#KendoWindowLevel1 #business-details") or soup.select_one("#business-details")
    if details_root is None:
        out["source_status"] = "details_not_found"
        return out

    title = details_root.select_one("h2")
    if title:
        out["matched_name"] = _normalize_space(title.get_text(" ", strip=True)) or None

    for h4 in details_root.select("h4"):
        line = _normalize_space(h4.get_text(" ", strip=True))
        if not line:
            continue
        for key, rx in FIELD_PATTERNS.items():
            m = rx.match(line)
            if m:
                out[key] = _normalize_space(m.group(1)) or None
                break

    for col in details_root.select(".col-md-4, .col-md-6"):
        heading_el = col.select_one("h4")
        heading = _normalize_space(heading_el.get_text(" ", strip=True) if heading_el else "")
        lines = [
            _normalize_space(h.get_text(" ", strip=True))
            for h in col.select("h4")
            if _normalize_space(h.get_text(" ", strip=True))
        ]

        if heading.lower() == "registered agent":
            values = [x for x in lines[1:] if x.lower() != "registered agent"]
            out["registered_agent"] = ", ".join(values) if values else None
        elif heading.lower() == "principal office address":
            values = [x for x in lines[1:] if x.lower() != "principal office address"]
            out["principal_address"] = ", ".join(values) if values else None

    if not any([out["business_status"], out["entity_type"], out["control_number"], out["business_start_date"]]):
        out["source_status"] = "detail_fields_missing"

    return out


def fetch_business_registration(
    company_name: str,
    *,
    headless: bool = True,
    manual_wait_ms: int = 120000,
) -> dict:
    company_name = _normalize_space(company_name)
    if not company_name:
        return _empty_result("empty_query")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(viewport={"width": 1440, "height": 1100})
            page = context.new_page()

            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(2500)

            suffix = _find_suffix(page)
            if not suffix:
                browser.close()
                return _empty_result("widget_suffix_not_found")

            name_selector = f"#Name_{suffix}"
            page.locator(name_selector).fill(company_name)

            if not headless:
                print("")
                print("TN SOS manual step required:")
                print("1) Click the Cloudflare 'Verify you are human' box")
                print("2) Click Search")
                print("3) Click the correct Details button")
                print("4) Leave the browser open until parsing finishes")
                print("")

                try:
                    page.wait_for_selector("#KendoWindowLevel1 #business-details, #business-details", timeout=manual_wait_ms)
                except PlaywrightTimeoutError:
                    try:
                        page.wait_for_selector("button.k-grid-Details, .k-grid-table tbody tr", timeout=2000)
                        browser.close()
                        return _empty_result("details_not_opened")
                    except PlaywrightTimeoutError:
                        browser.close()
                        return _empty_result("results_not_found")
            else:
                browser.close()
                return _empty_result("manual_verification_required")

            html = page.content()
            current_url = page.url
            browser.close()
            return _parse_detail_html(html, current_url)

    except PlaywrightTimeoutError:
        return _empty_result("timeout")
    except Exception:
        return _empty_result("exception")