# pdb-pipeline/scrapers/tpad_parcel_detail.py
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Optional, Tuple, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


TPAD_BASE = "https://assessment.cot.tn.gov"
DETAIL_PATH = "/tpad/Parcel/Details"


def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.35,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; PDBParcelBot/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return s


_SESS = _session()

# Prefer full digit runs first to avoid truncation (e.g., 1940 -> 1940 not 194)
_INT_RE = re.compile(r"([0-9]+(?:,[0-9]{3})*)")
_MONEY_RE = re.compile(r"\$?\s*([0-9]+(?:,[0-9]{3})*)(?:\.[0-9]{2})?")


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _money_to_number(s: str) -> Optional[int]:
    if not s:
        return None
    m = _MONEY_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


def _int_from(s: str) -> Optional[int]:
    if not s:
        return None
    m = _INT_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


def _find_card(soup: BeautifulSoup, header_text: str) -> Optional[BeautifulSoup]:
    for h in soup.select(".card-header"):
        if _clean_text(h.get_text()) == header_text:
            return h.find_parent(class_="card")
    return None


def _split_code_desc(raw: str | None) -> Dict[str, Any]:
    """
    TPAD frequently uses patterns like:
      "01 - PUBLIC / PUBLIC"
      "8 - HEAT AND COOLING PKG"
      "AV - AVERAGE"
    Return {raw, code, desc} best-effort.
    """
    raw = _clean_text(raw or "")
    if not raw:
        return {"raw": None, "code": None, "desc": None}

    # split on " - " first
    if " - " in raw:
        left, right = raw.split(" - ", 1)
        left = _clean_text(left)
        right = _clean_text(right)
        code = left if left else None
        desc = right if right else None
        return {"raw": raw, "code": code, "desc": desc}

    return {"raw": raw, "code": None, "desc": raw}


def _extract_links(soup: BeautifulSoup) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    pdf = soup.select_one('a.pdf-button[href*="/tpad/Parcel/PrintPDF"]')
    if pdf and pdf.get("href"):
        out["pdf_path"] = pdf.get("href")
        out["pdf_url"] = TPAD_BASE + pdf.get("href")

    gis = soup.select_one('a[href*="tnmap.tn.gov/assessment/#/parcel/"]')
    if gis and gis.get("href"):
        out["gis_url"] = gis.get("href")

    # "Return to results" link can be useful for drillthrough context
    back = soup.select_one('a[href*="/tpad/Search?serializedParameters="]')
    if back and back.get("href"):
        href = back.get("href")
        out["search_back_path"] = href
        out["search_back_url"] = TPAD_BASE + href if href.startswith("/") else href

    return out


def _extract_county_information(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "County Information")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    out: Dict[str, Any] = {}
    txt = _clean_text(body.get_text(" ", strip=True))
    out["raw_text"] = txt

    for div in body.select(".col"):
        t = _clean_text(div.get_text(" ", strip=True))
        if ":" in t:
            k, v = t.split(":", 1)
            out[_clean_text(k)] = _clean_text(v)

    out_norm = {
        "county_number": _int_from(out.get("County Number", "")),
        "reappraisal_year": _int_from(out.get("Reappraisal Year", "")),
    }

    return {"raw": out, "parsed": out_norm}


def _extract_owner_mailing(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "Property Owner and Mailing Address")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    lines = [_clean_text(x.get_text()) for x in body.select(".col") if _clean_text(x.get_text())]
    out: Dict[str, Any] = {"raw_lines": lines}

    filtered = [
        l for l in lines
        if l.lower() not in {"january 1 owner"} and not l.lower().startswith("january 1 owner")
    ]
    if filtered:
        out["owner_name"] = filtered[0]
    if len(filtered) >= 2:
        out["mailing_street"] = filtered[1]
    if len(filtered) >= 3:
        out["mailing_city_state_zip"] = filtered[2]

    return out


def _extract_property_location(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "Property Location")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    txt = _clean_text(body.get_text(" ", strip=True))
    address = None
    for p in body.select("p.detailsPage"):
        t = _clean_text(p.get_text(" ", strip=True))
        if t.lower().startswith("address:"):
            address = _clean_text(t.split(":", 1)[1])
            break

    return {"address": address, "raw_text": txt}


def _extract_value_information(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "Value Information")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    out: Dict[str, Any] = {}
    for p in body.select("p.detailsPage strong"):
        label = _clean_text(p.get_text()).rstrip(":")
        row = p.find_parent(class_="col-8")
        val_txt = ""
        if row:
            sib = row.find_next_sibling(class_="col-4")
            if sib:
                val_txt = _clean_text(sib.get_text())
        if label:
            out[label] = val_txt

    out_num = {
        "land_market_value": _money_to_number(out.get("Land Market Value", "")),
        "improvement_value": _money_to_number(out.get("Improvement Value", "")),
        "total_market_appraisal": _money_to_number(out.get("Total Market Appraisal", "")),
        "assessment_percentage": _int_from(out.get("Assessment Percentage", "")),
        "assessment": _money_to_number(out.get("Assessment", "")),
    }
    return {"raw": out, "parsed": out_num}


def _extract_subdivision_data(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "Subdivision Data")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    out: Dict[str, Any] = {}
    p = body.select_one("p")
    if p:
        t = _clean_text(p.get_text(" ", strip=True))
        if ":" in t:
            k, v = t.split(":", 1)
            out[_clean_text(k)] = _clean_text(v)
        else:
            out["raw_line"] = t

    for span in body.select(".parcelSubdivisionElements"):
        t = _clean_text(span.get_text(" ", strip=True))
        if ":" in t:
            k, v = t.split(":", 1)
            out[_clean_text(k)] = _clean_text(v)

    parsed = {
        "plat_book": out.get("Plat Book"),
        "plat_page": out.get("Plat Page"),
        "block": out.get("Block"),
        "lot": out.get("Lot"),
        "subdivision": out.get("Subdivision"),
    }
    return {"raw": out, "parsed": parsed}


def _extract_general_information(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "General Information")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    out: Dict[str, Any] = {}
    for p in body.select("p.detailsPage"):
        t = _clean_text(p.get_text(" ", strip=True))
        if ":" in t:
            k, v = t.split(":", 1)
            out[_clean_text(k)] = _clean_text(v)

    # Normalize the utility fields you care about into a consistent structure
    utilities = {
        "water_sewer": _split_code_desc(out.get("Utilities - Water/Sewer")),
        "gas": _split_code_desc(out.get("Utilities - Gas/Gas Type")),
        "electricity": _split_code_desc(out.get("Utilities - Electricity")),
    }

    # Other intent/supporting context fields
    parsed = {
        "district": out.get("District"),
        "neighborhood": out.get("Neighborhood"),
        "number_of_buildings": _int_from(out.get("Number of buildings", "")),
        "utilities": utilities,
        "zoning": out.get("Zoning"),
    }

    return {"raw": out, "parsed": parsed}


def _extract_building_residential(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "Building Information - Residential")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    out: Dict[str, Any] = {}
    for p in body.select("p.detailsPage"):
        strong = p.select_one("strong")
        if not strong:
            continue
        k = _clean_text(strong.get_text()).rstrip(":")
        full = _clean_text(p.get_text(" ", strip=True))
        v = _clean_text(full.replace(strong.get_text(), "", 1)).lstrip(":").strip()
        if k:
            out[k] = v

    # Building Areas table
    areas: List[Dict[str, Any]] = []
    for table in body.select("table.table"):
        headers = [_clean_text(th.get_text()) for th in table.select("thead th")]
        if "Areas" in headers and "Square Feet" in headers:
            for tr in table.select("tbody tr"):
                tds = [_clean_text(td.get_text()) for td in tr.select("td")]
                if len(tds) >= 2:
                    areas.append({"area": tds[0], "sqft": _int_from(tds[1])})
            break
    if areas:
        out["Building Areas"] = areas

    # Building sketch image (first)
    sketch_img = None
    img = body.select_one("img.buildingSketch")
    if img and img.get("src"):
        sketch_img = img.get("src")

    stories = None
    if out.get("Stories"):
        try:
            stories = float(_clean_text(out["Stories"]).split()[0])
        except Exception:
            stories = None

    # --- Parsed fields (including your 12 “queryable” intent fields) ---
    parsed = {
        # existing useful
        "actual_year_built": _int_from(out.get("Actual Year Built", "")),
        "sqft_living_area": _int_from(out.get("Square Feet of Living Area", "")),
        "stories": stories,
        "building_sketch_url": sketch_img,

        # 12 queryable intent fields (best-effort raw + normalized)
        "roof_framing": _split_code_desc(out.get("Roof Framing")),
        "foundation": _split_code_desc(out.get("Foundation")),
        "heat_and_ac": _split_code_desc(out.get("Heat and AC")),
        "quality": _split_code_desc(out.get("Quality")),
        "condition": _split_code_desc(out.get("Condition")),
        "plumbing_fixtures": _int_from(out.get("Plumbing Fixtures", "")),

        "roof_cover_deck": _split_code_desc(out.get("Roof Cover/Deck")),
        # keep these in case you want them later
        "exterior_wall": _split_code_desc(out.get("Exterior Wall")),
    }

    return {"raw": out, "parsed": parsed}


def _extract_outbuildings(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    card = _find_card(soup, "Outbuildings & Yard Items")
    if not card:
        return []
    body = card.select_one(".card-body")
    if not body:
        return []

    table = body.select_one("table")
    if not table:
        return []

    rows: List[Dict[str, Any]] = []
    for tr in table.select("tbody tr"):
        tds = [_clean_text(x.get_text()) for x in tr.select("th,td")]
        if len(tds) >= 4:
            rows.append(
                {
                    "building_card": tds[0],
                    "type": tds[1],
                    "description": tds[2],
                    "area_units": _int_from(tds[3]) or tds[3],
                }
            )
    return rows


def _extract_sales(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    card = _find_card(soup, "Sale Information")
    if not card:
        return []
    body = card.select_one(".card-body")
    if not body:
        return []

    table = body.select_one("table")
    if not table:
        return []

    headers = [_clean_text(th.get_text()) for th in table.select("thead th")]
    rows: List[Dict[str, Any]] = []

    for tr in table.select("tbody tr"):
        tds = [_clean_text(td.get_text()) for td in tr.select("td")]
        if not tds:
            continue
        item: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            if i < len(tds):
                item[h] = tds[i]
        if "Price" in item:
            item["Price_num"] = _money_to_number(item["Price"])
        rows.append(item)

    return rows


def _extract_land_info(soup: BeautifulSoup) -> Dict[str, Any]:
    card = _find_card(soup, "Land Information")
    if not card:
        return {}
    body = card.select_one(".card-body")
    if not body:
        return {}

    summary: Dict[str, Any] = {}
    for div in body.select(".row .col-lg-2, .row .col-lg-2.col-md-4"):
        t = _clean_text(div.get_text(" ", strip=True))
        if ":" in t:
            k, v = t.split(":", 1)
            summary[_clean_text(k)] = _clean_text(v)

    land_rows: List[Dict[str, Any]] = []
    table = body.select_one("table")
    if table:
        headers = [_clean_text(th.get_text()) for th in table.select("thead th")]
        for tr in table.select("tbody tr"):
            tds = [_clean_text(td.get_text()) for td in tr.select("td")]
            if not tds:
                continue
            item: Dict[str, Any] = {}
            for i, h in enumerate(headers):
                if i < len(tds):
                    item[h] = tds[i]
            land_rows.append(item)

    parsed = {
        "deed_acres": _int_from(summary.get("Deed Acres", "")),
        "calculated_acres": _int_from(summary.get("Calculated Acres", "")),
        "total_land_units": _int_from(summary.get("Total Land Units", "")),
    }

    return {"summary": summary, "rows": land_rows, "parsed": parsed}


def parse_parcel_details_html(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")

    county_tax = ""
    el = soup.select_one(".parcel-county-and-tax-year")
    if el:
        county_tax = _clean_text(el.get_text())

    tax_year = None
    m = re.search(r"Tax Year\s+(\d{4})", county_tax)
    if m:
        try:
            tax_year = int(m.group(1))
        except Exception:
            tax_year = None

    payload = {
        "page": {
            "title": _clean_text(soup.title.get_text()) if soup.title else None,
            "county_tax_year": county_tax or None,
            "tax_year": tax_year,
            "links": _extract_links(soup),
        },
        "county_information": _extract_county_information(soup),
        "subdivision_data": _extract_subdivision_data(soup),
        "owner_mailing": _extract_owner_mailing(soup),
        "property_location": _extract_property_location(soup),
        "value_information": _extract_value_information(soup),
        "general_information": _extract_general_information(soup),
        "building_residential": _extract_building_residential(soup),
        "outbuildings": _extract_outbuildings(soup),
        "sale_information": _extract_sales(soup),
        "land_information": _extract_land_info(soup),
    }
    return payload


def fetch_parcel_details_html(parcel_id: str, jur: str, timeout: int = 20) -> Tuple[Optional[str], Optional[int]]:
    params = {"parcelId": parcel_id, "jur": jur, "parcelKey": f"{jur}{parcel_id}"}
    url = f"{TPAD_BASE}{DETAIL_PATH}"
    try:
        r = _SESS.get(url, params=params, timeout=timeout, allow_redirects=True)
        return (r.text if r.status_code == 200 else None), r.status_code
    except Exception:
        return None, None


def stable_hash(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()