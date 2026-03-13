# pdb-pipeline/enrichers/company_entity_expansion_v1.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timezone
import re


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# Normalization
# ----------------------------

FREE_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
}

BAD_HOST_SUFFIXES = {
    "facebook.com", "m.facebook.com", "linkedin.com", "instagram.com",
    "twitter.com", "x.com", "yelp.com", "yellowpages.com",
    "mapquest.com", "bing.com", "google.com",
}

BUSINESS_PATH_HINTS = {
    "/contact",
    "/about",
    "/services",
    "/team",
    "/our-team",
}

BAD_TITLE_HINTS = {
    "facebook",
    "linkedin",
    "instagram",
    "twitter",
    "x.com",
    "yelp",
    "yellow pages",
    "mapquest",
    "directory",
    "listing",
}

PHONE_RE = re.compile(r"(\+?1[\s\-\.]?)?(\(?\d{3}\)?)[\s\-\.]?(\d{3})[\s\-\.]?(\d{4})")


def normalize_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = str(url).strip()
    if not u:
        return None
    if "://" not in u:
        u = "https://" + u
    try:
        host = urlparse(u).hostname
    except Exception:
        return None
    if not host:
        return None
    host = host.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = str(url).strip()
    if not u:
        return None
    if "://" not in u:
        u = "https://" + u
    return u


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    m = PHONE_RE.search(str(phone))
    if not m:
        return None
    digits = re.sub(r"\D", "", m.group(0))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


def _canonicalize_name(name: Optional[str]) -> str:
    if not name:
        return ""
    s = str(name).strip().lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\b(llc|inc|corp|corporation|co|company|ltd|lp|llp|pllc|trust)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _token_overlap_score(a: str, b: str) -> int:
    sa = set(_canonicalize_name(a).split())
    sb = set(_canonicalize_name(b).split())
    if not sa or not sb:
        return 0
    inter = len(sa & sb)
    if inter == 0:
        return 0
    return int(round(100 * inter / max(1, min(len(sa), len(sb)))))


def score_candidate_domain(domain: str) -> int:
    """
    Higher is better. Reject obvious junk/social/listing hosts.
    """
    if not domain:
        return -999
    if domain in FREE_DOMAINS:
        return -999
    if any(domain.endswith(suf) for suf in BAD_HOST_SUFFIXES):
        return -50

    parts = domain.split(".")
    if len(parts) <= 2:
        return 30
    if len(parts) == 3:
        return 15
    return 5


def score_candidate_url(
    *,
    candidate_url: str,
    candidate_title: Optional[str],
    canonical_name: str,
) -> int:
    domain = normalize_domain(candidate_url)
    if not domain:
        return -999

    score = score_candidate_domain(domain)

    title = (candidate_title or "").strip().lower()
    if title:
        if any(bad in title for bad in BAD_TITLE_HINTS):
            score -= 20
        overlap = _token_overlap_score(canonical_name, candidate_title)
        score += overlap // 4

    parsed = urlparse(normalize_url(candidate_url) or candidate_url)
    path = (parsed.path or "").strip().lower()

    if path in {"", "/"}:
        score += 12
    elif any(path.startswith(hint) for hint in BUSINESS_PATH_HINTS):
        score += 4
    else:
        score -= 3

    return score


def pick_best_website(
    candidates: List[Dict[str, Any]],
    canonical_name: str,
) -> Tuple[Optional[str], Optional[str], float, Dict[str, Any]]:
    """
    Returns:
      (website_url, domain, confidence 0..1, debug)
    """
    best_url = None
    best_domain = None
    best_score = -999
    scored_rows: List[Dict[str, Any]] = []

    for item in candidates or []:
        url = item.get("url")
        title = item.get("title")
        domain = normalize_domain(url)
        if not domain:
            continue

        score = score_candidate_url(
            candidate_url=url,
            candidate_title=title,
            canonical_name=canonical_name,
        )

        scored_rows.append(
            {
                "url": url,
                "title": title,
                "domain": domain,
                "score": score,
                "source": item.get("source"),
            }
        )

        if score > best_score:
            best_score = score
            best_url = normalize_url(url)
            best_domain = domain

    debug = {
        "scored_candidates": scored_rows[:25],
        "best_score": best_score,
    }

    if not best_domain:
        return None, None, 0.0, debug

    conf = 0.90 if best_score >= 55 else 0.80 if best_score >= 40 else 0.65 if best_score >= 25 else 0.50
    return best_url, best_domain, conf, debug


# ----------------------------
# SERP adapters
# ----------------------------

def _serp_query_brave(query: str, county_hint: Optional[str]) -> List[Dict[str, Any]]:
    from scrapers.brave_serp import fetch_brave_serp

    location = f"{county_hint}, TN" if county_hint else "Tennessee"
    rows = fetch_brave_serp(query=query, location=location, max_results=10) or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "url": r.get("raw_website"),
                "title": r.get("raw_company_name"),
                "snippet": None,
                "source": "brave_serp",
            }
        )
    return out


def _serp_query_ddg(query: str, county_hint: Optional[str]) -> List[Dict[str, Any]]:
    from scrapers.ddg_serp import fetch_ddg_serp

    location = f"{county_hint}, TN" if county_hint else "Tennessee"
    rows = fetch_ddg_serp(query=query, location=location, max_results=10) or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "url": r.get("raw_website"),
                "title": r.get("raw_company_name"),
                "snippet": None,
                "source": "ddg_serp",
            }
        )
    return out


def _serp_query_bing(query: str, county_hint: Optional[str]) -> List[Dict[str, Any]]:
    from scrapers.bing_serp import fetch_bing_serp

    location = f"{county_hint}, TN" if county_hint else "Tennessee"
    rows = fetch_bing_serp(query=query, location=location) or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "url": r.get("raw_website"),
                "title": r.get("raw_company_name"),
                "snippet": None,
                "source": "bing_serp",
            }
        )
    return out


# ----------------------------
# Listing/contact adapters
# ----------------------------

def _google_places_lookup(canonical_name: str, county_hint: Optional[str]) -> Dict[str, Any]:
    """
    Best-effort place discovery using existing Google Places scraper helpers.
    Returns only lightweight extracted fields.
    """
    try:
        from scrapers.google_places import google_places_search, fetch_place_details
    except Exception:
        return {}

    try:
        # East TN biased fallback; this is enrichment, not canonical truth.
        # You can make this county-center aware later.
        location = "35.9606,-83.9207"
        rows = google_places_search(keyword=canonical_name, location=location, radius=25000) or []
        if not rows:
            return {}

        best = rows[0]
        place_id = best.get("place_id")
        details = fetch_place_details(place_id) if place_id else {}

        return {
            "website": details.get("website"),
            "phone_primary": normalize_phone(details.get("formatted_phone_number")),
            "mailing_address": details.get("formatted_address") or best.get("vicinity"),
            "google_reviews_rating": details.get("rating"),
            "google_reviews_count": details.get("user_ratings_total"),
            "source": "google_places",
        }
    except Exception:
        return {}


def _website_contact_lookup(website: Optional[str]) -> Dict[str, Any]:
    if not website:
        return {}

    try:
        from scrapers.website_scraper import scrape_company_site
    except Exception:
        return {}

    try:
        payload = scrape_company_site(website)
        return {
            "email_primary": payload.get("email_primary"),
            "contact_form_url": payload.get("contact_form_url"),
            "physical_address_guess": payload.get("physical_address_guess"),
            "website_status": payload.get("website_status"),
            "source": "website_scraper",
        }
    except Exception:
        return {}


def _split_address_lines(raw: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Very conservative one-line address splitter.
    """
    if not raw:
        return None, None, None, None

    s = " ".join(str(raw).strip().split())
    parts = [p.strip() for p in s.split(",") if p.strip()]

    street = parts[0] if parts else None
    city = None
    state = None
    zip_code = None

    if len(parts) >= 2:
        city = parts[1]

    if len(parts) >= 3:
        m = re.search(r"\b([A-Z]{2})\b(?:\s+(\d{5}(?:-\d{4})?))?", parts[2].upper())
        if m:
            state = m.group(1)
            zip_code = m.group(2)

    if not state:
        m = re.search(r"\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b", s.upper())
        if m:
            state = m.group(1)
            zip_code = m.group(2)

    return street, city, state, zip_code


# ----------------------------
# Main expansion
# ----------------------------

@dataclass
class ExpansionResult:
    company_id: int
    canonical_name: str
    website: Optional[str]
    domain: Optional[str]
    phone_primary: Optional[str]
    email_primary: Optional[str]
    contact_form_url: Optional[str]
    mailing_street: Optional[str]
    mailing_city: Optional[str]
    mailing_state: Optional[str]
    mailing_zip: Optional[str]
    source_confidence: float
    sources: List[str]
    debug: Dict[str, Any]


def expand_company_entity_v1(
    company_id: int,
    canonical_name: str,
    county_hint: Optional[str] = None,
) -> ExpansionResult:
    """
    v2:
    - real scraper wiring
    - SERP-first website/domain discovery
    - optional Google Places phone/address/website hint
    - optional website contact extraction for email/contact form
    """
    q = canonical_name.strip()
    if county_hint:
        q = f"{q} {county_hint} TN"
    query = q

    sources_used: List[str] = []
    debug: Dict[str, Any] = {"query": query, "serp": {}, "listing": {}, "website_contact": {}}
    serp_candidates: List[Dict[str, Any]] = []

    # Brave
    try:
        brave = _serp_query_brave(query, county_hint)
        if brave:
            sources_used.append("brave_serp")
        debug["serp"]["brave_serp"] = brave[:10]
        serp_candidates.extend(brave)
    except Exception as e:
        debug["brave_error"] = str(e)

    # DDG
    try:
        ddg = _serp_query_ddg(query, county_hint)
        if ddg:
            sources_used.append("ddg_serp")
        debug["serp"]["ddg_serp"] = ddg[:10]
        serp_candidates.extend(ddg)
    except Exception as e:
        debug["ddg_error"] = str(e)

    # Bing
    try:
        bing = _serp_query_bing(query, county_hint)
        if bing:
            sources_used.append("bing_serp")
        debug["serp"]["bing_serp"] = bing[:10]
        serp_candidates.extend(bing)
    except Exception as e:
        debug["bing_error"] = str(e)

    website, domain, conf, website_debug = pick_best_website(
        candidates=serp_candidates,
        canonical_name=canonical_name,
    )
    debug["website_pick"] = website_debug

    phone_primary = None
    email_primary = None
    contact_form_url = None
    mailing_street = None
    mailing_city = None
    mailing_state = None
    mailing_zip = None

    # Google Places enrichment
    # gp = _google_places_lookup(canonical_name=canonical_name, county_hint=county_hint)
    # if gp:
    #     sources_used.append("google_places")
    #     debug["listing"]["google_places"] = gp

    #     if not website and gp.get("website"):
    #         website = normalize_url(gp.get("website"))
    #         domain = normalize_domain(website)
    #         conf = max(conf, 0.78)

    #     phone_primary = normalize_phone(gp.get("phone_primary"))

    #     street, city, state, zip_code = _split_address_lines(gp.get("mailing_address"))
    #     mailing_street = street
    #     mailing_city = city
    #     mailing_state = state
    #     mailing_zip = zip_code

    # Website contact extraction
    wc = _website_contact_lookup(website)
    if wc:
        sources_used.append("website_scraper")
        debug["website_contact"] = wc

        email_primary = wc.get("email_primary")
        contact_form_url = wc.get("contact_form_url")

    # De-dup sources
    sources_used = sorted(set(sources_used))

    return ExpansionResult(
        company_id=company_id,
        canonical_name=canonical_name,
        website=website,
        domain=domain,
        phone_primary=phone_primary,
        email_primary=email_primary,
        contact_form_url=contact_form_url,
        mailing_street=mailing_street,
        mailing_city=mailing_city,
        mailing_state=mailing_state,
        mailing_zip=mailing_zip,
        source_confidence=conf,
        sources=sources_used,
        debug=debug,
    )


def write_company_expansions_to_mongo(
    *,
    parent_run_id: str,
    source_key: str,
    expansions: List[ExpansionResult],
) -> int:
    """
    Writes raw expansion payloads for ETL application.
    """
    from db.mongo_client import get_mongo

    mongo = get_mongo()
    coll = mongo.db["raw_company_expansions"]

    now = utcnow()
    n = 0
    for ex in expansions or []:
        coll.insert_one(
            {
                "source_key": source_key,
                "parent_run_id": parent_run_id,
                "captured_at": now,
                "raw_payload": {
                    "company_id": ex.company_id,
                    "canonical_name": ex.canonical_name,
                    "website": ex.website,
                    "domain": ex.domain,
                    "phone_primary": ex.phone_primary,
                    "email_primary": ex.email_primary,
                    "contact_form_url": ex.contact_form_url,
                    "mailing_street": ex.mailing_street,
                    "mailing_city": ex.mailing_city,
                    "mailing_state": ex.mailing_state,
                    "mailing_zip": ex.mailing_zip,
                    "source_confidence": ex.source_confidence,
                    "sources": ex.sources,
                    "debug": ex.debug,
                },
            }
        )
        n += 1

    mongo.db["ingestion_runs"].insert_one(
        {
            "run_id": f"company_expansion::{parent_run_id}",
            "source_key": source_key,
            "parent_run_id": parent_run_id,
            "started_at": now,
            "completed_at": utcnow(),
            "status": "completed",
            "record_count": n,
            "acquisition_method": "serp_discovery",
        }
    )

    return n
