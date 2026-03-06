# pdb-pipeline/enrichers/company_entity_expansion_v1.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timezone


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


def normalize_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
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
    # prefer normal business domains over long subdomains
    parts = domain.split(".")
    if len(parts) <= 2:
        return 30
    if len(parts) == 3:
        return 15
    return 5


def pick_best_website(candidates: List[str]) -> Tuple[Optional[str], Optional[str], float]:
    """
    Returns (website_url, domain, confidence 0..1)
    """
    best_url = None
    best_domain = None
    best_score = -999

    for url in candidates or []:
        d = normalize_domain(url)
        if not d:
            continue
        s = score_candidate_domain(d)
        if s > best_score:
            best_score = s
            best_url = url
            best_domain = d

    if not best_domain:
        return None, None, 0.0

    # simple mapping to confidence bucket
    conf = 0.85 if best_score >= 25 else 0.70 if best_score >= 10 else 0.55
    return best_url, best_domain, conf


# ----------------------------
# SERP adapters (pluggable)
# ----------------------------

def _serp_query_brave(query: str) -> List[Dict[str, Any]]:
    """
    Adapter wrapper around your existing scraper.
    You will paste/align to actual function signature after we inspect brave_serp.py.
    Expected normalized output: list of {"url": "...", "title": "...", "snippet": "..."}.
    """
    from scrapers.brave_serp import brave_search  # <-- likely needs adjustment
    return brave_search(query)  # <-- likely needs adjustment


def _serp_query_ddg(query: str) -> List[Dict[str, Any]]:
    from scrapers.ddg_serp import ddg_search  # <-- likely needs adjustment
    return ddg_search(query)  # <-- likely needs adjustment


def _serp_query_bing(query: str) -> List[Dict[str, Any]]:
    from scrapers.bing_serp import bing_search  # <-- likely needs adjustment
    return bing_search(query)  # <-- likely needs adjustment


def _extract_urls_from_serp(results: List[Dict[str, Any]]) -> List[str]:
    urls: List[str] = []
    for r in results or []:
        u = r.get("url") or r.get("link") or r.get("href")
        if u and isinstance(u, str):
            urls.append(u)
    return urls


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
    v1: SERP-first website/domain discovery, optional listing confirm later.
    """
    q = canonical_name.strip()
    if county_hint:
        q = f"{q} {county_hint} TN"
    query = q

    sources_used: List[str] = []
    debug: Dict[str, Any] = {"query": query, "serp_urls": {}}
    url_candidates: List[str] = []

    # Brave
    try:
        brave = _serp_query_brave(query)
        sources_used.append("brave_serp")
        brave_urls = _extract_urls_from_serp(brave)
        debug["serp_urls"]["brave_serp"] = brave_urls[:10]
        url_candidates.extend(brave_urls)
    except Exception as e:
        debug["brave_error"] = str(e)

    # DDG
    try:
        ddg = _serp_query_ddg(query)
        sources_used.append("ddg_serp")
        ddg_urls = _extract_urls_from_serp(ddg)
        debug["serp_urls"]["ddg_serp"] = ddg_urls[:10]
        url_candidates.extend(ddg_urls)
    except Exception as e:
        debug["ddg_error"] = str(e)

    # Bing
    try:
        bing = _serp_query_bing(query)
        sources_used.append("bing_serp")
        bing_urls = _extract_urls_from_serp(bing)
        debug["serp_urls"]["bing_serp"] = bing_urls[:10]
        url_candidates.extend(bing_urls)
    except Exception as e:
        debug["bing_error"] = str(e)

    # Pick best website/domain from all candidates
    website, domain, conf = pick_best_website(url_candidates)

    # v1: no phone/address yet (hook point later: google_places/yelp/yp)
    return ExpansionResult(
        company_id=company_id,
        canonical_name=canonical_name,
        website=website,
        domain=domain,
        phone_primary=None,
        mailing_street=None,
        mailing_city=None,
        mailing_state=None,
        mailing_zip=None,
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

    # record run for routing/audit
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