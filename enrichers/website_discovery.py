# enrichers/website_discovery.py
"""
Website discovery (pipeline side).

Goal:
- Produce a reliable website/domain for directory-style records
- Avoid search engines
- Prefer extraction from YellowPages listing card and/or listing detail page
- Fail-soft: never crash pipeline

Returned fields:
- raw_website: str | None
- domain: str | None
- website_discovery: { method, confidence, detail_url_used, notes }
"""

from __future__ import annotations

import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote

__version__ = "1.0.0"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {"User-Agent": UA}

BAD_DOMAINS = {
    "yellowpages.com",
    "www.yellowpages.com",
    "yelp.com",
    "www.yelp.com",
    "facebook.com",
    "www.facebook.com",
    "linkedin.com",
    "www.linkedin.com",
    "mapquest.com",
    "www.mapquest.com",
    "bbb.org",
    "www.bbb.org",
}

def _extract_domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        u = url.strip()
        if not u:
            return None
        # allow raw domains like "example.com"
        if "://" not in u and "/" not in u:
            u = "https://" + u
        host = urlparse(u).netloc.lower().strip()
        host = host.replace("www.", "", 1)
        return host or None
    except Exception:
        return None

def _is_bad_domain(domain: str | None) -> bool:
    if not domain:
        return True
    d = domain.lower().strip()
    return (d in BAD_DOMAINS) or any(d.endswith("." + bd.replace("www.", "")) for bd in BAD_DOMAINS)

def _clean_redirect_url(url: str) -> str:
    """
    YellowPages sometimes uses redirect wrappers.
    Try to unwrap common patterns.
    """
    try:
        u = url.strip()
        if not u:
            return u

        parsed = urlparse(u)
        q = parse_qs(parsed.query)

        # common param names seen in redirect wrappers
        for key in ("url", "u", "dest", "destination", "redirect"):
            if key in q and q[key]:
                return unquote(q[key][0])

        return u
    except Exception:
        return url

def _normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip()
    if not u:
        return None
    u = _clean_redirect_url(u)

    # strip tracking fragments
    u = re.sub(r"#.*$", "", u)

    # fix protocol-relative
    if u.startswith("//"):
        u = "https:" + u

    # if raw domain
    if "://" not in u and "." in u and " " not in u:
        u = "https://" + u

    return u

def discover_website_from_yp_detail(detail_url: str, timeout: int = 20) -> dict:
    """
    Requests-based fallback to grab website from YellowPages listing detail page.
    This is optional and may be blocked sometimes; fail-soft.
    """
    out = {
        "raw_website": None,
        "domain": None,
        "website_discovery": {
            "method": "yp_detail_requests",
            "confidence": 0.0,
            "detail_url_used": detail_url,
            "notes": None,
        },
    }

    try:
        resp = requests.get(detail_url, headers=HEADERS, timeout=timeout)
        if resp.status_code >= 400:
            out["website_discovery"]["notes"] = f"http_{resp.status_code}"
            return out

        html = resp.text or ""
        if "captcha" in html.lower():
            out["website_discovery"]["notes"] = "captcha"
            return out

        soup = BeautifulSoup(html, "html.parser")

        # selectors that frequently carry the outbound site link
        candidates = []

        # common: a with class "primary-btn website-link" or similar
        for a in soup.select('a[href^="http"]'):
            href = a.get("href") or ""
            txt = (a.get_text(" ", strip=True) or "").lower()

            # strong signal if text implies website
            if "website" in txt or "visit website" in txt:
                candidates.append(href)

        # sometimes: "Website" is in a dedicated section
        for a in soup.select('a.track-visit-website[href]'):
            candidates.append(a.get("href"))

        # choose first non-bad domain
        for href in candidates:
            u = _normalize_url(href)
            d = _extract_domain(u)
            if not d or _is_bad_domain(d):
                continue
            out["raw_website"] = u
            out["domain"] = d
            out["website_discovery"]["confidence"] = 0.70
            return out

        out["website_discovery"]["notes"] = "no_outbound_link_found"
        return out

    except Exception as e:
        out["website_discovery"]["notes"] = f"error:{type(e).__name__}"
        return out

def discover_website(record: dict) -> dict:
    """
    Pipeline-level website discovery for a single raw record.

    Inputs expected (best-effort):
    - record.raw_website (optional)
    - record.source_id (YP listing path like "/knoxville-tn/mip/...")
    - record.detail_url (optional)
    """
    # 1) If scraper already extracted a website, validate and return.
    existing = _normalize_url(record.get("raw_website"))
    if existing:
        d = _extract_domain(existing)
        if d and not _is_bad_domain(d):
            return {
                "raw_website": existing,
                "domain": d,
                "website_discovery": {
                    "method": "yp_card",
                    "confidence": 0.80,
                    "detail_url_used": None,
                    "notes": None,
                },
            }

    # 2) Try detail URL if present; else build from source_id if it looks like a YP listing path.
    detail_url = record.get("detail_url")
    if not detail_url:
        sid = record.get("source_id") or ""
        if isinstance(sid, str) and sid.startswith("/"):
            detail_url = "https://www.yellowpages.com" + sid

    if detail_url:
        return discover_website_from_yp_detail(detail_url)

    # 3) No website discovered
    return {
        "raw_website": None,
        "domain": None,
        "website_discovery": {
            "method": "none",
            "confidence": 0.0,
            "detail_url_used": None,
            "notes": "no_source_id_or_detail_url",
        },
    }
