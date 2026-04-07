from __future__ import annotations

from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

__version__ = "4.0.0"

MAX_RESULTS = 30
SEARCH_URL = "https://www.bing.com/search"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

BLOCKED_DOMAINS = [
    "stackoverflow.com",
    "reddit.com",
    "github.com",
    "medium.com",
    "wikipedia.org",
    "bing.com",
    "microsoft.com",
    "youtube.com",
    "facebook.com",
    "linkedin.com",
]

BLOCKED_TITLE_PHRASES = [
    "what is",
    "how to",
    "difference",
    "guide",
    "tutorial",
    "stack overflow",
]

BUSINESS_HINTS = [
    "llc",
    "inc",
    "company",
    "services",
    "group",
    "management",
    "contractor",
    "electric",
    "plumbing",
    "roofing",
    "real estate",
    "insurance",
]


def _domain(url: Optional[str]) -> Optional[str]:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return None


def _is_business_result(title: str, url: str, snippet: str = "") -> bool:
    t = (title or "").lower()
    s = (snippet or "").lower()
    d = _domain(url) or ""

    if not d:
        return False

    if any(b in d for b in BLOCKED_DOMAINS):
        return False

    if any(p in t for p in BLOCKED_TITLE_PHRASES):
        return False

    haystack = f"{t} {s} {d}"

    if any(h in haystack for h in BUSINESS_HINTS):
        return True

    if d and "." in d and len(d.split(".")) >= 2:
        path = urlparse(url).path.strip("/")
        if not path:
            return True

    return False


def fetch_bing_serp(query: str, location: str) -> list[dict]:
    print(f"Bing web SERP: {query} ({location})")

    q = f"{query} {location}"
    url = f"{SEARCH_URL}?q={quote_plus(q)}&count={MAX_RESULTS}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li.b_algo")
        results = []
        rank = 0
        now = datetime.utcnow()

        for card in cards:
            if rank >= MAX_RESULTS:
                break

            link = card.select_one("h2 a")
            if not link:
                continue

            title = link.get_text(" ", strip=True)
            href = link.get("href")
            snippet_el = card.select_one(".b_caption p")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

            if not title or not href:
                continue

            if not _is_business_result(title, href, snippet):
                continue

            rank += 1
            results.append({
                "source": "bing_serp",
                "source_confidence": 0.75,
                "source_rank": rank,
                "raw_company_name": title,
                "raw_website": href,
                "domain": _domain(href),
                "raw_address": None,
                "raw_phone": None,
                "lat": None,
                "lng": None,
                "captured_at": now,
                "snippet": snippet,
            })

        print(f"BING kept {len(results)}/{len(cards)} for [{query}] [{location}]")
        return results

    except Exception as e:
        print(f"Bing skipped: {e}")
        return []