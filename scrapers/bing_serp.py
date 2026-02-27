import feedparser
from datetime import datetime
from urllib.parse import urlparse

__version__ = "3.1.0"

MAX_RESULTS = 30

# Hard rejects — content sites, not businesses
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
    "news",
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
]


def _domain(url: str | None) -> str | None:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return None


def _is_business_result(title: str, url: str) -> bool:
    t = title.lower()
    d = _domain(url) or ""

    if any(b in d for b in BLOCKED_DOMAINS):
        return False

    if any(p in t for p in BLOCKED_TITLE_PHRASES):
        return False

    if any(h in t for h in BUSINESS_HINTS):
        return True

    # allow clean domains with no path
    if d and "/" not in urlparse(url).path.strip("/"):
        return True

    return False


def fetch_bing_serp(query: str, location: str) -> list[dict]:
    print(f"🔍 Bing SERP (RSS): {query} ({location})")

    q = f"{query} {location}"
    feed_url = f"https://www.bing.com/news/search?q={q.replace(' ', '+')}&format=rss"

    feed = feedparser.parse(feed_url)

    results = []
    rank = 0
    now = datetime.utcnow()

    for entry in feed.entries:
        if rank >= MAX_RESULTS:
            break

        title = entry.get("title", "").strip()
        link = entry.get("link")

        if not title or not link:
            continue

        if not _is_business_result(title, link):
            continue

        rank += 1

        results.append({
            "source": "bing_serp",
            "source_confidence": 0.75,
            "source_rank": rank,
            "raw_company_name": title,
            "raw_website": link,
            "domain": _domain(link),
            "raw_address": None,
            "raw_phone": None,
            "lat": None,
            "lng": None,
            "captured_at": now,
        })

    print(f"BING kept {len(results)}/{len(feed.entries)} for [{query}] [{location}]")
    return results
