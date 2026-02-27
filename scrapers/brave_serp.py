import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse

__version__ = "1.0.0"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

BASE_URL = "https://search.brave.com/search"


def _extract_domain(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return None


def fetch_brave_serp(query: str, location: str, max_results: int = 30) -> list[dict]:
    """
    Brave HTML SERP scraper (FREE).
    Commercially biased, business-friendly results.
    """

    print(f"🔍 Brave SERP: {query} ({location})")

    q = f"{query} {location}"

    params = {
        "q": q,
        "source": "web",
    }

    try:
        resp = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"⚠️ Brave SERP failed [{q}]: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    results = []
    now = datetime.utcnow()
    rank = 0

    for item in soup.select("a.result-header"):
        href = item.get("href")
        title = item.get_text(strip=True)

        if not href or not title:
            continue

        rank += 1
        if rank > max_results:
            break

        results.append({
            "source": "brave_serp",
            "source_confidence": 0.75,
            "source_rank": rank,
            "raw_company_name": title,
            "raw_website": href,
            "domain": _extract_domain(href),
            "raw_address": None,
            "raw_phone": None,
            "lat": None,
            "lng": None,
            "captured_at": now,
        })

    print(f"BRAVE produced {len(results)} results for [{query}] [{location}]")
    return results
