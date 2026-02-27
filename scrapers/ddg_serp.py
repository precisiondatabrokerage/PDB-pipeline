# scrapers/ddg_serp.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse
import time
import random

__version__ = "3.0.0"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
}

BASE_URL = "https://lite.duckduckgo.com/lite/"

def _extract_domain(url):
    try:
        return urlparse(url).netloc.replace("www.", "")
    except:
        return None

def fetch_ddg_serp(query: str, location: str | None = None, max_results: int = 10) -> list[dict]:
    q = f"{query} {location}" if location else query
    print(f"🔍 DDG SERP (lite): {q}")

    try:
        resp = requests.get(
            BASE_URL,
            params={"q": q},
            headers=HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"⚠️ DDG lite failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    now = datetime.utcnow()
    results = []

    links = soup.select("a.result-link")

    for rank, a in enumerate(links[:max_results], start=1):
        href = a.get("href")
        title = a.get_text(strip=True)

        if not href or not title:
            continue

        results.append({
            "source": "duckduckgo_serp",
            "source_confidence": 0.65,
            "source_rank": rank,
            "raw_company_name": title,
            "raw_website": href,
            "domain": _extract_domain(href),
            "raw_address": None,
            "raw_phone": None,
            "lat": None,
            "lng": None,
            "scraped_at": now,
            "is_business": True,
        })

    time.sleep(random.uniform(0.8, 1.5))
    return results
