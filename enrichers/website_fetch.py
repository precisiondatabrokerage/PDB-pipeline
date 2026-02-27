# enrichers/website_fetch.py

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PDB/1.0)"
}


def fetch_website_html(url: str | None, timeout: int = 15) -> dict:
    """
    Fetch raw HTML only.
    No parsing, no inference, no confidence.
    """

    if not url:
        return {
            "website_html": None,
            "website_status": None,
        }

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        return {
            "website_html": resp.text if resp.status_code == 200 else None,
            "website_status": resp.status_code,
        }
    except Exception:
        return {
            "website_html": None,
            "website_status": None,
        }
