from __future__ import annotations

import re
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SURFACE_HINT_RE = re.compile(
    r"(contact|about|team|staff|leadership|management|people|careers|jobs|employment|news|press|media|blog)",
    re.I,
)

DEFAULT_PATHS = [
    "/",
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/team",
    "/staff",
    "/our-team",
    "/leadership",
    "/management",
    "/people",
    "/careers",
    "/jobs",
    "/employment",
    "/news",
    "/press",
    "/media",
    "/blog",
]


def _get(url: str):
    try:
        return requests.get(
            url,
            timeout=10,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; PDBBot/1.0)"
            },
        )
    except Exception:
        return None


def _collect_internal_links(soup: BeautifulSoup, base: str):
    links = []
    seen = set()
    base_host = urlparse(base).netloc

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True)
        haystack = f"{href} {text}"

        if not SURFACE_HINT_RE.search(haystack):
            continue

        full = urljoin(base, href)
        parsed = urlparse(full)
        if parsed.netloc != base_host:
            continue

        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if normalized in seen:
            continue

        seen.add(normalized)
        links.append(normalized)

    return links


def scrape_company_site(url: str | None) -> dict:
    if not url:
        return {}

    if not url.startswith("http"):
        url = "https://" + url

    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

    queue = deque([urljoin(base, p) for p in DEFAULT_PATHS])
    visited = set()

    best_email = None
    best_contact_form_url = None
    best_footer_like_text = None
    final_status = None

    while queue and len(visited) < 10:
        target = queue.popleft()
        if target in visited:
            continue
        visited.add(target)

        resp = _get(target)
        if not resp:
            continue

        final_status = resp.status_code
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        emails = EMAIL_REGEX.findall(text)
        if emails and not best_email:
            best_email = emails[0]

        if not best_contact_form_url:
            if soup.find("form"):
                best_contact_form_url = target
            else:
                for a in soup.find_all("a", href=True):
                    t = a.get_text(" ", strip=True).lower()
                    if "contact" in t or "get in touch" in t:
                        href = a["href"]
                        best_contact_form_url = urljoin(base, href)
                        break

        footer = soup.find("footer")
        if footer and not best_footer_like_text:
            best_footer_like_text = footer.get_text(" ", strip=True)

        for link in _collect_internal_links(soup, base):
            if link not in visited and link not in queue:
                queue.append(link)

    return {
        "website_status": final_status if final_status is not None else "failed",
        "email_primary": best_email,
        "contact_form_url": best_contact_form_url,
        "physical_address_guess": best_footer_like_text,
    }