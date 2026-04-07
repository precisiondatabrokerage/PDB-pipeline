from __future__ import annotations

import re
from collections import deque
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"(\+?1[\s\-\.]?)?(\(?\d{3}\)?)[\s\-\.]?(\d{3})[\s\-\.]?(\d{4})")
SURFACE_HINT_RE = re.compile(
    r"(contact|about|team|staff|leadership|management|people|careers|jobs|employment|news|press|media|blog|articles|insights)",
    re.I,
)

DEFAULT_PATHS = [
    "/",
    "/contact",
    "/contact-us",
    "/contactus",
    "/about",
    "/about-us",
    "/company",
    "/our-story",
    "/who-we-are",
    "/team",
    "/our-team",
    "/staff",
    "/leadership",
    "/management",
    "/people",
    "/careers",
    "/jobs",
    "/employment",
    "/news",
    "/newsroom",
    "/press",
    "/media",
    "/blog",
    "/articles",
    "/insights",
]

_session = None


def _get_session():
    global _session
    if _session:
        return _session

    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; PDBBot/1.0)"
    })
    _session = session
    return session


def _norm(value: str | None):
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _canonicalize(url: str | None, base: str | None = None):
    if not url:
        return None
    full = urljoin(base, url) if base else url
    full, _ = urldefrag(full)
    parsed = urlparse(full)
    if parsed.scheme not in {"http", "https"}:
        return None
    path = parsed.path or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _get(url: str):
    try:
        return _get_session().get(url, timeout=10)
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

        if href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue

        if not SURFACE_HINT_RE.search(haystack):
            continue

        full = _canonicalize(href, base)
        if not full:
            continue

        parsed = urlparse(full)
        if parsed.netloc != base_host:
            continue

        if full in seen:
            continue

        seen.add(full)
        links.append(full)

    return links


def scrape_company_site(url: str | None) -> dict:
    if not url:
        return {}

    if not url.startswith("http"):
        url = "https://" + url

    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

    queue = deque()
    for path in DEFAULT_PATHS:
        full = _canonicalize(urljoin(base, path))
        if full:
            queue.append(full)

    visited = set()

    best_email = None
    best_phone = None
    best_contact_form_url = None
    best_footer_like_text = None
    final_status = None
    surface_urls = []

    while queue and len(visited) < 12:
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

        surface_urls.append(target)

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        emails = EMAIL_REGEX.findall(text)
        if emails and not best_email:
            best_email = emails[0].lower()

        if not best_phone:
            m = PHONE_REGEX.search(text)
            if m:
                best_phone = re.sub(r"\D", "", m.group(0))
                if len(best_phone) == 11 and best_phone.startswith("1"):
                    best_phone = best_phone[1:]

        if not best_contact_form_url:
            if soup.find("form"):
                best_contact_form_url = target
            else:
                for a in soup.find_all("a", href=True):
                    t = (a.get_text(" ", strip=True) or "").lower()
                    if "contact" in t or "get in touch" in t:
                        href = a["href"]
                        best_contact_form_url = urljoin(base, href)
                        break

        footer = soup.find("footer")
        if footer and not best_footer_like_text:
            best_footer_like_text = _norm(footer.get_text(" ", strip=True))

        for link in _collect_internal_links(soup, base):
            if link not in visited and link not in queue:
                queue.append(link)

    return {
        "website_status": final_status if final_status is not None else "failed",
        "email_primary": best_email,
        "phone_primary": best_phone,
        "contact_form_url": best_contact_form_url,
        "physical_address_guess": best_footer_like_text,
        "surface_urls": surface_urls[:12],
    }