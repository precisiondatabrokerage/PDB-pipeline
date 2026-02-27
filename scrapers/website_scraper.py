import re
import requests
from bs4 import BeautifulSoup

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

def scrape_company_site(url: str | None) -> dict:
    """
    Scrapes a company website for contact info, address hints, and emails.
    """
    if not url:
        return {}

    try:
        resp = requests.get(url, timeout=10)
    except Exception:
        return {"website_status": "failed"}

    status = resp.status_code
    soup = BeautifulSoup(resp.text, "html.parser")

    # find emails anywhere on the page
    text = soup.get_text(" ", strip=True)
    emails = EMAIL_REGEX.findall(text)
    email = emails[0] if emails else None

    # find contact form
    contact_form_url = None
    for a in soup.find_all("a", href=True):
        t = a.get_text().lower()
        if "contact" in t or "get in touch" in t:
            href = a["href"]
            if href.startswith("http"):
                contact_form_url = href
            elif href.startswith("/"):
                # convert relative → absolute
                base = url.rstrip("/")
                contact_form_url = base + href
            break

    # attempt to grab footer address
    footer = soup.find("footer")
    footer_text = footer.get_text(" ", strip=True) if footer else None

    return {
        "website_status": status,
        "email_primary": email,
        "contact_form_url": contact_form_url,
        "physical_address_guess": footer_text
    }
