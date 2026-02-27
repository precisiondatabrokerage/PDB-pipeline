import re
from datetime import datetime
from typing import Optional, Dict

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

BASE_URL = "https://www.bbb.org"
SEARCH_TIMEOUT_MS = 10_000
PROFILE_TIMEOUT_MS = 10_000

# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _clean_name(name: str) -> str:
    """
    Normalize business name for BBB search.
    """
    n = name.lower()
    n = re.sub(r"[^a-z0-9\s]", "", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def _looks_like_noise(name: str) -> bool:
    """
    Hard gate: block obvious non-business SERP junk.
    """
    blacklist = [
        "stack overflow",
        "wikipedia",
        "reddit",
        "github",
        "medium",
        "tutorial",
        "guide",
        "what is",
        "how to",
        "documentation",
        "docs",
        "error",
        "syntax",
        "property",
        "api",
        "c#",
        "typescript",
        "python",
        "php",
    ]
    lname = name.lower()
    return any(term in lname for term in blacklist)


# -------------------------------------------------
# Core scraper
# -------------------------------------------------

def fetch_bbb_profile(
    business_name: Optional[str],
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> Optional[Dict]:
    """
    SAFE BBB lookup.

    Guarantees:
    - Never raises
    - Never blocks pipeline
    - Returns None or structured payload
    """

    if not business_name:
        return None

    if _looks_like_noise(business_name):
        return {
            "bbb_found": False,
            "bbb_skipped": True,
            "skip_reason": "non_business_name",
        }

    query = _clean_name(business_name)
    if not query:
        return None

    search_url = f"{BASE_URL}/search"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            page = browser.new_page()

            # -----------------------------
            # Search BBB
            # -----------------------------
            page.goto(search_url, timeout=SEARCH_TIMEOUT_MS)
            page.wait_for_selector("input[name='search']", timeout=SEARCH_TIMEOUT_MS)

            page.fill("input[name='search']", query)
            page.keyboard.press("Enter")

            page.wait_for_timeout(3000)

            # Attempt to find first profile link
            profile_link = page.locator("a[href*='/profile/']").first

            if not profile_link.count():
                browser.close()
                return {
                    "bbb_found": False,
                    "bbb_searched": True,
                    "searched_name": business_name,
                }

            href = profile_link.get_attribute("href")
            if not href:
                browser.close()
                return {
                    "bbb_found": False,
                    "bbb_searched": True,
                }

            profile_url = href if href.startswith("http") else BASE_URL + href

            # -----------------------------
            # Load BBB profile
            # -----------------------------
            page.goto(profile_url, timeout=PROFILE_TIMEOUT_MS)
            page.wait_for_timeout(2000)

            def safe_text(selector):
                try:
                    return page.locator(selector).first.text_content().strip()
                except:
                    return None

            # Accreditation
            accreditation = safe_text("span:has-text('Accredited')")
            is_accredited = bool(accreditation)

            # Rating
            rating_text = safe_text("span[class*='LetterGrade']")
            rating = rating_text.strip() if rating_text else None

            # Complaints
            complaints_text = safe_text("p:has-text('complaints')")
            complaints = None
            if complaints_text:
                nums = re.findall(r"\d+", complaints_text)
                if nums:
                    complaints = int(nums[0])

            browser.close()

            return {
                "bbb_found": True,
                "bbb_profile_url": profile_url,
                "bbb_accredited": is_accredited,
                "bbb_rating": rating,
                "bbb_complaints": complaints,
                "bbb_checked_at": datetime.utcnow().isoformat(),
            }

    except PlaywrightTimeout:
        return {
            "bbb_found": False,
            "bbb_error": "timeout",
        }

    except Exception as e:
        return {
            "bbb_found": False,
            "bbb_error": str(e),
        }
