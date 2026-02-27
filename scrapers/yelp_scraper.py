import os
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

__version__ = "v2-business-only"

SEARCH_GROUPS = {
    "property_management": ["Property Management", "Property Manager"],
    "commercial_real_estate": ["Commercial Real Estate", "Commercial Real Estate Brokers"],
    "insurance_agents": ["Insurance Agents", "Insurance Agency", "Business Insurance"],
    "contractors_handymen": ["Contractors", "Handyman"],
    "plumbing": ["Plumbers", "Plumbing Services"],
    "electrical": ["Electricians", "Electrical Contractors"],
    "hvac": ["Heating & Air Conditioning", "HVAC"],
    "appliance_repair": ["Appliances and Repair", "Appliance Repair"],
    "roofing": ["Roofing", "Roofers"],
    "locksmiths": ["Locksmiths"],
    "painters": ["Painters", "Painting Contractors"],
    "landscaping": ["Landscaping", "Landscape Design"],
    "tree_services": ["Tree Services", "Tree Removal"],
    "nurseries_gardening": ["Nurseries & Gardening", "Garden Centers"],
    "home_cleaning": ["Home Cleaning", "House Cleaning"],
    "movers": ["Movers", "Moving Companies"],
    "furniture_stores": ["Furniture Stores"],
    "florists": ["Florists", "Flower Shops"],
}

BASE_URL = "https://www.yelp.com"


def _scroll(page, times=3):
    for _ in range(times):
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(1200)


def _extract_biz_urls(page):
    urls = {}
    for link in page.locator('a[href^="/biz/"]').all():
        href = link.get_attribute("href")
        if not href:
            continue
        slug = href.split("?")[0]
        urls[slug] = BASE_URL + slug
    return list(urls.values())


def _scrape_business(page, url):
    page.goto(url, timeout=30000)
    page.wait_for_selector("h1", timeout=10000)

    def safe_text(sel):
        try:
            return page.locator(sel).first.text_content().strip()
        except:
            return None

    def safe_attr(sel, attr):
        try:
            return page.locator(sel).first.get_attribute(attr)
        except:
            return None

    name = safe_text("h1")
    if not name:
        return None

    return {
        "source": "yelp",
        "source_confidence": 0.85,
        "raw_company_name": name,
        "raw_address": safe_text("address"),
        "raw_phone": safe_text('a[href^="tel:"]'),
        "raw_website": safe_attr('a[rel="nofollow"][href^="http"]', "href"),
        "google_reviews_rating": (
            float(safe_attr("span[aria-label*='star']", "aria-label").split()[0])
            if safe_attr("span[aria-label*='star']", "aria-label")
            else None
        ),
        "google_reviews_count": (
            int("".join(c for c in safe_text("span:has-text('reviews')") if c.isdigit()))
            if safe_text("span:has-text('reviews')")
            else None
        ),
        "lat": None,
        "lng": None,
        "scraped_at": datetime.utcnow(),
    }


def fetch_yelp_data(location: str) -> list[dict]:
    print(f"🔍 Yelp scrape for location: {location}")
    results = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            for industry, terms in SEARCH_GROUPS.items():
                for term in terms:
                    page.goto(
                        f"{BASE_URL}/search?find_desc={term}&find_loc={location}",
                        timeout=30000,
                    )
                    page.wait_for_selector("main", timeout=10000)
                    _scroll(page)

                    for biz_url in _extract_biz_urls(page):
                        data = _scrape_business(page, biz_url)
                        if not data:
                            continue
                        data["industry_type"] = industry
                        data["search_term"] = term
                        results.append(data)

            browser.close()

        deduped = {}
        for r in results:
            key = f"{r.get('raw_company_name')}|{r.get('raw_address')}"
            deduped[key] = r

        return list(deduped.values())

    except Exception as e:
        print(f"⚠️ Yelp skipped: {e}")
        return []
