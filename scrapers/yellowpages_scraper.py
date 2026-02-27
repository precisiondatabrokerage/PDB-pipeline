import os
import csv
import time
import random
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup


# ======================================================
# ENV + MongoDB Setup
# ======================================================
load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB")
MONGO_COLLECTION = os.getenv("MONGODB_COMPANY_COLLECTION")

print("MONGODB_URI =", MONGODB_URI)
print("MONGO_DB =", MONGO_DB)
print("MONGO_COLLECTION =", MONGO_COLLECTION)

client = MongoClient(MONGODB_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

print("🚀 YellowPages Playwright scraper starting…")


# ======================================================
# CONFIG
# ======================================================
LOCATION = "Knoxville, TN"

INDUSTRIES = {
    "property_management": [
        "Property Management",
        "Property Manager",
        "Rental Property Management",
        "Apartment Management"
    ],
    "insurance_agents": [
        "Insurance Agents",
        "Insurance Agency",
        "Business Insurance",
        "Commercial Insurance"
    ],
    "commercial_real_estate": [
        "Commercial Real Estate",
        "Commercial Real Estate Brokers",
        "Commercial Property Management"
    ],
    "churches": [
        "Church",
        "Baptist Church",
        "Non-Denominational Church",
        "Christian Church"
    ],
}

YELLOWPAGES_URL = "https://www.yellowpages.com/search"


# ======================================================
# ANTI-BOT SHIELD
# ======================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Mobile Safari/604.1",
]

PROXY_LIST = []  # optional


def random_proxy():
    if not PROXY_LIST:
        return None
    return {"server": random.choice(PROXY_LIST)}


def random_delay(min_s=1.6, max_s=4.8):
    time.sleep(random.uniform(min_s, max_s))


def is_blocked(html: str) -> bool:
    signals = [
        "px-captcha",
        "verify you are human",
        "are you a human",
        "/captcha",
        "distil",
        "access to this page has been denied",
    ]
    return any(s in html.lower() for s in signals)


def create_context(browser):
    """Generate a highly randomized browser profile."""
    return browser.new_context(
        viewport={
            "width": 1280 + random.randint(-60, 60),
            "height": 900 + random.randint(-60, 60),
        },
        user_agent=random.choice(USER_AGENTS),
        locale=random.choice(["en-US", "en-GB"]),
        timezone_id=random.choice([
            "America/New_York",
            "America/Chicago",
            "America/Denver",
            "America/Los_Angeles"
        ]),
        geolocation={"longitude": -83.92, "latitude": 35.96},
        permissions=["geolocation"],
        proxy=random_proxy(),
    )


def human_mouse(page):
    """Simulate natural mouse movements."""
    for _ in range(random.randint(3, 8)):
        x = random.randint(100, 900)
        y = random.randint(100, 700)
        page.mouse.move(x, y, steps=random.randint(5, 18))
        time.sleep(random.uniform(0.07, 0.25))


def human_scroll(page):
    """Simulate scrolling like a human."""
    for _ in range(random.randint(2, 5)):
        page.mouse.wheel(0, random.randint(300, 900))
        time.sleep(random.uniform(0.2, 0.5))


# ======================================================
# CSV Export
# ======================================================
def export_csv(records: list, filename: str):
    if not records:
        print(f"⚠️ No records to write for {filename}")
        return

    keys = sorted(records[0].keys())

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)

    print(f"📄 CSV saved: {filename}")


# ======================================================
# HTML Scraper
# ======================================================
def parse_yellowpages_html(html, industry_tag):
    soup = BeautifulSoup(html, "html.parser")

    business_cards = soup.select(".result, .v-card")
    results = []

    for card in business_cards:
        name_tag = card.select_one("a.business-name span")
        if not name_tag:
            continue

        name = name_tag.get_text(strip=True)

        link_tag = card.select_one("a.business-name")
        source_id = link_tag["href"] if link_tag else None

        phone_tag = card.select_one(".phones")
        phone = phone_tag.get_text(strip=True) if phone_tag else None

        address_tag = card.select_one(".street-address")
        address = address_tag.get_text(strip=True) if address_tag else None

        results.append({
            "source": "yellowpages",
            "industry": industry_tag,
            "raw_company_name": name,
            "raw_address": address,
            "raw_phone": phone,
            "raw_email": None,
            "raw_website": None,
            "lat": None,
            "lng": None,
            "source_id": source_id,
            "raw_json": None,
        })

    return results


# ======================================================
# Main Scraper
# ======================================================
def scrape_yellowpages():

    final_results = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)

        # create first randomized stealth context
        context = create_context(browser)
        page = context.new_page()

        stealth = Stealth()
        stealth.apply_stealth_sync(page)

        print("🔍 Starting YellowPages scrape…")

        for industry_tag, queries in INDUSTRIES.items():
            print(f"\n🏷️ Industry: {industry_tag}")

            for query in queries:
                print(f"📡 Searching YellowPages for: '{query}' in {LOCATION} …")

                params = f"?search_terms={query.replace(' ', '+')}&geo_location_terms={LOCATION.replace(' ', '+')}"
                full_url = YELLOWPAGES_URL + params

                # ==========================
                # REQUEST WITH BOT SHIELD
                # ==========================
                try:
                    page.goto(full_url, timeout=45000)
                    random_delay()

                    human_mouse(page)
                    human_scroll(page)

                    html = page.content()

                    if is_blocked(html):
                        print("🛑 BLOCKED — regenerating fingerprint...")
                        context.close()

                        context = create_context(browser)
                        page = context.new_page()
                        stealth.apply_stealth_sync(page)

                        # retry once
                        page.goto(full_url, timeout=45000)
                        random_delay()
                        html = page.content()

                        if is_blocked(html):
                            print("❌ STILL BLOCKED — skipping query")
                            continue

                    parsed = parse_yellowpages_html(html, industry_tag)
                    print(f" → {len(parsed)} results scraped for query '{query}'")

                    final_results.extend(parsed)

                except Exception as e:
                    print(f"❌ Error loading page: {e}")
                    continue

        browser.close()

    # ======================================================
    # Dedupe
    # ======================================================
    print(f"\n📦 Total raw YellowPages results: {len(final_results)}")

    deduped = {}
    for r in final_results:
        key = (r["source_id"], r["raw_company_name"])
        deduped[key] = r

    final = list(deduped.values())

    print(f"🧹 After dedupe: {len(final)} unique businesses")

    # ======================================================
    # Write CSV
    # ======================================================
    export_csv(final, "yellowpages_multi_industry.csv")

    # ======================================================
    # Write to MongoDB
    # ======================================================
    if final:
        ops = []
        for r in final:
            ops.append(
                UpdateOne(
                    {"source": "yellowpages", "source_id": r["source_id"]},
                    {"$set": r},
                    upsert=True
                )
            )

        result = collection.bulk_write(ops)
        print(f"✅ Mongo Upserted: {result.upserted_count}, Modified: {result.modified_count}")
    else:
        print("⚠️ No MongoDB operations created.")

    print("🎉 YellowPages Playwright scraping completed.")


# ======================================================
# ENTRY POINT
# ======================================================
if __name__ == "__main__":
    scrape_yellowpages()
