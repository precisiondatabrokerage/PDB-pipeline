import os
import time
import csv
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ======================================================
# Load environment and set up Mongo
# ======================================================

load_dotenv()

GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_API_KEY")  # not used here, but fine if present

MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB")
MONGO_COLLECTION = os.getenv("MONGODB_COMPANY_COLLECTION")

print("MONGODB_URI =", MONGODB_URI)
print("MONGO_DB =", MONGO_DB)
print("MONGO_COLLECTION =", MONGO_COLLECTION)

mongo_client = MongoClient(MONGODB_URI)
mongo_db = mongo_client[MONGO_DB]
collection = mongo_db[MONGO_COLLECTION]

print("🚀 yellowpages_multi_industry.py started...")


# ======================================================
# Industry configuration
# ======================================================

INDUSTRIES = {
    "property_management": {
        "label": "Property Management",
        "queries": [
            "Property Management",
            "Property Manager",
            "Rental Property Management",
            "Apartment Management",
        ],
    },
    "insurance_agents": {
        "label": "Insurance Agents",
        "queries": [
            "Insurance Agents",
            "Insurance Agency",
            "Business Insurance",
            "Commercial Insurance",
        ],
    },
    "commercial_real_estate": {
        "label": "Commercial Real Estate",
        "queries": [
            "Commercial Real Estate",
            "Commercial Real Estate Brokers",
            "Commercial Property Management",
        ],
    },
    "churches": {
        "label": "Churches",
        "queries": [
            "Church",
            "Baptist Church",
            "Non-Denominational Church",
            "Christian Church",
        ],
    },
}

KNOXVILLE_LOCATION = "Knoxville, TN"


# ======================================================
# Helpers
# ======================================================

def export_to_csv(records, filename="yellowpages_knoxville_multi_industry.csv"):
    """Writes final deduped business records to a CSV file."""
    if not records:
        print("⚠️ No records to write to CSV.")
        return

    # Collect all keys across records to avoid KeyError
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    keys = sorted(list(all_keys))

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    print(f"📄 CSV export complete! File saved as: {filename}")


def extract_zip_from_address(address: str | None) -> str | None:
    if not address:
        return None
    # look for 5-digit zip
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", address)
    if m:
        return m.group(1)
    return None


# ======================================================
# YellowPages Scraper Functions
# ======================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_yellowpages(query: str, location: str, max_pages: int = 3) -> list[dict]:
    """
    Scrape YellowPages search results for a given query and location.
    Attempts up to max_pages of pagination.
    """
    base_url = "https://www.yellowpages.com/search"
    results = []

    for page in range(1, max_pages + 1):
        params = {
            "search_terms": query,
            "geo_location_terms": location,
            "page": page,
        }

        print(f"   → YellowPages GET {base_url} [query='{query}'] page={page}")
        resp = requests.get(base_url, params=params, headers=HEADERS, timeout=15)

        if resp.status_code != 200:
            print(f"   ⚠️ Non-200 status from YellowPages: {resp.status_code}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # Typical listing container: div with "result" or "v-card" class
        cards = soup.select("div.result, div.v-card")

        if not cards:
            # No more results / changed layout / got blocked
            if page == 1:
                print("   ⚠️ No business cards found on first page (structure may have changed).")
            break

        for card in cards:
            # Name and profile URL
            name_tag = card.select_one("a.business-name")
            if not name_tag:
                continue

            name = name_tag.get_text(strip=True)
            profile_href = name_tag.get("href")
            if not profile_href:
                continue

            # Full URL for the listing
            if profile_href.startswith("http"):
                profile_url = profile_href
            else:
                profile_url = "https://www.yellowpages.com" + profile_href

            # Phone (using .phones or any <div> with phones class)
            phone_tag = card.select_one("div.phones, p.phones, span.phones")
            phone = phone_tag.get_text(strip=True) if phone_tag else None

            # Address (p.adr is common)
            addr_tag = card.select_one("p.adr")
            if addr_tag:
                address = addr_tag.get_text(" ", strip=True)
            else:
                # Fallback: some cards have nested address spans
                street = card.select_one("span.street-address")
                locality = card.select_one("span.locality")
                parts = []
                if street:
                    parts.append(street.get_text(strip=True))
                if locality:
                    parts.append(locality.get_text(strip=True))
                address = ", ".join(parts) if parts else None

            # Website
            website_tag = card.select_one("a.track-visit-website, a.website-link")
            website = website_tag.get("href") if website_tag else None

            # Categories
            cat_tag = card.select_one("div.categories, div.info span.category, div.info-section .categories")
            categories = cat_tag.get_text(" | ", strip=True) if cat_tag else None

            # Build record
            results.append(
                {
                    "yp_profile_url": profile_url,
                    "name": name,
                    "address": address,
                    "phone": phone,
                    "website": website,
                    "categories": categories,
                }
            )

        # Be polite: small delay
        time.sleep(1.5)

    print(f"   → Found {len(results)} raw YellowPages results for query '{query}'")
    return results


# ======================================================
# Main Multi-Industry Scraper
# ======================================================

def run_yellowpages_multi_industry():
    print("🔍 Starting YellowPages scrape for Knoxville multi-industry companies...")

    all_records = []

    for industry_key, cfg in INDUSTRIES.items():
        label = cfg["label"]
        queries = cfg["queries"]

        print(f"\n🏷️ Industry: {industry_key} ({label})")

        for q in queries:
            print(f"📡 Searching YellowPages for: '{q}' in {KNOXVILLE_LOCATION} ...")
            try:
                records = fetch_yellowpages(q, KNOXVILLE_LOCATION, max_pages=3)
            except Exception as e:
                print(f"   ❌ Error during YellowPages fetch for '{q}': {e}")
                records = []

            # Tag with industry + query
            for r in records:
                r["industry_tag"] = industry_key
                r["industry_label"] = label
                r["search_query"] = q
                r["city"] = "Knoxville"
                r["state"] = "TN"
                r["zip"] = extract_zip_from_address(r.get("address"))
                r["discovery_source"] = "yellowpages"

            print(f"   → {len(records)} records after tagging for '{q}'")
            all_records.extend(records)

    print(f"\n📦 Total raw YellowPages results before dedupe: {len(all_records)}")

    # Deduplicate across all industries
    deduped = {}
    for r in all_records:
        # Use profile URL as primary dedupe key, fallback to name + address
        key = r.get("yp_profile_url") or f"{r.get('name')}|{r.get('address')}"
        deduped[key] = r

    final_records = list(deduped.values())
    print(f"🧹 After dedupe: {len(final_records)} unique businesses.")

    # ======================================================
    # CSV Export
    # ======================================================
    export_to_csv(final_records, "yellowpages_knoxville_multi_industry.csv")

    # ======================================================
    # MongoDB bulk upsert into rawCompanies
    # ======================================================
    ops = []
    for r in final_records:
        profile_url = r.get("yp_profile_url")

        doc = {
            "source": "yellowpages",
            "source_id": profile_url,
            "scraped_at": datetime.utcnow(),

            "industry_tag": r.get("industry_tag"),
            "industry_label": r.get("industry_label"),
            "search_query": r.get("search_query"),
            "discovery_source": r.get("discovery_source"),

            "raw_company_name": r.get("name"),
            "raw_address": r.get("address"),
            "raw_phone": r.get("phone"),
            "raw_email": None,
            "raw_website": r.get("website"),
            "raw_city": r.get("city"),
            "raw_state": r.get("state"),
            "raw_zip": r.get("zip"),

            "lat": None,
            "lng": None,

            "yellowpages_categories": r.get("categories"),
            "yellowpages_url": profile_url,

            "raw_json": None,
        }

        ops.append(
            UpdateOne(
                {"source": "yellowpages", "source_id": profile_url},
                {"$set": doc},
                upsert=True,
            )
        )

    if ops:
        try:
            result = collection.bulk_write(ops)
            print(f"✅ Upserted: {result.upserted_count}, Modified: {result.modified_count}")
        except Exception as e:
            print("❌ MongoDB bulk_write failed:", e)
    else:
        print("⚠️ No MongoDB operations created.")

    print("🎉 YellowPages multi-industry scraping completed.")


# ======================================================
# Entry point
# ======================================================

if __name__ == "__main__":
    run_yellowpages_multi_industry()
