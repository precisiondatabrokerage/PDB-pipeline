import os
import time
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# Load environment variables
load_dotenv()

GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

# MongoDB connection
MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB")
MONGO_COLLECTION = os.getenv("MONGODB_COMPANY_COLLECTION")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

print("🚀 Multi-Industry Google Places Scraper started...")
print("MongoDB:", MONGO_URI, MONGO_DB, MONGO_COLLECTION)


# ======================================================
# CSV Export
# ======================================================
def export_to_csv(records, filename):
    if not records:
        print("⚠️ No records to write:", filename)
        return

    keys = sorted(records[0].keys())

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)

    print(f"📄 CSV saved → {filename}")


# ======================================================
# Google API Functions
# ======================================================
def google_places_search(query, location, radius=25000):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

    params = {
        "key": GOOGLE_PLACES_KEY,
        "keyword": query,
        "location": location,
        "radius": radius
    }

    results = []

    while True:
        res = requests.get(url, params=params)
        data = res.json()

        if "results" in data:
            results.extend(data["results"])

        next_page = data.get("next_page_token")
        if next_page:
            time.sleep(2)
            params["pagetoken"] = next_page
        else:
            break

    return results


def fetch_place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"

    params = {
        "key": GOOGLE_PLACES_KEY,
        "place_id": place_id,
        "fields": "website,formatted_address,formatted_phone_number,rating,user_ratings_total"
    }

    try:
        res = requests.get(url, params=params)
        return res.json().get("result", {})
    except:
        return {}


# ======================================================
# INDUSTRY DEFINITIONS
# ======================================================
industry_queries = {
    "property_management": [
        "property management",
        "property manager",
        "apartment management",
        "real estate management"
    ],
    "churches": [
        "church",
        "baptist church",
        "methodist church",
        "non-denominational church"
    ],
    "insurance_agents": [
        "insurance agency",
        "insurance agent",
        "commercial insurance"
    ],
    "commercial_real_estate": [
        "commercial real estate broker",
        "commercial realtor",
        "CRE broker"
    ]
}


# ======================================================
# MASTER SCRAPER
# ======================================================
def scrape_industry(industry_name, keywords):
    print(f"\n🔍 Scraping Industry: {industry_name}")

    knox_latlng = "35.9606,-83.9207"

    all_results = []

    # Fetch results for each keyword
    for q in keywords:
        print(f"📡 Searching: {q}")
        results = google_places_search(q, knox_latlng)
        print(f" → {len(results)} found")
        all_results.extend(results)

    print(f"📦 Total before dedupe: {len(all_results)}")

    # Dedupe by place_id
    deduped = {}
    for r in all_results:
        deduped[r["place_id"]] = r

    print(f"🧹 Unique businesses: {len(deduped)}")

    csv_records = []
    ops = []

    # Build records
    for pid, r in deduped.items():
        details = fetch_place_details(pid)

        name = r.get("name")
        address = details.get("formatted_address") or r.get("vicinity")
        phone = details.get("formatted_phone_number")
        website = details.get("website")
        loc = r.get("geometry", {}).get("location", {})
        lat = loc.get("lat")
        lng = loc.get("lng")

        # CSV
        csv_records.append({
            "industry_type": industry_name,
            "place_id": pid,
            "name": name,
            "address": address,
            "phone": phone,
            "website": website,
            "lat": lat,
            "lng": lng,
            "rating": details.get("rating"),
            "reviews": details.get("user_ratings_total")
        })

        # Mongo
        ops.append(UpdateOne(
            {"source": "google_places", "source_id": pid},
            {
                "$set": {
                    "source": "google_places",
                    "source_id": pid,
                    "industry_type": industry_name,
                    "scraped_at": datetime.utcnow(),
                    "raw_company_name": name,
                    "raw_address": address,
                    "raw_phone": phone,
                    "raw_website": website,
                    "lat": lat,
                    "lng": lng,
                    "google_rating": details.get("rating"),
                    "google_reviews_count": details.get("user_ratings_total"),
                    "raw_json": r
                }
            },
            upsert=True
        ))

    # Save CSV
    export_to_csv(csv_records, f"{industry_name}.csv")

    # Write to Mongo
    if ops:
        result = collection.bulk_write(ops)
        print(f"✅ Mongo: {result.upserted_count} inserted, {result.modified_count} updated")


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    for industry, keywords in industry_queries.items():
        scrape_industry(industry, keywords)

    print("\n🎉 All industries scraped successfully!")


