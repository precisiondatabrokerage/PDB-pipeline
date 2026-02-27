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
MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB")
MONGO_COLLECTION = os.getenv("MONGODB_COMPANY_COLLECTION")

print("MONGODB_URI =", MONGODB_URI)
print("MONGO_DB =", MONGO_DB)
print("MONGO_COLLECTION =", MONGO_COLLECTION)

client = MongoClient(MONGODB_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

print("🚀 google_places_companies.py started...")


# ======================================================
# CSV Export Helper
# ======================================================
def export_to_csv(records, filename="google_places_export.csv"):
    """Writes final deduped business records to a CSV file."""
    if not records:
        print("⚠️ No records to write to CSV.")
        return

    keys = sorted(list(records[0].keys()))

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)

    print(f"📄 CSV export complete! File saved as: {filename}")


# ======================================================
# Google API Functions
# ======================================================
def google_places_search(query, location, radius=25000):
    """
    Calls Google Places Nearby Search with pagination.
    """
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
            time.sleep(2)  # Required delay
            params["pagetoken"] = next_page
        else:
            break

    return results


def fetch_place_details(place_id):
    """
    Fetches website, phone, and address fields using Place Details API.
    """
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
# Main Scraper Logic
# ======================================================
def scrape_knoxville_property_managers():
    print("🔍 Starting Google Places scrape for Knoxville Metro property managers...")

    # Knoxville city center
    knox_latlng = "35.9606,-83.9207"

    queries = [
        "property management",
        "property manager",
        "apartment management",
        "real estate management",
    ]

    all_results = []

    # ---- FETCH ALL RESULTS ----
    for q in queries:
        print(f"📡 Searching Google Places for: {q} ...")
        results = google_places_search(query=q, location=knox_latlng)
        print(f" → Found {len(results)} results")
        all_results.extend(results)

    print(f"📦 Total raw Google Places results before dedupe: {len(all_results)}")

    # ---- DEDUPE ----
    deduped = {}
    for r in all_results:
        deduped[r["place_id"]] = r

    print(f"🧹 After dedupe: {len(deduped)} unique businesses.")

    # ======================================================
    # BUILD CSV OUTPUT LIST
    # ======================================================
    csv_records = []

    # ======================================================
    # BUILD MONGO UPSERT OPERATIONS
    # ======================================================
    ops = []

    for pid, r in deduped.items():
        details = fetch_place_details(pid)

        name = r.get("name")
        address = details.get("formatted_address") or r.get("vicinity")
        phone = details.get("formatted_phone_number")
        website = details.get("website")

        loc = r.get("geometry", {}).get("location", {})
        lat = loc.get("lat")
        lng = loc.get("lng")

        # ---- CSV RECORD ----
        csv_records.append({
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

        # ---- MONGO UPSERT ----
        ops.append(
            UpdateOne(
                {"source": "google_places", "source_id": pid},
                {
                    "$set": {
                        "source": "google_places",
                        "source_id": pid,
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
            )
        )

    # ======================================================
    # WRITE CSV
    # ======================================================
    export_to_csv(csv_records, "knoxville_property_managers.csv")

    # ======================================================
    # WRITE MONGO (if connection works)
    # ======================================================
    if ops:
        try:
            result = collection.bulk_write(ops)
            print(f"✅ Upserted: {result.upserted_count}, Modified: {result.modified_count}")
        except Exception as e:
            print("❌ MongoDB write failed:", e)
    else:
        print("⚠️ No MongoDB operations created.")

    print("🎉 Google Places scraping completed.")


# ======================================================
# ENTRY POINT
# ======================================================
if __name__ == "__main__":
    scrape_knoxville_property_managers()
