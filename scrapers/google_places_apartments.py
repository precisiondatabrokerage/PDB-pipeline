import os
import time
import requests
import csv
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

# ===============================
# CSV EXPORTER
# ===============================
def export_to_csv(records, filename="apartments_knoxville.csv"):
    if not records:
        print("⚠️ No records found!")
        return
    
    keys = sorted(records[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)

    print(f"📄 CSV saved → {filename}")


# ===============================
# GOOGLE API QUERIES
# ===============================
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


# ===============================
# MAIN SCRAPER
# ===============================
def scrape_apartment_complexes():
    print("🏢 Scraping Knoxville apartment complexes...")

    knoxville_center = "35.9606,-83.9207"

    queries = [
        "apartment complex",
        "apartments",
        "multi-family housing",
        "apartment community",
        "luxury apartments",
        "apartment rental"
    ]

    all_results = []

    # Run all keyword variations
    for q in queries:
        print(f"📡 Searching: {q}")
        results = google_places_search(q, knoxville_center)
        print(f" → {len(results)} results")
        all_results.extend(results)

    print(f"📦 Total raw before dedupe: {len(all_results)}")

    # Deduplicate by place_id
    deduped = {}
    for r in all_results:
        deduped[r["place_id"]] = r

    print(f"🧹 Unique apartment complexes: {len(deduped)}")

    # Build CSV-ready output
    csv_records = []

    for pid, r in deduped.items():
        details = fetch_place_details(pid)

        name = r.get("name")
        address = details.get("formatted_address") or r.get("vicinity")
        phone = details.get("formatted_phone_number")
        website = details.get("website")

        loc = r.get("geometry", {}).get("location", {})
        lat = loc.get("lat")
        lng = loc.get("lng")

        csv_records.append({
            "place_id": pid,
            "name": name,
            "address": address,
            "phone": phone,
            "website": website,
            "lat": lat,
            "lng": lng,
            "rating": details.get("rating"),
            "reviews": details.get("user_ratings_total"),
        })

    export_to_csv(csv_records, "apartments_knoxville.csv")
    print("🎉 Apartment scrape completed!")


# =================================
# ENTRY POINT
# =================================
if __name__ == "__main__":
    scrape_apartment_complexes()
