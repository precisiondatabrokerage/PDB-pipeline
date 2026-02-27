import os
import time
import math
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

from db.mongo_client import get_mongo

# ======================================================
# ENV
# ======================================================

load_dotenv()
GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
if not GOOGLE_PLACES_KEY:
    raise RuntimeError("GOOGLE_PLACES_API_KEY not set")

# ======================================================
# METRO GRID BUILDER
# ======================================================

def build_grid(center_lat, center_lng, grid_size=5, step_km=3):
    """
    Builds a grid of lat/lng points around a metro center.
    """
    points = []
    earth_radius = 6371  # km

    lat_step = (step_km / earth_radius) * (180 / math.pi)
    lng_step = (step_km / earth_radius) * (180 / math.pi) / math.cos(
        center_lat * math.pi / 180
    )

    half = grid_size // 2

    for i in range(-half, half + 1):
        for j in range(-half, half + 1):
            lat = center_lat + (i * lat_step)
            lng = center_lng + (j * lng_step)
            points.append(f"{lat},{lng}")

    return points


# Knoxville default grid (can be parameterized later)
GRID_POINTS = build_grid(35.9606, -83.9207, grid_size=5, step_km=3)

# ======================================================
# GOOGLE API CALLS
# ======================================================

def google_places_search(keyword, location, radius=3000):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "key": GOOGLE_PLACES_KEY,
        "keyword": keyword,
        "location": location,
        "radius": radius,
    }

    results = []

    while True:
        res = requests.get(url, params=params, timeout=20)
        data = res.json()

        results.extend(data.get("results", []))

        token = data.get("next_page_token")
        if token:
            time.sleep(2)
            params["pagetoken"] = token
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
        res = requests.get(url, params=params, timeout=20)
        return res.json().get("result", {})
    except Exception:
        return {}


# ======================================================
# INGESTION FUNCTION (ATLAS ONLY)
# ======================================================

def fetch_places(
    query: str,
    run_id: str,
    market: str = "Knoxville, TN",
    grid_points: list[str] | None = None,
) -> int:
    """
    Google Places ingestion → Mongo Atlas (raw_businesses).

    - Append-only
    - One document per observed business
    - Returns count written
    """

    mongo = get_mongo()
    raw_businesses = mongo.raw_businesses

    grid = grid_points or GRID_POINTS
    written = 0

    for point in grid:
        results = google_places_search(query, point)

        for r in results:
            place_id = r.get("place_id")
            if not place_id:
                continue

            details = fetch_place_details(place_id)

            address = details.get("formatted_address") or r.get("vicinity")
            phone = details.get("formatted_phone_number")
            website = details.get("website")

            loc = r.get("geometry", {}).get("location", {})
            lat = loc.get("lat")
            lng = loc.get("lng")

            doc = {
                "run_id": run_id,
                "source": "google_places",
                "source_id": place_id,
                "captured_at": datetime.now(timezone.utc),
                "market": market,
                "query": query,

                # Full raw payloads (never modified)
                "raw": {
                    "nearby_search": r,
                    "details": details,
                },

                # Minimal extracted fields for ETL convenience
                "extracted": {
                    "raw_company_name": r.get("name"),
                    "raw_address": address,
                    "raw_phone": phone,
                    "raw_website": website,
                    "lat": lat,
                    "lng": lng,
                    "google_reviews_rating": details.get("rating"),
                    "google_reviews_count": details.get("user_ratings_total"),
                }
            }

            raw_businesses.insert_one(doc)
            written += 1

    return written
