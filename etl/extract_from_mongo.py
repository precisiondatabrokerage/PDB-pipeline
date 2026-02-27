from datetime import datetime
from typing import List, Dict
from db.mongo_client import get_mongo


def fetch_raw_records_for_run(run_id: str) -> List[Dict]:
    """
    Fetch raw business records for a given ingestion run_id
    from Mongo Atlas (raw_businesses collection).

    Returns records in the same shape expected by normalize().
    """

    if not run_id:
        raise ValueError("run_id is required")

    mongo = get_mongo()
    raw_businesses = mongo.raw_businesses

    cursor = raw_businesses.find(
        {"run_id": run_id},
        {
            "_id": 0,
            "extracted": 1,
            "source": 1,
            "source_id": 1,
            "captured_at": 1,
        },
    )

    records = []

    for doc in cursor:
        extracted = doc.get("extracted") or {}

        record = {
            # identity
            "source": doc.get("source"),
            "source_id": doc.get("source_id"),
            "scraped_at": doc.get("captured_at", datetime.utcnow()),

            # extracted raw fields
            "raw_company_name": extracted.get("raw_company_name"),
            "raw_address": extracted.get("raw_address"),
            "raw_phone": extracted.get("raw_phone"),
            "raw_website": extracted.get("raw_website"),

            # geo
            "lat": extracted.get("lat"),
            "lng": extracted.get("lng"),

            # google-specific (optional)
            "google_reviews_rating": extracted.get("google_reviews_rating"),
            "google_reviews_count": extracted.get("google_reviews_count"),

            # keep full extracted payload for traceability
            "raw_json": extracted,
        }

        records.append(record)

    print(f"📥 Loaded {len(records)} raw records from Mongo for run_id={run_id}")
    return records
