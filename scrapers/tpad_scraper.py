# PDB-pipeline/scrapers/tpad_scraper.py

import csv
from uuid import uuid4
from datetime import datetime, timezone

from db.mongo_client import get_mongo


def utcnow():
    return datetime.now(timezone.utc)


def ingest_tpad_csv(file_path: str, source_key: str = "tn_tpad") -> str:
    """
    Ingests a TPAD CSV bulk export into Mongo raw_property_records.
    Returns run_id.
    """

    mongo = get_mongo()

    run_id = str(uuid4())
    now = utcnow()

    raw_collection = mongo.db["raw_property_records"]

    inserted_count = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            raw_collection.insert_one({
                "source_key": source_key,
                "run_id": run_id,
                "captured_at": now,
                "raw_payload": row
            })
            inserted_count += 1

    # Log ingestion run
    mongo.ingestion_runs.insert_one({
        "run_id": run_id,
        "source_key": source_key,
        "started_at": now,
        "completed_at": utcnow(),
        "status": "completed",
        "record_count": inserted_count,
        "acquisition_method": "open_data_download"
    })

    print(f"✅ TPAD ingestion complete — {inserted_count} records inserted")
    print(f"Run ID: {run_id}")

    return run_id
