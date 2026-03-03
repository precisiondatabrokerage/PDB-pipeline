# =====================================================
# PDB-pipeline/scrapers/tpad_scraper.py
#
# TPAD CSV → Mongo Raw Layer
#
# - Accepts optional county parameter
# - Normalizes CSV column names into canonical raw_payload shape
# - Stores county in both ingestion_runs and raw_payload
# - Produces deterministic run_id
# =====================================================

import csv
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional

from db.mongo_client import get_mongo


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def utcnow():
    return datetime.now(timezone.utc)


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _normalize_row(row: dict, county: Optional[str]) -> dict:
    """
    Normalize TPAD CSV row into canonical raw_payload shape
    expected by downstream TPAD parser.
    """

    return {
        # Core ownership
        "owner_name": _clean(row.get("Owner")),
        "owner_name_raw": _clean(row.get("Owner")),
        "owner_type": None,  # resolved later in owner_parser

        # Property identifiers
        "parcel_id": _clean(row.get("Parcel ID")),
        "control_map": _clean(row.get("Control Map")),
        "group": _clean(row.get("Group")),
        "parcel": _clean(row.get("Parcel")),

        # Location
        "property_address": _clean(row.get("Property Address")),
        "subdivision": _clean(row.get("Subdivision")),
        "lot": _clean(row.get("Lot")),
        "county": county,

        # Classification
        "class": _clean(row.get("Class")),
        "special_interest": _clean(row.get("Special Interest")),

        # Transaction
        "sale_date": _clean(row.get("Sale Date")),

        # Raw copy retained for audit/debug
        "raw_csv_row": row,
    }


# --------------------------------------------------
# Main Ingestion
# --------------------------------------------------

def ingest_tpad_csv(
    file_path: str,
    source_key: str = "tn_tpad",
    county: Optional[str] = None,
) -> str:
    """
    Ingests a TPAD CSV bulk export into Mongo raw_property_records.
    Returns run_id.
    """

    mongo = get_mongo()

    run_id = str(uuid4())
    now = utcnow()

    raw_collection = mongo.db["raw_property_records"]
    ingestion_runs = mongo.db["ingestion_runs"]

    inserted_count = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            normalized_payload = _normalize_row(row, county)

            raw_collection.insert_one({
                "source_key": source_key,
                "run_id": run_id,
                "captured_at": now,
                "raw_payload": normalized_payload,
            })

            inserted_count += 1

    # Log ingestion run (router record for ETL)
    ingestion_runs.insert_one({
        "run_id": run_id,
        "source_key": source_key,
        "county": county,
        "started_at": now,
        "completed_at": utcnow(),
        "status": "completed",
        "record_count": inserted_count,
        "acquisition_method": "open_data_download",
    })

    print(f"TPAD ingestion complete — {inserted_count} records inserted")
    print(f"Run ID: {run_id}")

    return run_id