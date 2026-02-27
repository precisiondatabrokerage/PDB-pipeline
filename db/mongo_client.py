import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING, DESCENDING

@dataclass
class MongoCollections:
    client: MongoClient
    db: any
    raw_businesses: any
    ingestion_runs: any

def utcnow():
    return datetime.now(timezone.utc)

def get_mongo() -> MongoCollections:
    uri = os.getenv("MONGODB_URI")
    dbname = os.getenv("MONGODB_DB", "pdb_raw")
    if not uri:
        raise RuntimeError("MONGODB_URI not set")

    client = MongoClient(uri)
    db = client[dbname]

    raw_businesses = db["raw_businesses"]
    ingestion_runs = db["ingestion_runs"]

    # lightweight indexes (safe to call repeatedly)
    raw_businesses.create_index([("run_id", ASCENDING)])
    raw_businesses.create_index([("captured_at", DESCENDING)])
    raw_businesses.create_index([("source", ASCENDING), ("source_id", ASCENDING)])
    ingestion_runs.create_index([("run_id", ASCENDING)], unique=True)
    ingestion_runs.create_index([("started_at", DESCENDING)])

    return MongoCollections(
        client=client,
        db=db,
        raw_businesses=raw_businesses,
        ingestion_runs=ingestion_runs,
    )
