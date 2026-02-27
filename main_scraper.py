from dotenv import load_dotenv
load_dotenv()

import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

# =====================================================
# ENABLED SCRAPER
# =====================================================
from scrapers.yellowpages_playwright import fetch_yellowpages_playwright

# =====================================================
# Enrichers
# =====================================================
from enrichers.website_discovery import discover_website
from enrichers.website_fetch import fetch_website_html

# =====================================================
# Mongo
# =====================================================
from db.mongo_client import get_mongo


def utcnow():
    return datetime.now(timezone.utc)


def _app_env():
    return (os.getenv("APP_ENV") or "").strip().lower() or "production"


def _safe_source_id(name: Optional[str], phone: Optional[str], city: str) -> str:
    n = (name or "unknown").strip().lower()
    p = (phone or "").strip()
    return f"{n}|{p}|{city.lower()}"


def _scraper_versions():
    import scrapers.yellowpages_playwright as yp
    import enrichers.website_discovery as wd
    import enrichers.website_fetch as wf

    return {
        "yellowpages": getattr(yp, "__version__", "unknown"),
        "website_discovery": getattr(wd, "__version__", "unknown"),
        "website_fetch": getattr(wf, "__version__", "unknown"),
    }


def run_pipeline(trigger: str = "manual"):
    mongo = get_mongo()
    run_id = str(uuid.uuid4())
    now = utcnow()

    mongo.ingestion_runs.insert_one({
        "run_id": run_id,
        "status": "started",
        "environment": _app_env(),
        "trigger": trigger,
        "sources": ["yellowpages"],
        "scraper_versions": _scraper_versions(),
        "raw_counts": {"total": 0, "by_source": {}},
        "started_at": now,
        "completed_at": None,
        "etl_processed": False,
        "errors": [],
        "created_at": now,
        "updated_at": now,
    })

    def bump(n: int):
        if n <= 0:
            return
        mongo.ingestion_runs.update_one(
            {"run_id": run_id},
            {
                "$inc": {
                    "raw_counts.total": int(n),
                    "raw_counts.by_source.yellowpages": int(n),
                },
                "$set": {"updated_at": utcnow()},
            },
        )

    markets = ["Knoxville, TN", "Maryville, TN"]

    industries = [
        "Property Management",
        "HOA Management",
        "Commercial Real Estate",
        "Insurance Agencies",
    ]

    docs: List[Dict] = []
    errors: List[str] = []

    for city in markets:
        for industry in industries:
            try:
                results = fetch_yellowpages_playwright(
                    search_term=industry,
                    location=city,
                ) or []
            except Exception as e:
                errors.append(f"yellowpages[{industry}][{city}]: {type(e).__name__}")
                continue

            for r in results:
                wd = discover_website(r)
                raw_website = wd.get("raw_website")
                domain = wd.get("domain")

                website_payload = fetch_website_html(raw_website)

                raw = dict(r)
                raw.update({
                    "raw_website": raw_website,
                    "domain": domain,
                    "website_discovery": wd.get("website_discovery"),
                    "website_html": website_payload.get("website_html"),
                    "website_status": website_payload.get("website_status"),
                })

                docs.append({
                    "run_id": run_id,
                    "source": "yellowpages",
                    "source_id": _safe_source_id(
                        r.get("raw_company_name"),
                        r.get("raw_phone"),
                        city,
                    ),
                    "source_confidence": 0.80,
                    "ingested_at": now,
                    "captured_at": now,
                    "market": city,
                    "query": industry,
                    "raw": raw,
                    "extracted": raw,  # ETL derives everything
                })

    if docs:
        mongo.raw_businesses.insert_many(docs)
        bump(len(docs))

    if errors:
        mongo.ingestion_runs.update_one(
            {"run_id": run_id},
            {"$push": {"errors": {"$each": errors}}},
        )

    run = mongo.ingestion_runs.find_one({"run_id": run_id}) or {}
    total = (run.get("raw_counts") or {}).get("total") or 0
    status = "completed" if total > 0 else "failed"

    mongo.ingestion_runs.update_one(
        {"run_id": run_id},
        {
            "$set": {
                "status": status,
                "completed_at": utcnow(),
                "updated_at": utcnow(),
            }
        },
    )

    print(f"INGESTION COMPLETE — {total} raw records (run_id={run_id})")


if __name__ == "__main__":
    run_pipeline()
