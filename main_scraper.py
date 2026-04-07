from dotenv import load_dotenv
load_dotenv()

import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

# =====================================================
# ENABLED SCRAPER
# =====================================================
from scrapers.yellowpages_scraper import (
    DEFAULT_YELLOWPAGES_INDUSTRIES,
    DEFAULT_YELLOWPAGES_LOCATIONS,
    fetch_yellowpages_scraper,
)

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
    import scrapers.yellowpages_scraper as yp
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

    markets = list(DEFAULT_YELLOWPAGES_LOCATIONS)
    industries = list(DEFAULT_YELLOWPAGES_INDUSTRIES)

    docs: List[Dict] = []
    errors: List[str] = []
    seen_doc_keys = set()

    for city in markets:
        for industry in industries:
            try:
                results = fetch_yellowpages_scraper(
                    search_term=industry,
                    location=city,
                    headless=True,
                    max_pages=2,
                    max_scrolls=3,
                ) or []
            except Exception as e:
                errors.append(f"yellowpages[{industry}][{city}]: {type(e).__name__}")
                continue

            print(f"[debug] scraper_results city={city} industry={industry} count={len(results)}")
            if results:
                print(f"[debug] first_result={results[0]}")

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

                doc = {
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
                    "extracted": raw,
                }

                dedupe_key = (
                    doc["source"],
                    doc["source_id"],
                    doc["market"],
                    doc["query"],
                )
                if dedupe_key in seen_doc_keys:
                    continue

                seen_doc_keys.add(dedupe_key)
                docs.append(doc)

    print(f"[debug] docs_total_before_insert={len(docs)}")
    if docs:
        mongo.raw_businesses.insert_many(docs)
        print(f"[debug] inserted_docs={len(docs)}")
        bump(len(docs))
        latest_run = mongo.ingestion_runs.find_one({"run_id": run_id}) or {}
        print(f"[debug] raw_counts_after_bump={latest_run.get('raw_counts')}")

    if errors:
        mongo.ingestion_runs.update_one(
            {"run_id": run_id},
            {"$push": {"errors": {"$each": errors}}},
        )

    print(f"[debug] fetching_final_run_doc run_id={run_id}")
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