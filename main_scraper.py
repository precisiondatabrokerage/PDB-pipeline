from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config.web_scraper_targets import (
    count_active_yellowpages_query_pairs,
    get_active_yellowpages_targets,
)
from scrapers.yellowpages_scraper import fetch_yellowpages_scraper

from enrichers.website_discovery import discover_website
from enrichers.website_fetch import fetch_website_html

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


def _summarize_website_payload(website_payload: Any) -> Dict[str, Any]:
    """
    Safe, lightweight website fetch summary for Mongo raw docs.

    Important:
    - DO NOT store full website HTML in raw_businesses.
    - Only keep status + tiny metadata so a single huge page cannot exceed BSON limits.
    """
    if not isinstance(website_payload, dict):
        return {
            "website_status": None,
            "website_html_bytes": 0,
            "website_html_present": False,
        }

    html = website_payload.get("website_html")
    html_present = isinstance(html, str) and bool(html)

    if isinstance(html, str):
        html_bytes = len(html.encode("utf-8", errors="ignore"))
    else:
        html_bytes = 0

    return {
        "website_status": website_payload.get("website_status"),
        "website_html_bytes": html_bytes,
        "website_html_present": html_present,
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
        "query_plan": {
            "yellowpages_query_pairs": count_active_yellowpages_query_pairs(),
        },
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

    targets = get_active_yellowpages_targets()
    errors: List[str] = []
    seen_doc_keys = set()

    print(f"[debug] active_yellowpages_query_pairs={len(targets)}")

    for idx, target in enumerate(targets, start=1):
        city = target["market"]
        industry = target["industry"]
        headless = bool(target.get("headless", True))
        max_pages = int(target.get("max_pages", 1))
        max_scrolls = int(target.get("max_scrolls", 2))

        print(
            f"[debug] target {idx}/{len(targets)} "
            f"city={city} industry={industry} max_pages={max_pages} max_scrolls={max_scrolls}"
        )

        try:
            results = fetch_yellowpages_scraper(
                search_term=industry,
                location=city,
                headless=headless,
                max_pages=max_pages,
                max_scrolls=max_scrolls,
            ) or []
        except Exception as e:
            errors.append(f"yellowpages[{industry}][{city}]: {type(e).__name__}: {e}")
            continue

        print(f"[debug] scraper_results city={city} industry={industry} count={len(results)}")
        if results:
            print(f"[debug] first_result={results[0]}")

        docs_to_insert: List[Dict] = []

        for r in results:
            try:
                wd = discover_website(r)
                raw_website = wd.get("raw_website")
                domain = wd.get("domain")

                website_payload = fetch_website_html(raw_website)
                website_summary = _summarize_website_payload(website_payload)

                raw = dict(r)
                raw.update({
                    "raw_website": raw_website,
                    "domain": domain,
                    "website_discovery": wd.get("website_discovery"),
                    "website_status": website_summary.get("website_status"),
                    "website_html_bytes": website_summary.get("website_html_bytes"),
                    "website_html_present": website_summary.get("website_html_present"),
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
                docs_to_insert.append(doc)

            except Exception as e:
                errors.append(
                    f"enrichment[{industry}][{city}][{r.get('raw_company_name')}]: {type(e).__name__}: {e}"
                )
                continue

        if docs_to_insert:
            mongo.raw_businesses.insert_many(docs_to_insert)
            inserted_count = len(docs_to_insert)
            bump(inserted_count)
            print(
                f"[debug] inserted_docs city={city} industry={industry} count={inserted_count}"
            )

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