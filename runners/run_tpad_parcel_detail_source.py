# pdb-pipeline/runners/run_tpad_parcel_detail_source.py
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

def _load_env():
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(REPO_ROOT / ".env", override=False)
    except Exception:
        pass

_load_env()

from db.mongo_client import get_mongo
from scrapers.tpad_parcel_detail import (
    fetch_parcel_details_html,
    parse_parcel_details_html,
    stable_hash,
)

RAW_SEED_COLLECTION = "raw_property_records"
RAW_OUT_COLLECTION = "raw_parcel_details"

UTC = timezone.utc


def utcnow():
    return datetime.now(UTC)


def _get_collection(mongo_obj: Any, name: str):
    """
    Supports:
      1) pymongo Database: db["collection"]
      2) wrapper with .get_collection()
      3) wrapper exposing collections as attributes
      4) wrapper that stores db as .db
    """
    try:
        return mongo_obj[name]  # type: ignore[index]
    except Exception:
        pass

    getter = getattr(mongo_obj, "get_collection", None)
    if callable(getter):
        return getter(name)

    if hasattr(mongo_obj, name):
        return getattr(mongo_obj, name)

    inner = getattr(mongo_obj, "db", None)
    if inner is not None:
        try:
            return inner[name]
        except Exception:
            pass
        if hasattr(inner, name):
            return getattr(inner, name)

    raise TypeError(f"Unsupported mongo client shape: cannot resolve collection '{name}' type={type(mongo_obj)}")


def _ensure_indexes(mongo_obj: Any):
    out_coll = _get_collection(mongo_obj, RAW_OUT_COLLECTION)
    try:
        out_coll.create_index([("run_id", 1), ("parcel_id", 1), ("jur", 1), ("status", 1)])
        out_coll.create_index([("run_id", 1), ("parcel_id", 1), ("jur", 1), ("detail_hash", 1)])
    except Exception:
        pass


def _extract_seed_targets(mongo_obj: Any, run_id: str, limit: int) -> List[Tuple[str, str]]:
    """
    limit = 0 means NO LIMIT (fetch all seed parcels for run_id)
    """
    coll = _get_collection(mongo_obj, RAW_SEED_COLLECTION)

    cursor = coll.find(
        {"run_id": run_id},
        {"raw_payload": 1},
    )

    if limit and int(limit) > 0:
        cursor = cursor.limit(int(limit))

    out: List[Tuple[str, str]] = []
    for doc in cursor:
        payload = doc.get("raw_payload") or {}
        parcel_id = (payload.get("parcel_id") or payload.get("Parcel ID") or "").strip()
        county = (payload.get("county") or payload.get("County") or "").strip()
        if parcel_id:
            out.append((parcel_id, county))
    return out


def _already_ok(out_coll, run_id: str, parcel_id: str, jur: str) -> bool:
    doc = out_coll.find_one(
        {"run_id": run_id, "parcel_id": parcel_id, "jur": jur, "status": "ok"},
        {"_id": 1},
    )
    return doc is not None


def run_tpad_parcel_detail_source(
    run_id: str,
    jur: str,
    limit: int = 25,
    sleep_s: float = 0.35,
    timeout_s: int = 20,
    resume: bool = False,
    max_attempts: int = 3,
) -> Dict[str, Any]:
    mongo = get_mongo()
    _ensure_indexes(mongo)

    print(
        f"[parcel_detail_source] start run_id={run_id} jur={jur} "
        f"limit={limit} sleep={sleep_s}s timeout={timeout_s}s resume={resume} max_attempts={max_attempts}"
    )

    seed = _extract_seed_targets(mongo, run_id=run_id, limit=limit)
    print(f"[parcel_detail_source] seed_found={len(seed)} (from {RAW_SEED_COLLECTION})")

    if not seed:
        return {"ok": True, "found": 0, "inserted": 0, "skipped": 0, "failed": 0}

    out_coll = _get_collection(mongo, RAW_OUT_COLLECTION)

    inserted = 0
    skipped = 0
    failed = 0

    for idx, (parcel_id, county) in enumerate(seed, start=1):
        parcel_id = (parcel_id or "").strip()
        if not parcel_id:
            skipped += 1
            continue

        if resume and _already_ok(out_coll, run_id, parcel_id, jur):
            skipped += 1
            if idx <= 5 or idx % 200 == 0:
                print(f"[parcel_detail_source] {idx}/{len(seed)} parcel_id={parcel_id} skipped (already ok)")
            continue

        attempt_count = out_coll.count_documents({"run_id": run_id, "parcel_id": parcel_id, "jur": jur})
        if attempt_count >= max_attempts:
            failed += 1
            print(f"[parcel_detail_source] {idx}/{len(seed)} parcel_id={parcel_id} skipped (max_attempts reached)")
            continue

        try:
            html, status = fetch_parcel_details_html(parcel_id=parcel_id, jur=jur, timeout=timeout_s)

            if not html or status != 200:
                failed += 1
                out_coll.insert_one(
                    {
                        "source_key": "tn_tpad",
                        "run_id": run_id,
                        "captured_at": utcnow(),
                        "parcel_id": parcel_id,
                        "county": county or None,
                        "jur": jur,
                        "status": "fail",
                        "http_status": status,
                        "error": "no html" if not html else f"http_status={status}",
                        "attempt_count": attempt_count + 1,
                    }
                )
                print(f"[parcel_detail_source] {idx}/{len(seed)} parcel_id={parcel_id} status={status} fail")
                time.sleep(sleep_s)
                continue

            payload = parse_parcel_details_html(html)
            detail_hash = stable_hash(payload)

            out_coll.insert_one(
                {
                    "source_key": "tn_tpad",
                    "run_id": run_id,
                    "captured_at": utcnow(),
                    "parcel_id": parcel_id,
                    "county": county or None,
                    "jur": jur,
                    "detail_hash": detail_hash,
                    "status": "ok",
                    "http_status": status,
                    "attempt_count": attempt_count + 1,
                    "raw_payload": payload,
                }
            )
            inserted += 1
            print(f"[parcel_detail_source] {idx}/{len(seed)} parcel_id={parcel_id} inserted")

        except Exception as e:
            failed += 1
            out_coll.insert_one(
                {
                    "source_key": "tn_tpad",
                    "run_id": run_id,
                    "captured_at": utcnow(),
                    "parcel_id": parcel_id,
                    "county": county or None,
                    "jur": jur,
                    "status": "fail",
                    "http_status": None,
                    "error": str(e),
                    "attempt_count": attempt_count + 1,
                }
            )
            print(f"[parcel_detail_source] {idx}/{len(seed)} parcel_id={parcel_id} exception={e}")

        time.sleep(sleep_s)

    print(f"[parcel_detail_source] done found={len(seed)} inserted={inserted} skipped={skipped} failed={failed}")
    return {"ok": True, "found": len(seed), "inserted": inserted, "skipped": skipped, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description="TPAD Parcel Detail drilldown ingestion (Mongo raw_parcel_details)")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--jur", required=True, help="County jurisdiction code (e.g. 005 for Blount)")
    parser.add_argument("--limit", type=int, default=25, help="0 = all parcels for run")
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--resume", action="store_true", help="Skip parcels already stored as ok for run_id/jur")
    parser.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args()

    run_tpad_parcel_detail_source(
        run_id=args.run_id,
        jur=args.jur,
        limit=args.limit,
        sleep_s=args.sleep,
        timeout_s=args.timeout,
        resume=args.resume,
        max_attempts=args.max_attempts,
    )


if __name__ == "__main__":
    main()
