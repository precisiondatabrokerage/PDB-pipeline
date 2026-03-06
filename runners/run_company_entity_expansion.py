# pdb-pipeline/runners/run_company_entity_expansion.py
# HOW TO RUN:
# cd pdb-pipeline
# $env:APP_ENV="production"
# python .\runners\run_company_entity_expansion.py --parent-run-id <TPAD_RUN_ID> --limit 200

from __future__ import annotations

import argparse
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

from db.mongo_client import get_mongo
from enrichers.company_entity_expansion_v1 import (
    expand_company_entity_v1,
    write_company_expansions_to_mongo,
)
import psycopg2
import os


def utcnow():
    return datetime.now(timezone.utc)


def _pg_conn():
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("SUPABASE_POSTGRES_URL")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN (or SUPABASE_POSTGRES_URL) not set")
    return psycopg2.connect(dsn)


def fetch_company_candidates(limit: int = 200):
    sql = """
    SELECT
      c.id,
      c.canonical_name,
      COUNT(po.property_id) AS property_count,
      MAX(p.county) AS county_hint
    FROM public.companies c
    JOIN public.property_ownership po ON po.company_id = c.id
    JOIN public.properties p ON p.property_id = po.property_id
    WHERE c.domain IS NULL OR c.website IS NULL
    GROUP BY c.id, c.canonical_name
    ORDER BY property_count DESC, c.id DESC
    LIMIT %s
    """

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return cur.fetchall()


def run_company_expansion(parent_run_id: str, limit: int = 200):
    source_key = "company_expansion"

    rows = fetch_company_candidates(limit=limit)
    if not rows:
        print("No company candidates found (all expanded or none exist).")
        return

    expansions = []
    for (company_id, canonical_name, property_count, county_hint) in rows:
        ex = expand_company_entity_v1(
            company_id=int(company_id),
            canonical_name=str(canonical_name or "").strip(),
            county_hint=(county_hint or None),
        )
        expansions.append(ex)

    inserted = write_company_expansions_to_mongo(
        parent_run_id=parent_run_id,
        source_key=source_key,
        expansions=expansions,
    )

    print(f"Company entity expansion complete — {inserted} raw expansions written to Mongo")
    print(f"Parent run_id: {parent_run_id}")
    print("Next: run ETL for the same TPAD run_id to apply expansions into Postgres.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--parent-run-id", required=True)
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()
    run_company_expansion(args.parent_run_id, limit=args.limit)