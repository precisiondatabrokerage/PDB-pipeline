from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv()
load_dotenv(PROJECT_ROOT / ".env", override=True)

APP_ENV = (os.getenv("APP_ENV") or "local").strip().lower()
if APP_ENV == "production":
    load_dotenv(PROJECT_ROOT / ".env.production", override=True)
else:
    load_dotenv(PROJECT_ROOT / ".env.local", override=True)

from db.mongo_client import get_mongo  # noqa: E402

ERROR_SOURCE_RE = re.compile(r"^\s*([A-Za-z0-9_]+)\[")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_pg_conn():
    app_env = (os.getenv("APP_ENV") or "local").strip().lower()

    if app_env == "production":
        dsn = (
            os.getenv("SUPABASE_POSTGRES_URL")
            or os.getenv("DATABASE_URL")
            or os.getenv("POSTGRES_DSN")
        )
    else:
        dsn = (
            os.getenv("POSTGRES_DSN")
            or os.getenv("DATABASE_URL")
            or os.getenv("SUPABASE_POSTGRES_URL")
        )

    if not dsn:
        raise RuntimeError(
            f"No Postgres DSN found for APP_ENV={app_env}. "
            "Expected SUPABASE_POSTGRES_URL for production or "
            "POSTGRES_DSN/DATABASE_URL for local."
        )

    return psycopg2.connect(dsn)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def _safe_rate(numerator: float, denominator: float, digits: int = 4) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), digits)


def _nested_get(doc: dict, *path: str) -> Any:
    cur = doc
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _extract_query_key(doc: dict) -> Optional[str]:
    query = (doc.get("query") or "").strip()
    market = (doc.get("market") or "").strip()

    if query and market:
        return f"{query}||{market}"
    if query:
        return query
    if market:
        return market
    return None


def _extract_error_source(err: Any) -> str:
    text = str(err or "").strip()
    m = ERROR_SOURCE_RE.match(text)
    if m:
        return m.group(1).strip().lower()
    return "unknown"


def _has_website_in_raw_doc(doc: dict) -> bool:
    value = (
        _nested_get(doc, "raw", "raw_website")
        or _nested_get(doc, "extracted", "raw_website")
        or doc.get("raw_website")
        or doc.get("website")
    )
    return bool(str(value).strip()) if value is not None else False


CREATE_DATASET_METRICS_WEB_SQL = """
CREATE TABLE IF NOT EXISTS public.dataset_metrics_web (
    run_id TEXT PRIMARY KEY,
    dataset_id UUID NULL,
    trigger TEXT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    error_message TEXT NULL,

    raw_records_ingested INTEGER NOT NULL DEFAULT 0,
    companies_seeded INTEGER NOT NULL DEFAULT 0,
    companies_enriched INTEGER NOT NULL DEFAULT 0,
    contacts_found INTEGER NOT NULL DEFAULT 0,
    leads_inserted INTEGER NOT NULL DEFAULT 0,
    sellable_leads_inserted INTEGER NOT NULL DEFAULT 0,
    premium_leads_inserted INTEGER NOT NULL DEFAULT 0,

    elapsed_seconds_total NUMERIC(12,2) NULL,
    source_failure_rate NUMERIC(8,4) NOT NULL DEFAULT 0,
    alert_triggered BOOLEAN NOT NULL DEFAULT false,

    source_benchmarks JSONB NOT NULL DEFAULT '{}'::jsonb,
    alert_thresholds JSONB NOT NULL DEFAULT '{}'::jsonb,
    alerts JSONB NOT NULL DEFAULT '[]'::jsonb,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_DATASET_METRICS_WEB_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_dataset_metrics_web_dataset_id ON public.dataset_metrics_web (dataset_id)",
    "CREATE INDEX IF NOT EXISTS idx_dataset_metrics_web_status ON public.dataset_metrics_web (status)",
    "CREATE INDEX IF NOT EXISTS idx_dataset_metrics_web_created_at ON public.dataset_metrics_web (created_at DESC)",
]


def ensure_dataset_metrics_web_table() -> None:
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public")
            cur.execute(CREATE_DATASET_METRICS_WEB_SQL)
            for stmt in CREATE_DATASET_METRICS_WEB_INDEXES:
                cur.execute(stmt)
        conn.commit()


def build_source_benchmarks(run_id: Optional[str], metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    metrics = metrics or {}
    if not run_id:
        return {}

    mongo = get_mongo()
    db = mongo.db

    raw_docs = list(
        db["raw_businesses"].find(
            {"run_id": run_id},
            {
                "_id": 0,
                "source": 1,
                "query": 1,
                "market": 1,
                "raw.raw_website": 1,
                "extracted.raw_website": 1,
                "raw_website": 1,
                "website": 1,
            },
        )
    )

    run_doc = db["ingestion_runs"].find_one(
        {"run_id": run_id},
        {"_id": 0, "sources": 1, "errors": 1, "trigger": 1},
    ) or {}

    stats = defaultdict(
        lambda: {
            "raw_records": 0,
            "website_hits_raw": 0,
            "query_keys": set(),
            "error_query_count": 0,
        }
    )

    for doc in raw_docs:
        source = str(doc.get("source") or "unknown").strip().lower()
        s = stats[source]
        s["raw_records"] += 1

        qk = _extract_query_key(doc)
        if qk:
            s["query_keys"].add(qk)

        if _has_website_in_raw_doc(doc):
            s["website_hits_raw"] += 1

    for err in (run_doc.get("errors") or []):
        source = _extract_error_source(err)
        stats[source]["error_query_count"] += 1

    all_sources = set(str(s).strip().lower() for s in (run_doc.get("sources") or []) if s) | set(stats.keys())
    if not all_sources:
        all_sources = {"unknown"}

    single_source_run = len(all_sources) == 1
    single_source_name = next(iter(all_sources)) if single_source_run else None

    companies_seeded = _safe_int(metrics.get("companies_seeded"))
    companies_with_website = _safe_int(metrics.get("companies_with_website"))
    companies_scanned = max(1, _safe_int(metrics.get("companies_scanned")))
    named_contacts_found = _safe_int(metrics.get("named_contacts_found"))
    sellable_leads_inserted = _safe_int(metrics.get("sellable_leads_inserted"))
    premium_leads_inserted = _safe_int(metrics.get("premium_leads_inserted"))

    out: Dict[str, Dict[str, Any]] = {}

    for source in sorted(all_sources):
        item = stats[source]
        successful_query_pairs = len(item["query_keys"])
        failed_query_pairs = _safe_int(item["error_query_count"])
        total_query_pairs = successful_query_pairs + failed_query_pairs

        benchmark = {
            "raw_records": _safe_int(item["raw_records"]),
            "successful_query_pairs": successful_query_pairs,
            "failed_query_pairs": failed_query_pairs,
            "total_query_pairs": total_query_pairs,
            "source_failure_rate": _safe_rate(failed_query_pairs, total_query_pairs),
            "attribution_mode": "single-source-run" if single_source_run else "raw-ingestion-only",
        }

        if single_source_run and source == single_source_name:
            benchmark["companies_found_per_100_queries"] = round(
                _safe_rate(companies_seeded, max(1, total_query_pairs), digits=6) * 100,
                2,
            )
            benchmark["websites_discovered_rate"] = _safe_rate(companies_with_website, max(1, companies_seeded))
            benchmark["named_contact_rate"] = _safe_rate(named_contacts_found, companies_scanned)
            benchmark["sellable_email_rate"] = _safe_rate(sellable_leads_inserted, companies_scanned)
            benchmark["premium_contact_rate"] = _safe_rate(premium_leads_inserted, companies_scanned)
            benchmark["attribution_note"] = "Run-level contact metrics safely attributed because only one source was active in this run."
        else:
            benchmark["companies_found_per_100_queries"] = round(
                _safe_rate(item["raw_records"], max(1, total_query_pairs), digits=6) * 100,
                2,
            )
            benchmark["websites_discovered_rate"] = _safe_rate(item["website_hits_raw"], max(1, item["raw_records"]))
            benchmark["named_contact_rate"] = None
            benchmark["sellable_email_rate"] = None
            benchmark["premium_contact_rate"] = None
            benchmark["attribution_note"] = (
                "Multi-source contact attribution is unavailable until source lineage is persisted to canonical companies/leads."
            )

        out[source] = benchmark

    return out


def evaluate_alerts(
    metrics: Dict[str, Any],
    source_benchmarks: Optional[Dict[str, Dict[str, Any]]] = None,
    abnormal_source_failure_rate: float = 0.25,
) -> Dict[str, Any]:
    source_benchmarks = source_benchmarks or {}

    companies_seeded = _safe_int(metrics.get("companies_seeded"))
    leads_inserted = _safe_int(metrics.get("leads_inserted"))
    sellable_leads_inserted = _safe_int(metrics.get("sellable_leads_inserted"))
    max_source_failure_rate = max(
        (_safe_float(v.get("source_failure_rate")) for v in source_benchmarks.values()),
        default=0.0,
    )

    alerts = []

    if str(metrics.get("status") or "").lower() != "completed":
        alerts.append(
            {
                "key": "run_failed",
                "severity": "error",
                "message": "The scheduled run did not complete successfully.",
                "current": metrics.get("status"),
            }
        )

    if companies_seeded <= 0:
        alerts.append(
            {
                "key": "zero_inserted_companies",
                "severity": "error",
                "message": "No companies were seeded for this run.",
                "current": companies_seeded,
            }
        )

    if leads_inserted <= 0:
        alerts.append(
            {
                "key": "zero_inserted_contacts",
                "severity": "error",
                "message": "No new contacts/leads were inserted during this execution.",
                "current": leads_inserted,
            }
        )

    if sellable_leads_inserted <= 0:
        alerts.append(
            {
                "key": "zero_sellable_leads",
                "severity": "error",
                "message": "No sellable leads were inserted during this execution.",
                "current": sellable_leads_inserted,
            }
        )

    if max_source_failure_rate > float(abnormal_source_failure_rate):
        alerts.append(
            {
                "key": "abnormal_source_failure_rate",
                "severity": "error",
                "message": "At least one source exceeded the allowed query failure rate.",
                "current": round(max_source_failure_rate, 4),
                "threshold": float(abnormal_source_failure_rate),
            }
        )

    return {
        "source_failure_rate": round(max_source_failure_rate, 4),
        "alert_thresholds": {
            "abnormal_source_failure_rate": float(abnormal_source_failure_rate),
        },
        "alerts": alerts,
        "alert_triggered": bool(alerts),
    }


def upsert_web_run_metrics(payload: Dict[str, Any]) -> None:
    ensure_dataset_metrics_web_table()

    sql = """
    INSERT INTO public.dataset_metrics_web (
        run_id,
        dataset_id,
        trigger,
        status,
        error_message,
        raw_records_ingested,
        companies_seeded,
        companies_enriched,
        contacts_found,
        leads_inserted,
        sellable_leads_inserted,
        premium_leads_inserted,
        elapsed_seconds_total,
        source_failure_rate,
        alert_triggered,
        source_benchmarks,
        alert_thresholds,
        alerts,
        metrics,
        updated_at
    )
    VALUES (
        %s,
        NULLIF(%s, '')::uuid,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s::jsonb,
        %s::jsonb,
        %s::jsonb,
        %s::jsonb,
        %s
    )
    ON CONFLICT (run_id)
    DO UPDATE SET
        dataset_id = EXCLUDED.dataset_id,
        trigger = EXCLUDED.trigger,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message,
        raw_records_ingested = EXCLUDED.raw_records_ingested,
        companies_seeded = EXCLUDED.companies_seeded,
        companies_enriched = EXCLUDED.companies_enriched,
        contacts_found = EXCLUDED.contacts_found,
        leads_inserted = EXCLUDED.leads_inserted,
        sellable_leads_inserted = EXCLUDED.sellable_leads_inserted,
        premium_leads_inserted = EXCLUDED.premium_leads_inserted,
        elapsed_seconds_total = EXCLUDED.elapsed_seconds_total,
        source_failure_rate = EXCLUDED.source_failure_rate,
        alert_triggered = EXCLUDED.alert_triggered,
        source_benchmarks = EXCLUDED.source_benchmarks,
        alert_thresholds = EXCLUDED.alert_thresholds,
        alerts = EXCLUDED.alerts,
        metrics = EXCLUDED.metrics,
        updated_at = EXCLUDED.updated_at
    """

    now = utcnow()

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public")
            cur.execute(
                sql,
                (
                    str(payload.get("run_id") or ""),
                    str(payload.get("dataset_id") or ""),
                    payload.get("trigger"),
                    payload.get("status") or "unknown",
                    payload.get("error_message"),
                    _safe_int(payload.get("raw_records_ingested")),
                    _safe_int(payload.get("companies_seeded")),
                    _safe_int(payload.get("companies_enriched")),
                    _safe_int(payload.get("contacts_found")),
                    _safe_int(payload.get("leads_inserted")),
                    _safe_int(payload.get("sellable_leads_inserted")),
                    _safe_int(payload.get("premium_leads_inserted")),
                    round(_safe_float(payload.get("elapsed_seconds_total")), 2),
                    round(_safe_float(payload.get("source_failure_rate")), 4),
                    bool(payload.get("alert_triggered")),
                    Json(payload.get("source_benchmarks") or {}),
                    Json(payload.get("alert_thresholds") or {}),
                    Json(payload.get("alerts") or []),
                    Json(payload),
                    now,
                ),
            )
        conn.commit()