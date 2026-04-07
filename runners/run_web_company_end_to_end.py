from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOS_ROOT = PROJECT_ROOT.parent
DEFAULT_ETL_ROOT = REPOS_ROOT / "pdb-etl"

INGEST_RE = re.compile(
    r"INGESTION COMPLETE\s+[—-]\s+(\d+)\s+raw records\s+\(run_id=([^)]+)\)",
    re.IGNORECASE,
)
DATASET_ID_RE = re.compile(r"^dataset_id:\s*([0-9a-fA-F-]{36})\s*$")
RUN_ID_RE = re.compile(r"^run_id:\s*([0-9a-fA-F-]{36})\s*$")
KEY_VALUE_RE = re.compile(r"^([A-Za-z0-9_]+):\s*(.+?)\s*$")

load_dotenv()
load_dotenv(PROJECT_ROOT / ".env", override=True)

APP_ENV = (os.getenv("APP_ENV") or "local").strip().lower()
if APP_ENV == "production":
    load_dotenv(PROJECT_ROOT / ".env.production", override=True)
else:
    load_dotenv(PROJECT_ROOT / ".env.local", override=True)

from runners.web_run_metrics import (  # noqa: E402
    build_source_benchmarks,
    evaluate_alerts,
    upsert_web_run_metrics,
)


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


def _safe_int(value, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_float(value, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def _print_block(title: str, payload: Dict[str, object]) -> None:
    print("======================================")
    print(f" {title}")
    print("======================================")
    for k, v in payload.items():
        print(f"{k}: {v}")


def _run_streaming_command(
    *,
    label: str,
    cmd: list[str],
    cwd: Path,
) -> Dict[str, Optional[str]]:
    print("======================================")
    print(f" {label}")
    print("======================================")
    print(f"cwd: {cwd}")
    print(f"cmd: {' '.join(cmd)}")

    started = time.time()

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=os.environ.copy(),
    )

    assert proc.stdout is not None

    out: Dict[str, Optional[str]] = {
        "run_id": None,
        "dataset_id": None,
    }

    for raw_line in proc.stdout:
        line = raw_line.rstrip("\n")
        print(line)

        m_ingest = INGEST_RE.search(line)
        if m_ingest:
            out["raw_records_ingested"] = m_ingest.group(1)
            out["run_id"] = m_ingest.group(2)

        m_dataset = DATASET_ID_RE.match(line.strip())
        if m_dataset:
            out["dataset_id"] = m_dataset.group(1)

        m_run = RUN_ID_RE.match(line.strip())
        if m_run and not out.get("run_id"):
            out["run_id"] = m_run.group(1)

        m_kv = KEY_VALUE_RE.match(line.strip())
        if m_kv:
            key, value = m_kv.group(1), m_kv.group(2)
            out[key] = value

    rc = proc.wait()
    elapsed = round(time.time() - started, 2)

    out["return_code"] = str(rc)
    out["elapsed_seconds"] = str(elapsed)

    if rc != 0:
        raise RuntimeError(
            f"{label} failed with exit code {rc}. "
            f"Command: {' '.join(cmd)}"
        )

    return out


def _fetch_validation_counts(run_id: str) -> Dict[str, object]:
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                with run_dataset as (
                  select d.id, d.run_id, d.name, d.run_at, d.is_baseline, d.record_count
                  from public.datasets d
                  where d.run_id = %s
                ),
                run_companies as (
                  select c.*
                  from public.companies c
                  join run_dataset d
                    on d.id = c.dataset_id
                ),
                run_leads as (
                  select l.*
                  from public.leads l
                  join run_companies c
                    on c.id = l.company_id
                )
                select
                  (select id from run_dataset limit 1) as dataset_id,
                  (select record_count from run_dataset limit 1) as dataset_record_count,
                  (select count(*) from run_companies) as companies_in_run,
                  (select count(*) from run_companies where website is not null) as companies_with_website,
                  (select count(*) from run_companies where domain is not null) as companies_with_domain,
                  (select count(*) from run_companies where phone_primary is not null) as companies_with_phone,
                  (select count(*) from run_companies where email_primary is not null) as companies_with_company_email,
                  (select count(*) from run_companies where contact_form_url is not null) as companies_with_contact_form,
                  (select count(*) from run_leads) as leads_from_run_companies,
                  (select count(*) from run_leads where source = 'company_expansion') as company_expansion_leads,
                  (select count(*) from run_leads where lead_stage != 'prospect' and email is not null and syntax_valid is true and mx_valid is true) as sellable_leads_now,
                  (
                    select count(*)
                    from run_leads l
                    join run_companies c on c.id = l.company_id
                    where l.full_name is not null
                      and l.email is not null
                      and l.phone is not null
                      and (
                        c.mailing_street is not null
                        or c.physical_address is not null
                        or (
                          c.mailing_city is not null
                          and c.mailing_state is not null
                          and c.mailing_zip is not null
                        )
                      )
                  ) as premium_like_leads_now
                """,
                (run_id,),
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


def _assemble_metrics_payload(
    *,
    run_id: Optional[str],
    dataset_id: Optional[str],
    trigger: str,
    status: str,
    error_message: Optional[str],
    ingest_result: Dict[str, Optional[str]],
    etl_result: Dict[str, Optional[str]],
    expansion_result: Dict[str, Optional[str]],
    expansion_apply_result: Dict[str, Optional[str]],
    contact_result: Dict[str, Optional[str]],
    validation: Dict[str, object],
    total_elapsed: float,
) -> Dict[str, object]:
    payload = {
        "run_id": run_id,
        "dataset_id": validation.get("dataset_id") or etl_result.get("dataset_id") or dataset_id,
        "trigger": trigger,
        "status": status,
        "error_message": error_message,
        "raw_records_ingested": _safe_int(ingest_result.get("raw_records_ingested")),
        "dataset_record_count": _safe_int(validation.get("dataset_record_count") or etl_result.get("dataset_record_count")),
        "companies_seeded": _safe_int(etl_result.get("canonical_companies_upserted_current_state") or validation.get("companies_in_run")),
        "companies_enriched": _safe_int(expansion_apply_result.get("applied")),
        "contacts_found": _safe_int(contact_result.get("contacts_found")),
        "named_contacts_found": _safe_int(contact_result.get("named_contacts_found")),
        "leads_inserted": _safe_int(contact_result.get("leads_inserted_this_execution"), _safe_int(contact_result.get("leads_inserted_reported"))),
        "named_leads_inserted": _safe_int(contact_result.get("named_leads_inserted_this_execution"), _safe_int(contact_result.get("named_leads_inserted_reported"))),
        "sellable_leads_inserted": _safe_int(contact_result.get("sellable_leads_inserted_this_execution"), _safe_int(contact_result.get("sellable_leads_inserted_reported"))),
        "premium_leads_inserted": _safe_int(contact_result.get("premium_leads_inserted_this_execution"), _safe_int(contact_result.get("premium_leads_inserted_reported"))),
        "companies_scanned": _safe_int(contact_result.get("companies_scanned")),
        "companies_in_run": _safe_int(validation.get("companies_in_run")),
        "companies_with_website": _safe_int(validation.get("companies_with_website")),
        "companies_with_domain": _safe_int(validation.get("companies_with_domain")),
        "companies_with_phone": _safe_int(validation.get("companies_with_phone")),
        "companies_with_company_email": _safe_int(validation.get("companies_with_company_email")),
        "companies_with_contact_form": _safe_int(validation.get("companies_with_contact_form")),
        "leads_from_run_companies": _safe_int(validation.get("leads_from_run_companies")),
        "company_expansion_leads": _safe_int(validation.get("company_expansion_leads")),
        "sellable_leads_now": _safe_int(validation.get("sellable_leads_now")),
        "premium_like_leads_now": _safe_int(validation.get("premium_like_leads_now")),
        "raw_expansion_docs_written": _safe_int(expansion_result.get("raw_expansion_docs_written_delta")),
        "expansion_apply_found": _safe_int(expansion_apply_result.get("found")),
        "expansion_apply_applied": _safe_int(expansion_apply_result.get("applied")),
        "company_phone_updates": _safe_int(contact_result.get("company_phone_updates")),
        "company_email_updates": _safe_int(contact_result.get("company_email_updates")),
        "company_form_updates": _safe_int(contact_result.get("company_form_updates")),
        "ingestion_elapsed_seconds": round(_safe_float(ingest_result.get("elapsed_seconds")), 2),
        "etl_elapsed_seconds": round(_safe_float(etl_result.get("elapsed_seconds")), 2),
        "expansion_elapsed_seconds": round(_safe_float(expansion_result.get("elapsed_seconds")), 2),
        "expansion_apply_elapsed_seconds": round(_safe_float(expansion_apply_result.get("elapsed_seconds")), 2),
        "contact_discovery_elapsed_seconds": round(_safe_float(contact_result.get("elapsed_seconds")), 2),
        "elapsed_seconds_total": round(float(total_elapsed), 2),
    }

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-command end-to-end web/company orchestration across pdb-pipeline and pdb-etl."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional existing run_id. If provided, skip raw ingestion and resume from ETL.",
    )
    parser.add_argument(
        "--etl-root",
        default=str(DEFAULT_ETL_ROOT),
        help="Path to sibling pdb-etl repo.",
    )
    parser.add_argument(
        "--expansion-limit",
        type=int,
        default=100,
        help="Limit for company expansion selection.",
    )
    parser.add_argument(
        "--contact-limit",
        type=int,
        default=250,
        help="Limit for run-scoped contact discovery company scan.",
    )
    parser.add_argument(
        "--require-missing-enrichment",
        action="store_true",
        help="Pass through to company expansion runner.",
    )
    parser.add_argument(
        "--skip-expansion",
        action="store_true",
        help="Skip raw company expansion + apply.",
    )
    parser.add_argument(
        "--skip-contact-discovery",
        action="store_true",
        help="Skip company contact discovery.",
    )
    parser.add_argument(
        "--trigger",
        default="manual",
        help="Operational trigger label persisted with this run.",
    )
    parser.add_argument(
        "--metrics-json-out",
        default=None,
        help="Optional file path to write final metrics JSON.",
    )
    parser.add_argument(
        "--alert-source-failure-rate",
        type=float,
        default=0.25,
        help="Alert threshold for abnormal source query failure rate.",
    )
    args = parser.parse_args()

    etl_root = Path(args.etl_root).resolve()
    if not etl_root.exists():
        raise RuntimeError(f"ETL repo path does not exist: {etl_root}")

    python_exe = sys.executable
    orchestration_started = time.time()

    run_id = args.run_id
    dataset_id: Optional[str] = None

    ingest_result: Dict[str, Optional[str]] = {}
    etl_result: Dict[str, Optional[str]] = {}
    expansion_result: Dict[str, Optional[str]] = {}
    expansion_apply_result: Dict[str, Optional[str]] = {}
    contact_result: Dict[str, Optional[str]] = {}
    validation: Dict[str, object] = {}

    status = "completed"
    error_message: Optional[str] = None
    run_error: Optional[Exception] = None
    persist_error: Optional[Exception] = None

    try:
        if not run_id:
            ingest_result = _run_streaming_command(
                label="WEB RAW INGESTION",
                cmd=[python_exe, "-u", "-m", "runners.run_web_ingestion_with_metrics"],
                cwd=PROJECT_ROOT,
            )
            run_id = ingest_result.get("run_id")
            if not run_id:
                raise RuntimeError("Failed to capture run_id from ingestion output.")
        else:
            _print_block(
                "RESUME MODE",
                {
                    "run_id": run_id,
                    "note": "Skipping raw ingestion because --run-id was provided.",
                },
            )

        etl_result = _run_streaming_command(
            label="WEB ETL",
            cmd=[python_exe, "-u", "-m", "runners.run_web_etl_with_metrics", "--run-id", run_id],
            cwd=etl_root,
        )
        dataset_id = etl_result.get("dataset_id")
        if not dataset_id:
            raise RuntimeError("Failed to capture dataset_id from ETL output.")

        if not args.skip_expansion:
            expansion_cmd = [
                python_exe,
                "-u",
                "-m",
                "runners.run_company_entity_expansion_with_metrics",
                "--parent-run-id",
                run_id,
                "--limit",
                str(int(args.expansion_limit)),
            ]
            if args.require_missing_enrichment:
                expansion_cmd.append("--require-missing-enrichment")

            expansion_result = _run_streaming_command(
                label="COMPANY ENTITY EXPANSION",
                cmd=expansion_cmd,
                cwd=PROJECT_ROOT,
            )

            expansion_apply_result = _run_streaming_command(
                label="COMPANY ENTITY EXPANSION APPLY",
                cmd=[
                    python_exe,
                    "-u",
                    "-m",
                    "runners.run_company_entity_expansion_apply_with_metrics",
                    "--parent-run-id",
                    run_id,
                    "--dataset-id",
                    dataset_id,
                ],
                cwd=etl_root,
            )

        if not args.skip_contact_discovery:
            contact_result = _run_streaming_command(
                label="COMPANY CONTACT DISCOVERY",
                cmd=[
                    python_exe,
                    "-u",
                    "-m",
                    "runners.run_company_contact_discovery_with_metrics",
                    "--run-id",
                    run_id,
                    "--limit-companies",
                    str(int(args.contact_limit)),
                    "--restrict-to-run-companies",
                ],
                cwd=etl_root,
            )

        validation = _fetch_validation_counts(run_id)
        total_elapsed = round(time.time() - orchestration_started, 2)

        _print_block(
            "FINAL RUN VALIDATION",
            {
                "run_id": run_id,
                "dataset_id": validation.get("dataset_id"),
                "dataset_record_count": validation.get("dataset_record_count"),
                "companies_in_run": validation.get("companies_in_run"),
                "companies_with_website": validation.get("companies_with_website"),
                "companies_with_domain": validation.get("companies_with_domain"),
                "companies_with_phone": validation.get("companies_with_phone"),
                "companies_with_company_email": validation.get("companies_with_company_email"),
                "companies_with_contact_form": validation.get("companies_with_contact_form"),
                "leads_from_run_companies": validation.get("leads_from_run_companies"),
                "company_expansion_leads": validation.get("company_expansion_leads"),
                "sellable_leads_now": validation.get("sellable_leads_now"),
                "premium_like_leads_now": validation.get("premium_like_leads_now"),
                "elapsed_seconds_total": total_elapsed,
            },
        )

    except Exception as e:
        status = "failed"
        error_message = f"{type(e).__name__}: {e}"
        run_error = e

    finally:
        total_elapsed = round(time.time() - orchestration_started, 2)

        payload = _assemble_metrics_payload(
            run_id=run_id,
            dataset_id=dataset_id,
            trigger=args.trigger,
            status=status,
            error_message=error_message,
            ingest_result=ingest_result,
            etl_result=etl_result,
            expansion_result=expansion_result,
            expansion_apply_result=expansion_apply_result,
            contact_result=contact_result,
            validation=validation,
            total_elapsed=total_elapsed,
        )

        source_benchmarks = build_source_benchmarks(run_id, payload) if run_id else {}
        alert_state = evaluate_alerts(
            payload,
            source_benchmarks=source_benchmarks,
            abnormal_source_failure_rate=float(args.alert_source_failure_rate),
        )

        payload["source_benchmarks"] = source_benchmarks
        payload["source_failure_rate"] = alert_state.get("source_failure_rate", 0.0)
        payload["alert_thresholds"] = alert_state.get("alert_thresholds", {})
        payload["alerts"] = alert_state.get("alerts", [])
        payload["alert_triggered"] = bool(alert_state.get("alert_triggered"))

        if args.metrics_json_out:
            metrics_path = Path(args.metrics_json_out).resolve()
            metrics_path.parent.mkdir(parents=True, exist_ok=True)
            metrics_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

        print("FINAL_METRICS_JSON: " + json.dumps(payload, sort_keys=True, default=str))

        try:
            if run_id:
                upsert_web_run_metrics(payload)
        except Exception as e:
            persist_error = e
            print(f"WEB METRICS PERSIST FAILED: {type(e).__name__}: {e}")

        if persist_error is not None and run_error is None:
            raise persist_error

        if run_error is not None:
            raise run_error


if __name__ == "__main__":
    main()