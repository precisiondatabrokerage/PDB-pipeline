from __future__ import annotations

import argparse
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


# -----------------------------------------------------
# Environment bootstrap
# -----------------------------------------------------
load_dotenv()
load_dotenv(PROJECT_ROOT / ".env", override=True)

APP_ENV = (os.getenv("APP_ENV") or "local").strip().lower()
if APP_ENV == "production":
    load_dotenv(PROJECT_ROOT / ".env.production", override=True)
else:
    load_dotenv(PROJECT_ROOT / ".env.local", override=True)


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
    args = parser.parse_args()

    etl_root = Path(args.etl_root).resolve()
    if not etl_root.exists():
        raise RuntimeError(f"ETL repo path does not exist: {etl_root}")

    python_exe = sys.executable
    orchestration_started = time.time()

    run_id = args.run_id
    dataset_id: Optional[str] = None

    # -------------------------------------------------
    # 1) Raw ingestion
    # -------------------------------------------------
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

    # -------------------------------------------------
    # 2) Web ETL
    # -------------------------------------------------
    etl_result = _run_streaming_command(
        label="WEB ETL",
        cmd=[python_exe, "-u", "-m", "runners.run_web_etl_with_metrics", "--run-id", run_id],
        cwd=etl_root,
    )
    dataset_id = etl_result.get("dataset_id")
    if not dataset_id:
        raise RuntimeError("Failed to capture dataset_id from ETL output.")

    # -------------------------------------------------
    # 3) Company expansion (optional)
    # -------------------------------------------------
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

        _run_streaming_command(
            label="COMPANY ENTITY EXPANSION",
            cmd=expansion_cmd,
            cwd=PROJECT_ROOT,
        )

        # -------------------------------------------------
        # 4) Expansion apply
        # -------------------------------------------------
        _run_streaming_command(
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

    # -------------------------------------------------
    # 5) Contact discovery (optional)
    # -------------------------------------------------
    if not args.skip_contact_discovery:
        _run_streaming_command(
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

    # -------------------------------------------------
    # 6) Validation counts
    # -------------------------------------------------
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


if __name__ == "__main__":
    main()