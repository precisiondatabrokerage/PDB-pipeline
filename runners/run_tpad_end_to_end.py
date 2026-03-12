# pdb-pipeline/runners/run_tpad_end_to_end.py

# ======================================================
# HOW TO RUN
#
# $env:APP_ENV="production"
# python -u -m runners.run_tpad_end_to_end --file artifacts/sevier_main_1_2026_03_10.csv --county Sevier --parcel-detail --jur 078 --parcel-limit 0 --resume
# ======================================================

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# ======================================================
# Paths / environment
# ======================================================

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
ETL_ROOT = Path(os.getenv("PDB_ETL_ROOT") or (PIPELINE_ROOT.parent / "pdb-etl")).resolve()

sys.path.insert(0, str(PIPELINE_ROOT))


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ======================================================
# Optional .env load (pipeline repo uses dotenv style)
# ======================================================

def _load_env():
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(PIPELINE_ROOT / ".env", override=False)
    except Exception:
        pass


_load_env()


# ======================================================
# Postgres checkpoint helpers (best-effort)
# ======================================================

def _get_checkpoint_dsn() -> Optional[str]:
    return os.getenv("SUPABASE_POSTGRES_URL") or os.getenv("POSTGRES_DSN")


def _with_pg(fn):
    dsn = _get_checkpoint_dsn()
    if not dsn:
        return None
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("set search_path to public")
                out = fn(conn, cur)
            conn.commit()
            return out
        finally:
            conn.close()
    except Exception:
        return None


def checkpoint_get(run_id: str, stage_key: str) -> Optional[Dict[str, Any]]:
    def _q(conn, cur):
        cur.execute(
            """
            SELECT status, started_at, completed_at, meta, updated_at
            FROM public.run_stage_checkpoints
            WHERE run_id=%s AND stage_key=%s
            """,
            (run_id, stage_key),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "status": r[0],
            "started_at": r[1].isoformat() if r[1] else None,
            "completed_at": r[2].isoformat() if r[2] else None,
            "meta": r[3] or {},
            "updated_at": r[4].isoformat() if r[4] else None,
        }

    return _with_pg(_q)


def checkpoint_set(run_id: str, stage_key: str, status: str, meta: Optional[dict] = None):
    meta = meta or {}

    def _q(conn, cur):
        cur.execute(
            """
            INSERT INTO public.run_stage_checkpoints
              (run_id, stage_key, status, started_at, completed_at, meta, updated_at)
            VALUES
              (%s, %s, %s, now(), CASE WHEN %s='completed' THEN now() ELSE NULL END, %s::jsonb, now())
            ON CONFLICT (run_id, stage_key)
            DO UPDATE SET
              status = EXCLUDED.status,
              completed_at = CASE WHEN EXCLUDED.status='completed' THEN now() ELSE public.run_stage_checkpoints.completed_at END,
              meta = COALESCE(public.run_stage_checkpoints.meta,'{}'::jsonb) || EXCLUDED.meta,
              updated_at = now()
            """,
            (run_id, stage_key, status, status, json.dumps(meta)),
        )
        return True

    _with_pg(_q)


# ======================================================
# Subprocess runner
# ======================================================

def run_cmd(cmd: list[str], cwd: Path):
    print(f"[runner] executing: {' '.join(cmd)}")
    print(f"[runner] cwd={cwd}")
    proc = subprocess.run(cmd, cwd=str(cwd))
    if proc.returncode != 0:
        raise RuntimeError(f"command failed (exit={proc.returncode})")


# ======================================================
# Main
# ======================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="TPAD CSV file path (relative to pipeline repo)")
    parser.add_argument("--county", required=True, help="County name (e.g. Sevier)")
    parser.add_argument("--parcel-detail", action="store_true", help="Enable parcel drilldown + apply")
    parser.add_argument("--jur", default=None, help="Jurisdiction code (e.g. 078). Required if --parcel-detail")
    parser.add_argument("--parcel-limit", type=int, default=0, help="0 = all parcels for run, else limit N")
    parser.add_argument("--parcel-sleep", type=float, default=0.35)
    parser.add_argument("--parcel-timeout", type=int, default=20)
    parser.add_argument("--resume", action="store_true", help="Use run_stage_checkpoints to skip completed stages")

    args = parser.parse_args()

    if args.parcel_detail and not args.jur:
        raise SystemExit("--jur is required when --parcel-detail is enabled")

    print("\n==============================")
    print(" PDB TPAD END-TO-END RUNNER")
    print("==============================")
    print(f"pipeline_root: {PIPELINE_ROOT}")
    print(f"etl_root: {ETL_ROOT}")
    print(f"started_at: {utcnow()}")
    print()

    start_time = time.time()

    # --------------------------------------------------
    # Stage 1: TPAD ingestion into Mongo raw_property_records
    # --------------------------------------------------
    # NOTE: pipeline repo uses runners.run_tpad_source (not run_tpad_ingestion)
    ingest_cmd = [
        sys.executable,
        "-u",
        "-m",
        "runners.run_tpad_source",
        "--file",
        args.file,
        "--county",
        args.county,
    ]

    print("[runner] starting TPAD ingestion")
    print("RUNNING TPAD INGESTION AGAINST MONGO RAW LAYER")
    print(f"File: {args.file}")
    print(f"County: {args.county}")

    proc = subprocess.run(
        ingest_cmd,
        cwd=str(PIPELINE_ROOT),
        capture_output=True,
        text=True,
    )

    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n", file=sys.stderr)

    if proc.returncode != 0:
        raise RuntimeError(f"TPAD ingestion failed (exit={proc.returncode})")

    run_id = None
    for line in (proc.stdout or "").splitlines():
        if "Run ID:" in line:
            run_id = line.split("Run ID:", 1)[1].strip()
            break

    if not run_id:
        raise RuntimeError("Could not parse Run ID from TPAD ingestion output")

    print(f"[runner] detected run_id → {run_id}")

    if args.resume:
        checkpoint_set(run_id, "tpad_raw_ingest", "completed", {"file": args.file, "county": args.county})

    # --------------------------------------------------
    # Stage 2: Parcel detail drilldown → Mongo raw_parcel_details
    # --------------------------------------------------
    if args.parcel_detail:
        stage_key = "parcel_detail_source"
        if args.resume:
            ck = checkpoint_get(run_id, stage_key)
            if ck and ck.get("status") == "completed":
                print(f"[runner] checkpoint skip: {stage_key} already completed")
            else:
                checkpoint_set(run_id, stage_key, "started", {"jur": args.jur, "limit": args.parcel_limit})
                try:
                    limit = int(args.parcel_limit)
                    cmd = [
                        sys.executable,
                        "-u",
                        "-m",
                        "runners.run_tpad_parcel_detail_source",
                        "--run-id",
                        run_id,
                        "--jur",
                        str(args.jur),
                        "--sleep",
                        str(args.parcel_sleep),
                        "--timeout",
                        str(args.parcel_timeout),
                        "--limit",
                        str(limit if limit > 0 else 0),
                    ]
                    run_cmd(cmd, PIPELINE_ROOT)
                    checkpoint_set(run_id, stage_key, "completed", {"jur": args.jur, "limit": args.parcel_limit})
                except Exception as e:
                    checkpoint_set(run_id, stage_key, "failed", {"error": str(e)})
                    raise
        else:
            limit = int(args.parcel_limit)
            cmd = [
                sys.executable,
                "-u",
                "-m",
                "runners.run_tpad_parcel_detail_source",
                "--run-id",
                run_id,
                "--jur",
                str(args.jur),
                "--sleep",
                str(args.parcel_sleep),
                "--timeout",
                str(args.parcel_timeout),
                "--limit",
                str(limit if limit > 0 else 0),
            ]
            run_cmd(cmd, PIPELINE_ROOT)

    # --------------------------------------------------
    # Stage 3: TPAD ETL → Postgres canonical insert
    # --------------------------------------------------
    stage_key = "tpad_etl"
    if args.resume:
        ck = checkpoint_get(run_id, stage_key)
        if ck and ck.get("status") == "completed":
            print(f"[runner] checkpoint skip: {stage_key} already completed")
        else:
            checkpoint_set(run_id, stage_key, "started", {})
            try:
                print("\n[runner] starting ETL\n")
                etl_cmd = [sys.executable, "-u", "-m", "runners.run_etl_for_run_id", "--run-id", run_id]
                run_cmd(etl_cmd, ETL_ROOT)
                checkpoint_set(run_id, stage_key, "completed", {})
            except Exception as e:
                checkpoint_set(run_id, stage_key, "failed", {"error": str(e)})
                raise
    else:
        print("\n[runner] starting ETL\n")
        etl_cmd = [sys.executable, "-u", "-m", "runners.run_etl_for_run_id", "--run-id", run_id]
        run_cmd(etl_cmd, ETL_ROOT)

    # --------------------------------------------------
    # Stage 4: Parcel detail apply → Postgres enrichment tables
    # --------------------------------------------------
    if args.parcel_detail:
        stage_key = "parcel_detail_apply"
        if args.resume:
            ck = checkpoint_get(run_id, stage_key)
            if ck and ck.get("status") == "completed":
                print(f"[runner] checkpoint skip: {stage_key} already completed")
            else:
                checkpoint_set(run_id, stage_key, "started", {"jur": args.jur})
                try:
                    cmd = [sys.executable, "-u", "-m", "runners.run_parcel_detail_etl_for_run_id", "--run-id", run_id]
                    run_cmd(cmd, ETL_ROOT)
                    checkpoint_set(run_id, stage_key, "completed", {"jur": args.jur})
                except Exception as e:
                    checkpoint_set(run_id, stage_key, "failed", {"error": str(e)})
                    raise
        else:
            cmd = [sys.executable, "-u", "-m", "runners.run_parcel_detail_etl_for_run_id", "--run-id", run_id]
            run_cmd(cmd, ETL_ROOT)

    elapsed = time.time() - start_time
    print("\n======================================")
    print(" PIPELINE COMPLETE")
    print("======================================")
    print(f"run_id: {run_id}")
    print(f"total runtime: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
