# =====================================================
# PDB-PIPELINE/runners/run_tpad_end_to_end.py
#
# End-to-End TPAD Runner
#
# Runs the complete pipeline:
#
#   1️⃣ TPAD Ingestion
#   2️⃣ ETL (resolve + canonical insert)
#   3️⃣ Company Expansion Apply
#   4️⃣ Company Contact Discovery
#   5️⃣ Prospect Enrichment
#   6️⃣ Prospect Contact Enrichment
#   7️⃣ Trigger Engine
#   8️⃣ Prospect Scoring
#
# Single command orchestration.
#
# =====================================================

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import re
from pathlib import Path

# Ensure UTF-8 stdout on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# -----------------------------------------------------
# Helper: run subprocess with streaming output
# -----------------------------------------------------

def run_cmd(cmd: list[str], cwd: Path) -> None:

    print(f"\n[runner] executing: {' '.join(cmd)}")
    print(f"[runner] cwd={cwd}")

    start = time.time()

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    for line in proc.stdout:
        print(line.rstrip())

    proc.wait()

    elapsed = round(time.time() - start, 2)

    if proc.returncode != 0:
        raise RuntimeError(f"command failed (exit={proc.returncode})")

    print(f"[runner] completed in {elapsed}s")


# -----------------------------------------------------
# TPAD ingestion
# -----------------------------------------------------

def run_tpad_ingestion(file: str, county: str | None, pipeline_root: Path) -> str:

    cmd = [
        sys.executable,
        "-m",
        "runners.run_tpad_source",
        "--file",
        file,
    ]

    if county:
        cmd += ["--county", county]

    print("\n[runner] starting TPAD ingestion")

    proc = subprocess.Popen(
        cmd,
        cwd=str(pipeline_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    output_lines = []

    for line in proc.stdout:
        line = line.rstrip()
        print(line)
        output_lines.append(line)

    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError("TPAD ingestion failed")

    # -------------------------------------------------
    # Robust run_id detection (UUID search)
    # -------------------------------------------------

    uuid_pattern = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

    matches = []

    for line in output_lines:
        matches.extend(re.findall(uuid_pattern, line))

    if not matches:
        raise RuntimeError("Could not parse run_id from ingestion output")

    run_id = matches[-1]

    print(f"\n[runner] detected run_id → {run_id}")

    return run_id


# -----------------------------------------------------
# ETL runner
# -----------------------------------------------------

def run_etl(run_id: str, etl_root: Path) -> None:

    cmd = [
        sys.executable,
        "-m",
        "runners.run_etl_for_run_id",
        "--run-id",
        run_id,
    ]

    print("\n[runner] starting ETL")

    run_cmd(cmd, etl_root)


# -----------------------------------------------------
# Main orchestration
# -----------------------------------------------------

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file",
        required=True,
        help="TPAD CSV export file",
    )

    parser.add_argument(
        "--county",
        required=False,
        help="County name override",
    )

    parser.add_argument(
        "--pipeline-root",
        default=".",
        help="Path to pipeline repo",
    )

    parser.add_argument(
        "--etl-root",
        default="../pdb-etl",
        help="Path to etl repo",
    )

    args = parser.parse_args()

    pipeline_root = Path(args.pipeline_root).resolve()
    etl_root = Path(args.etl_root).resolve()

    print("\n==============================")
    print(" PDB TPAD END-TO-END RUNNER")
    print("==============================")

    print(f"pipeline_root: {pipeline_root}")
    print(f"etl_root: {etl_root}")

    overall_start = time.time()

    # -------------------------------------------------
    # Step 1 — Ingestion
    # -------------------------------------------------

    run_id = run_tpad_ingestion(
        file=args.file,
        county=args.county,
        pipeline_root=pipeline_root,
    )

    # -------------------------------------------------
    # Step 2 — ETL
    # -------------------------------------------------

    run_etl(
        run_id=run_id,
        etl_root=etl_root,
    )

    elapsed = round(time.time() - overall_start, 2)

    print("\n======================================")
    print(" PIPELINE COMPLETE")
    print("======================================")

    print(f"run_id: {run_id}")
    print(f"total runtime: {elapsed}s")


# -----------------------------------------------------

if __name__ == "__main__":
    main()