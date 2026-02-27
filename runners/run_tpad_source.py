# PDB-pipeline/runners/run_tpad_source.py

from dotenv import load_dotenv
import os
import sys
from pathlib import Path
import argparse

# --------------------------------------------------
# Ensure project root is on PYTHONPATH
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# --------------------------------------------------
# Load environment
# --------------------------------------------------
load_dotenv()

print("🚨 RUNNING TPAD INGESTION AGAINST MONGO RAW LAYER")

# --------------------------------------------------
# Imports AFTER env + path setup
# --------------------------------------------------
from runners.compliance_gate import validate_source, ComplianceError
from scrapers.tpad_scraper import ingest_tpad_csv


def run_tpad(file_path: str):
    source_key = "tn_tpad"

    print(f"➡ Source: {source_key}")
    print(f"➡ File: {file_path}")

    try:
        validate_source(source_key)
    except ComplianceError as e:
        print(f"❌ Compliance Blocked: {e}")
        return

    print("🚀 Starting TPAD ingestion...")

    run_id = ingest_tpad_csv(
        file_path=file_path,
        source_key=source_key
    )

    print("\n✅ TPAD ingestion complete")
    print(f"Run ID: {run_id}")


# --------------------------------------------------
# CLI Entry
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    run_tpad(args.file)
