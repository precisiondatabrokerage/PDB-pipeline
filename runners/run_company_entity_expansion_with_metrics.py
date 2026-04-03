from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from runners.run_company_entity_expansion import (
    _parse_company_ids,
    fetch_company_candidates,
    get_raw_company_entity_expansion_collection,
    run_company_expansion,
)


def _print_stage_metrics(metrics: dict) -> None:
    print("======================================")
    print(" STAGE METRICS")
    print("======================================")
    for key, value in metrics.items():
        print(f"{key}: {value}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-run-id", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--company-ids", type=str, default=None)
    parser.add_argument("--require-missing-enrichment", action="store_true")
    parser.set_defaults(restrict_to_parent_run_companies=True)
    parser.add_argument(
        "--restrict-to-parent-run-companies",
        dest="restrict_to_parent_run_companies",
        action="store_true",
    )
    parser.add_argument(
        "--all-companies",
        dest="restrict_to_parent_run_companies",
        action="store_false",
    )
    args = parser.parse_args()

    company_ids = _parse_company_ids(args.company_ids)

    selected = fetch_company_candidates(
        parent_run_id=args.parent_run_id,
        limit=int(args.limit),
        company_ids=company_ids,
        require_missing_enrichment=bool(args.require_missing_enrichment),
        restrict_to_parent_run_companies=bool(args.restrict_to_parent_run_companies),
    )

    coll = get_raw_company_entity_expansion_collection()
    before_count = coll.count_documents({"parent_run_id": args.parent_run_id})

    started = time.time()
    run_company_expansion(
        parent_run_id=args.parent_run_id,
        limit=int(args.limit),
        company_ids=company_ids,
        require_missing_enrichment=bool(args.require_missing_enrichment),
        restrict_to_parent_run_companies=bool(args.restrict_to_parent_run_companies),
    )
    elapsed = round(time.time() - started, 2)

    after_count = coll.count_documents({"parent_run_id": args.parent_run_id})

    _print_stage_metrics(
        {
            "stage_key": "company_entity_expansion_raw",
            "status": "completed",
            "parent_run_id": args.parent_run_id,
            "selected_companies": len(selected),
            "raw_expansion_docs_written_delta": max(0, after_count - before_count),
            "require_missing_enrichment": bool(args.require_missing_enrichment),
            "restrict_to_parent_run_companies": bool(args.restrict_to_parent_run_companies),
            "elapsed_seconds": elapsed,
        }
    )


if __name__ == "__main__":
    main()