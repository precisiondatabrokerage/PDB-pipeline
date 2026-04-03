from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INGEST_RE = re.compile(
    r"INGESTION COMPLETE\s+[—-]\s+(\d+)\s+raw records\s+\(run_id=([^)]+)\)",
    re.IGNORECASE,
)


def _print_stage_metrics(metrics: dict) -> None:
    print("======================================")
    print(" STAGE METRICS")
    print("======================================")
    for key, value in metrics.items():
        print(f"{key}: {value}")


def _parse_ingestion_line(line: str) -> Tuple[Optional[int], Optional[str]]:
    m = INGEST_RE.search(line)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def main() -> None:
    started = time.time()
    raw_records = None
    run_id = None

    cmd = [sys.executable, "-u", str(PROJECT_ROOT / "main_scraper.py")]
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert proc.stdout is not None

    for line in proc.stdout:
        print(line, end="")
        parsed_raw, parsed_run = _parse_ingestion_line(line)
        if parsed_raw is not None:
            raw_records = parsed_raw
            run_id = parsed_run

    return_code = proc.wait()
    elapsed = round(time.time() - started, 2)

    status = "completed" if return_code == 0 else "failed"

    _print_stage_metrics(
        {
            "stage_key": "web_raw_ingestion",
            "status": status,
            "run_id": run_id,
            "raw_records_ingested": raw_records,
            "elapsed_seconds": elapsed,
            "command": "python -u main_scraper.py",
        }
    )

    if return_code != 0:
        raise SystemExit(return_code)


if __name__ == "__main__":
    main()