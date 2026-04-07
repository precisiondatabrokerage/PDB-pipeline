from __future__ import annotations

import csv

from scrapers.yellowpages_scraper import (
    DEFAULT_YELLOWPAGES_INDUSTRIES,
    fetch_yellowpages_scraper,
)

__version__ = "4.0.0-wrapper"


def export_to_csv(records, filename="yellowpages_knoxville_multi_industry.csv"):
    if not records:
        print("No records to write to CSV.")
        return

    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    keys = sorted(list(all_keys))

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    print(f"CSV export complete: {filename}")


def run_yellowpages_multi_industry(
    location: str = "Knoxville, TN",
    filename: str = "yellowpages_knoxville_multi_industry.csv",
):
    all_records = []
    seen = set()

    for industry in DEFAULT_YELLOWPAGES_INDUSTRIES:
        rows = fetch_yellowpages_scraper(
            search_term=industry,
            location=location,
            headless=True,
            max_pages=2,
            max_scrolls=3,
        )

        for row in rows:
            key = (
                row.get("source_id"),
                row.get("raw_company_name"),
                row.get("raw_address"),
            )
            if key in seen:
                continue
            seen.add(key)
            all_records.append(row)

    export_to_csv(all_records, filename=filename)
    print(f"YellowPages consolidated wrapper complete: {len(all_records)} rows")


if __name__ == "__main__":
    run_yellowpages_multi_industry()