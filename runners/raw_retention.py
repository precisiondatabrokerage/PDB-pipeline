from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from bson import json_util

from db.mongo_client import get_mongo

RUN_ID_SENTINEL = "__RUN_ID__"

DEFAULT_COLLECTION_FILTERS: Dict[str, Dict[str, Any]] = {
    "raw_businesses": {"run_id": RUN_ID_SENTINEL},
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    return value


def _resolve_filter(template: Any, run_id: str) -> Any:
    if isinstance(template, dict):
        return {k: _resolve_filter(v, run_id) for k, v in template.items()}
    if isinstance(template, list):
        return [_resolve_filter(v, run_id) for v in template]
    if template == RUN_ID_SENTINEL:
        return run_id
    return template


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_to_jsonable(dict(payload)), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _stream_collection_to_jsonl_gz(collection, query: Mapping[str, Any], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    with gzip.open(output_path, "wt", encoding="utf-8") as fh:
        cursor = collection.find(query, no_cursor_timeout=True).batch_size(500)
        try:
            for doc in cursor:
                fh.write(json_util.dumps(doc))
                fh.write("\n")
                written += 1
        finally:
            cursor.close()

    return written


def archive_run_raw_docs(
    *,
    run_id: str,
    archive_root: str | Path,
    collection_filters: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    mongo = get_mongo()
    archive_root = Path(archive_root).resolve()
    run_archive_dir = archive_root / run_id
    run_archive_dir.mkdir(parents=True, exist_ok=True)

    collection_filters = dict(collection_filters or DEFAULT_COLLECTION_FILTERS)

    archives: Dict[str, Any] = {}
    total_written = 0

    for collection_name, filter_template in collection_filters.items():
        query = _resolve_filter(filter_template, run_id)
        collection = mongo[collection_name]

        matched = collection.count_documents(query)
        if matched <= 0:
            archives[collection_name] = {
                "query": query,
                "matched": 0,
                "written": 0,
                "archive_file": None,
            }
            continue

        archive_file = run_archive_dir / f"{collection_name}.{run_id}.jsonl.gz"
        written = _stream_collection_to_jsonl_gz(collection, query, archive_file)
        total_written += written

        archives[collection_name] = {
            "query": query,
            "matched": int(matched),
            "written": int(written),
            "archive_file": str(archive_file),
        }

    manifest = {
        "run_id": run_id,
        "status": "archived",
        "archived_at": utcnow(),
        "archive_root": str(run_archive_dir),
        "collections": archives,
        "total_written": int(total_written),
    }

    manifest_path = run_archive_dir / "archive_manifest.json"
    _write_json(manifest_path, manifest)

    return {
        "status": "archived",
        "archived_at": manifest["archived_at"],
        "archive_root": str(run_archive_dir),
        "manifest_path": str(manifest_path),
        "collections": archives,
        "total_written": int(total_written),
    }


def purge_run_raw_docs(
    *,
    run_id: str,
    collection_filters: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    mongo = get_mongo()
    collection_filters = dict(collection_filters or DEFAULT_COLLECTION_FILTERS)

    deleted_total = 0
    collections: Dict[str, Any] = {}

    for collection_name, filter_template in collection_filters.items():
        query = _resolve_filter(filter_template, run_id)
        collection = mongo[collection_name]

        result = collection.delete_many(query)
        deleted = int(result.deleted_count or 0)
        deleted_total += deleted

        collections[collection_name] = {
            "query": query,
            "deleted": deleted,
        }

    return {
        "status": "purged",
        "purged_at": utcnow(),
        "collections": collections,
        "deleted_total": int(deleted_total),
    }


def mark_run_raw_retention(
    *,
    run_id: str,
    mode: str,
    archive_meta: Optional[Mapping[str, Any]] = None,
    purge_meta: Optional[Mapping[str, Any]] = None,
    status: str = "completed",
    error: Optional[str] = None,
) -> None:
    mongo = get_mongo()

    mongo.ingestion_runs.update_one(
        {"run_id": run_id},
        {
            "$set": {
                "raw_retention.mode": mode,
                "raw_retention.status": status,
                "raw_retention.archive": _to_jsonable(archive_meta) if archive_meta else None,
                "raw_retention.purge": _to_jsonable(purge_meta) if purge_meta else None,
                "raw_retention.error": error,
                "raw_retention.updated_at": utcnow(),
                "updated_at": utcnow(),
            }
        },
    )


def perform_run_raw_retention(
    *,
    run_id: str,
    mode: str,
    archive_root: str | Path,
    collection_filters: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    if mode not in {"keep", "archive-only", "archive-then-purge"}:
        raise ValueError(f"Unsupported raw retention mode: {mode}")

    if mode == "keep":
        result = {
            "status": "kept",
            "mode": mode,
            "archive": None,
            "purge": None,
        }
        mark_run_raw_retention(run_id=run_id, mode=mode, status="kept")
        return result

    archive_meta = archive_run_raw_docs(
        run_id=run_id,
        archive_root=archive_root,
        collection_filters=collection_filters,
    )

    purge_meta = None
    if mode == "archive-then-purge":
        purge_meta = purge_run_raw_docs(
            run_id=run_id,
            collection_filters=collection_filters,
        )

    status = "archived" if mode == "archive-only" else "archived_and_purged"

    result = {
        "status": status,
        "mode": mode,
        "archive": archive_meta,
        "purge": purge_meta,
    }

    mark_run_raw_retention(
        run_id=run_id,
        mode=mode,
        archive_meta=archive_meta,
        purge_meta=purge_meta,
        status=status,
    )

    return result