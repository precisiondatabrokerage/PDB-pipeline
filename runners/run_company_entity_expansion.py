from __future__ import annotations

import argparse
import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

from db.mongo_client import get_mongo  # noqa: E402


def get_pg_conn():
    dsn = (
        os.getenv("POSTGRES_DSN")
        or os.getenv("SUPABASE_POSTGRES_URL")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "No Postgres DSN found. Set POSTGRES_DSN or SUPABASE_POSTGRES_URL or DATABASE_URL."
        )
    return psycopg2.connect(dsn)


def get_raw_company_entity_expansion_collection():
    """
    Resolve the Mongo collection safely across different helper return shapes.
    """
    mongo = get_mongo()

    coll = getattr(mongo, "raw_company_entity_expansion", None)
    if coll is not None:
        return coll

    db_obj = getattr(mongo, "db", None)
    if db_obj is not None:
        coll = getattr(db_obj, "raw_company_entity_expansion", None)
        if coll is not None:
            return coll
        try:
            return db_obj["raw_company_entity_expansion"]
        except Exception:
            pass

    try:
        return mongo["raw_company_entity_expansion"]
    except Exception:
        pass

    raise RuntimeError(
        "Could not resolve Mongo collection 'raw_company_entity_expansion' from get_mongo()."
    )


def load_expand_fn():
    candidates = [
        "enrichers.company_entity_expansion_v1",
        "scrapers.company_entity_expansion_v1",
        "company_entity_expansion_v1",
        "transform.company_entity_expansion_v1",
        "services.company_entity_expansion_v1",
    ]

    last_err = None
    for mod_name in candidates:
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, "expand_company_entity_v1", None)
            if fn:
                print(f"[import] using {mod_name}.expand_company_entity_v1")
                return fn
        except Exception as e:
            last_err = e

    raise RuntimeError(
        "Could not import expand_company_entity_v1 from any known module path. "
        "Check where company_entity_expansion_v1.py actually lives."
    ) from last_err


def _parse_company_ids(raw: Optional[str]) -> List[int]:
    if not raw:
        return []

    out: List[int] = []
    for part in str(raw).split(","):
        s = part.strip().rstrip(">")
        if not s:
            continue
        out.append(int(s))
    return out


def fetch_company_candidates(
    limit: int,
    company_ids: Optional[List[int]] = None,
    require_missing_enrichment: bool = False,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            c.id,
            c.canonical_name,
            c.domain,
            c.website,
            c.phone_primary,
            c.email_primary,
            c.contact_form_url,
            c.mailing_city,
            c.mailing_state
        FROM public.companies c
        WHERE 1=1
    """
    params: List[Any] = []

    if company_ids:
        sql += " AND c.id = ANY(%s)"
        params.append(company_ids)

    if require_missing_enrichment:
        sql += """
            AND (
                c.website IS NULL
                OR c.domain IS NULL
                OR c.phone_primary IS NULL
                OR c.email_primary IS NULL
                OR c.contact_form_url IS NULL
            )
        """

    sql += """
        ORDER BY c.id
        LIMIT %s
    """
    params.append(int(limit))

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _build_expand_kwargs(
    expand_fn,
    row: Dict[str, Any],
    parent_run_id: str,
) -> Dict[str, Any]:
    sig = inspect.signature(expand_fn)
    accepted = set(sig.parameters.keys())

    candidate_values = {
        "company_id": int(row["id"]),
        "id": int(row["id"]),
        "canonical_name": row.get("canonical_name"),
        "company_name": row.get("canonical_name"),
        "name": row.get("canonical_name"),
        "city": row.get("mailing_city"),
        "mailing_city": row.get("mailing_city"),
        "state": row.get("mailing_state"),
        "mailing_state": row.get("mailing_state"),
        "parent_run_id": parent_run_id,
        "run_id": parent_run_id,
        "source_company": row,
        "company": row,
        "company_row": row,
        "row": row,
        "record": row,
        "payload": row,
    }

    kwargs: Dict[str, Any] = {}
    for key, value in candidate_values.items():
        if key in accepted:
            kwargs[key] = value

    return kwargs


def _call_expand_fn(
    expand_fn,
    row: Dict[str, Any],
    parent_run_id: str,
):
    kwargs = _build_expand_kwargs(expand_fn, row, parent_run_id)

    if kwargs:
        return expand_fn(**kwargs)

    sig = inspect.signature(expand_fn)
    params = list(sig.parameters.values())

    required_positional = [
        p for p in params
        if p.default is inspect._empty
        and p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]

    if len(required_positional) == 1:
        return expand_fn(row)

    if len(required_positional) == 2:
        return expand_fn(row, parent_run_id)

    raise RuntimeError(
        "Unable to call expand_company_entity_v1 safely. "
        f"Detected signature: {sig}"
    )


def _is_document(value: Any) -> bool:
    return isinstance(value, dict)


def _iter_documents_from_payload(
    payload: Any,
    row: Dict[str, Any],
    parent_run_id: str,
) -> List[Dict[str, Any]]:
    """
    Normalize arbitrary enricher return shapes into Mongo-insertable documents.
    Accepted:
    - dict
    - list/tuple of dicts
    - wrapper dict with keys like payload/doc/document/raw/result/results/items/documents
    Everything else is skipped.
    """
    docs: List[Dict[str, Any]] = []

    if payload is None:
        return docs

    if isinstance(payload, dict):
        # Direct document
        if payload:
            docs.append(payload)
            return docs

    if isinstance(payload, (list, tuple)):
        for item in payload:
            if isinstance(item, dict):
                docs.append(item)
            elif isinstance(item, (list, tuple)):
                for sub in item:
                    if isinstance(sub, dict):
                        docs.append(sub)
        return docs

    # Objects that expose a dict-like body
    if hasattr(payload, "__dict__"):
        obj_dict = dict(payload.__dict__)
        if obj_dict:
            payload = obj_dict

    if isinstance(payload, dict):
        candidate_keys = [
            "payload",
            "doc",
            "document",
            "raw",
            "result",
            "results",
            "items",
            "documents",
            "expansion",
            "expansions",
        ]
        for key in candidate_keys:
            value = payload.get(key)
            if isinstance(value, dict):
                docs.append(value)
                return docs
            if isinstance(value, (list, tuple)):
                for item in value:
                    if isinstance(item, dict):
                        docs.append(item)
                if docs:
                    return docs

        # Last resort: if it's a metadata wrapper, wrap it as a raw record
        docs.append(
            {
                "parent_run_id": parent_run_id,
                "company_id": int(row["id"]),
                "canonical_name": row.get("canonical_name"),
                "source": "run_company_entity_expansion_wrapper_fallback",
                "payload": payload,
            }
        )
        return docs

    return docs


def _ensure_minimum_metadata(
    doc: Dict[str, Any],
    row: Dict[str, Any],
    parent_run_id: str,
) -> Dict[str, Any]:
    out = dict(doc)

    out.setdefault("parent_run_id", parent_run_id)
    out.setdefault("company_id", int(row["id"]))
    out.setdefault("canonical_name", row.get("canonical_name"))
    out.setdefault("mailing_city", row.get("mailing_city"))
    out.setdefault("mailing_state", row.get("mailing_state"))

    return out


def run_company_expansion(
    parent_run_id: str,
    limit: int,
    company_ids: Optional[List[int]] = None,
    require_missing_enrichment: bool = False,
) -> None:
    expand_company_entity_v1 = load_expand_fn()

    rows = fetch_company_candidates(
        limit=limit,
        company_ids=company_ids,
        require_missing_enrichment=require_missing_enrichment,
    )

    print("======================================")
    print(" COMPANY ENTITY EXPANSION")
    print("======================================")
    print(f"parent_run_id: {parent_run_id}")
    print(f"limit: {limit}")
    print(f"require_missing_enrichment: {require_missing_enrichment}")
    print(f"selected_companies: {len(rows)}")

    if rows:
        print("\n[preview] selected companies:")
        for r in rows[:25]:
            print(
                f"  company_id={r['id']} "
                f"name={r.get('canonical_name')} "
                f"website={r.get('website')} "
                f"domain={r.get('domain')} "
                f"phone={r.get('phone_primary')} "
                f"email={r.get('email_primary')} "
                f"form={r.get('contact_form_url')}"
            )

    raw_coll = get_raw_company_entity_expansion_collection()

    written = 0
    skipped = 0
    errors = 0

    for row in rows:
        try:
            payload = _call_expand_fn(
                expand_fn=expand_company_entity_v1,
                row=row,
                parent_run_id=parent_run_id,
            )

            docs = _iter_documents_from_payload(
                payload=payload,
                row=row,
                parent_run_id=parent_run_id,
            )

            if not docs:
                skipped += 1
                print(
                    f"[skip] company_id={row['id']} "
                    f"name={row.get('canonical_name')} "
                    f"reason=no_insertable_documents "
                    f"payload_type={type(payload).__name__}"
                )
                continue

            prepared_docs = [
                _ensure_minimum_metadata(doc=d, row=row, parent_run_id=parent_run_id)
                for d in docs
                if isinstance(d, dict)
            ]

            if not prepared_docs:
                skipped += 1
                print(
                    f"[skip] company_id={row['id']} "
                    f"name={row.get('canonical_name')} "
                    "reason=normalized_documents_empty"
                )
                continue

            if len(prepared_docs) == 1:
                raw_coll.insert_one(prepared_docs[0])
                written += 1
            else:
                raw_coll.insert_many(prepared_docs, ordered=False)
                written += len(prepared_docs)

            print(
                f"[ok] company_id={row['id']} "
                f"name={row.get('canonical_name')} "
                f"docs_written={len(prepared_docs)}"
            )

        except Exception as e:
            errors += 1
            print(
                f"[error] company_id={row['id']} "
                f"name={row.get('canonical_name')} "
                f"{type(e).__name__}: {e}"
            )
            continue

    print("\n======================================")
    print(" COMPANY ENTITY EXPANSION COMPLETE")
    print("======================================")
    print(f"raw_expansions_written: {written}")
    print(f"skipped: {skipped}")
    print(f"errors: {errors}")
    print(f"parent_run_id: {parent_run_id}")
    print("Next: run ETL apply for the same parent_run_id.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-run-id", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--company-ids", type=str, default=None)
    parser.add_argument("--require-missing-enrichment", action="store_true")
    args = parser.parse_args()

    company_ids = _parse_company_ids(args.company_ids)

    run_company_expansion(
        parent_run_id=args.parent_run_id,
        limit=args.limit,
        company_ids=company_ids,
        require_missing_enrichment=args.require_missing_enrichment,
    )


if __name__ == "__main__":
    main()
