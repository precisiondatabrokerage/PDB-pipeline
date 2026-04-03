from __future__ import annotations

import argparse
import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# -----------------------------------------------------
# Environment bootstrap
# -----------------------------------------------------
# Base env first, then environment-specific override.
load_dotenv()
load_dotenv(PROJECT_ROOT / ".env", override=True)

APP_ENV = (os.getenv("APP_ENV") or "local").strip().lower()
if APP_ENV == "production":
    load_dotenv(PROJECT_ROOT / ".env.production", override=True)
else:
    load_dotenv(PROJECT_ROOT / ".env.local", override=True)

from db.mongo_client import get_mongo  # noqa: E402


def get_pg_conn():
    """
    Production must prefer the production canonical DB.
    Local/dev can prefer local DSNs.
    """
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


# =====================================================
# Mongo helpers
# =====================================================

def get_raw_company_entity_expansion_collection():
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


# =====================================================
# Import loader
# =====================================================

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


# =====================================================
# CLI helpers
# =====================================================

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


# =====================================================
# Dataset schema detection
# =====================================================

def _get_dataset_scope_mode() -> Tuple[bool, bool]:
    """
    Returns:
      (has_run_id_column, has_notes_column)

    We support both dataset lineage shapes:
    - new: public.datasets.run_id
    - old: public.datasets.notes with "run_id=<uuid>" marker
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'public'
                  and table_name = 'datasets'
                """
            )
            cols = {row[0] for row in cur.fetchall()}
            return ("run_id" in cols, "notes" in cols)
    finally:
        conn.close()


def _build_dataset_scope_sql(parent_run_id: str) -> Tuple[str, str, Tuple[Any, ...]]:
    """
    Returns:
      where_sql_fragment,
      selected_dataset_ref_sql,
      params_tuple

    selected_dataset_ref_sql is used for preview/debug output.
    """
    has_run_id, has_notes = _get_dataset_scope_mode()

    if has_run_id:
        return (
            " AND d.run_id = %s ",
            "d.run_id AS dataset_run_ref",
            (parent_run_id,),
        )

    if has_notes:
        return (
            " AND d.notes ILIKE %s ",
            "d.notes AS dataset_run_ref",
            (f"%run_id={parent_run_id}%",),
        )

    raise RuntimeError(
        "public.datasets has neither run_id nor notes columns. "
        "Cannot derive parent-run lineage for run-scoped company expansion."
    )


# =====================================================
# Postgres target debug
# =====================================================

def debug_pg_target(parent_run_id: str) -> None:
    """
    Print exactly which DB this runner is hitting and whether the run cohort
    is visible there.
    """
    where_scope_sql, _, scope_params = _build_dataset_scope_sql(parent_run_id)

    dataset_sql = f"""
        SELECT count(*)
        FROM public.datasets d
        WHERE 1=1
        {where_scope_sql}
    """

    companies_sql = f"""
        SELECT count(*)
        FROM public.companies c
        JOIN public.datasets d
          ON d.id = c.dataset_id
        WHERE 1=1
        {where_scope_sql}
    """

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select current_database(), current_user")
            db_name, db_user = cur.fetchone()

            cur.execute(dataset_sql, scope_params)
            dataset_count = cur.fetchone()[0]

            cur.execute(companies_sql, scope_params)
            company_count = cur.fetchone()[0]

        print("======================================")
        print(" POSTGRES TARGET DEBUG")
        print("======================================")
        print(f"APP_ENV: {APP_ENV}")
        print(f"current_database: {db_name}")
        print(f"current_user: {db_user}")
        print(f"datasets_for_run: {dataset_count}")
        print(f"companies_for_run: {company_count}")
    finally:
        conn.close()


# =====================================================
# Candidate selection
# =====================================================

def fetch_company_candidates(
    *,
    parent_run_id: str,
    limit: int,
    company_ids: Optional[List[int]] = None,
    require_missing_enrichment: bool = False,
    restrict_to_parent_run_companies: bool = True,
) -> List[Dict[str, Any]]:
    """
    Day 2 doctrine:
    - By default, select only companies created by the supplied parent run.
    - Primary lineage path: public.companies.dataset_id -> public.datasets.run_id
    - Legacy fallback: public.companies.dataset_id -> public.datasets.notes contains run_id marker
    - company_ids remains supported for surgical reruns
    - To intentionally target backlog companies, caller must pass restrict_to_parent_run_companies=False
    """
    where_scope_sql = ""
    dataset_run_ref_sql = "NULL::text AS dataset_run_ref"
    scope_params: Tuple[Any, ...] = ()

    if restrict_to_parent_run_companies:
        where_scope_sql, dataset_run_ref_sql, scope_params = _build_dataset_scope_sql(parent_run_id)

    sql = f"""
        SELECT
            c.id,
            c.canonical_name,
            c.domain,
            c.website,
            c.phone_primary,
            c.email_primary,
            c.contact_form_url,
            c.mailing_city,
            c.mailing_state,
            c.dataset_id,
            {dataset_run_ref_sql}
        FROM public.companies c
        LEFT JOIN public.datasets d
          ON d.id = c.dataset_id
        WHERE 1=1
        {where_scope_sql}
    """

    params: List[Any] = list(scope_params)

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


# =====================================================
# Expansion call wiring
# =====================================================

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
        p
        for p in params
        if p.default is inspect._empty
        and p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]

    if len(required_positional) == 1:
        return expand_fn(row)

    if len(required_positional) == 2:
        return expand_fn(row, parent_run_id)

    raise RuntimeError(
        "Unable to call expand_company_entity_v1 safely. "
        f"Detected signature: {sig}"
    )


# =====================================================
# Payload normalization
# =====================================================

def _iter_documents_from_payload(
    payload: Any,
    row: Dict[str, Any],
    parent_run_id: str,
) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []

    if payload is None:
        return docs

    if isinstance(payload, dict):
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
    out.setdefault("dataset_id", row.get("dataset_id"))
    out.setdefault("dataset_run_ref", row.get("dataset_run_ref"))

    return out


# =====================================================
# Main runner
# =====================================================

def run_company_expansion(
    parent_run_id: str,
    limit: int,
    company_ids: Optional[List[int]] = None,
    require_missing_enrichment: bool = False,
    restrict_to_parent_run_companies: bool = True,
) -> None:
    expand_company_entity_v1 = load_expand_fn()

    debug_pg_target(parent_run_id)

    rows = fetch_company_candidates(
        parent_run_id=parent_run_id,
        limit=limit,
        company_ids=company_ids,
        require_missing_enrichment=require_missing_enrichment,
        restrict_to_parent_run_companies=restrict_to_parent_run_companies,
    )

    print("======================================")
    print(" COMPANY ENTITY EXPANSION")
    print("======================================")
    print(f"parent_run_id: {parent_run_id}")
    print(f"limit: {limit}")
    print(f"require_missing_enrichment: {require_missing_enrichment}")
    print(f"restrict_to_parent_run_companies: {restrict_to_parent_run_companies}")
    print(f"selected_companies: {len(rows)}")

    if rows:
        print("\n[preview] selected companies:")
        for r in rows[:25]:
            print(
                f"  company_id={r['id']} "
                f"name={r.get('canonical_name')} "
                f"dataset_id={r.get('dataset_id')} "
                f"dataset_run_ref={r.get('dataset_run_ref')} "
                f"website={r.get('website')} "
                f"domain={r.get('domain')} "
                f"phone={r.get('phone_primary')} "
                f"email={r.get('email_primary')} "
                f"form={r.get('contact_form_url')}"
            )
    else:
        print(
            "[preview] no companies selected. "
            "If this is unexpected, verify that public.companies.dataset_id "
            "links to public.datasets and that datasets has either run_id "
            "or notes containing run_id=<parent_run_id>."
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
                    f"reason=no_prepared_documents"
                )
                continue

            raw_coll.insert_many(prepared_docs)
            written += len(prepared_docs)

            print(
                f"[ok] company_id={row['id']} "
                f"name={row.get('canonical_name')} "
                f"dataset_run_ref={row.get('dataset_run_ref')} "
                f"docs_written={len(prepared_docs)}"
            )

        except Exception as e:
            errors += 1
            print(
                f"[error] company_id={row.get('id')} "
                f"name={row.get('canonical_name')} "
                f"type={type(e).__name__} "
                f"msg={e}"
            )

    print("\n======================================")
    print(" COMPANY ENTITY EXPANSION COMPLETE")
    print("======================================")
    print(f"raw_expansions_written: {written}")
    print(f"skipped: {skipped}")
    print(f"errors: {errors}")
    print(f"parent_run_id: {parent_run_id}")
    print("Next: run ETL apply for the same parent_run_id.")


def main():
    parser = argparse.ArgumentParser(
        description="Run company entity expansion and write raw expansion docs to Mongo."
    )
    parser.add_argument(
        "--parent-run-id",
        required=True,
        help="Canonical parent run_id whose companies should be expanded.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of companies to expand.",
    )
    parser.add_argument(
        "--company-ids",
        type=str,
        default=None,
        help="Optional comma-separated company IDs for surgical reruns.",
    )
    parser.add_argument(
        "--require-missing-enrichment",
        action="store_true",
        help="Only select companies missing website/domain/phone/email/contact_form.",
    )

    parser.set_defaults(restrict_to_parent_run_companies=True)
    parser.add_argument(
        "--restrict-to-parent-run-companies",
        dest="restrict_to_parent_run_companies",
        action="store_true",
        help="Restrict selection to companies created by the supplied parent run. Default behavior.",
    )
    parser.add_argument(
        "--all-companies",
        dest="restrict_to_parent_run_companies",
        action="store_false",
        help="Disable run scoping and allow backlog-wide selection.",
    )

    args = parser.parse_args()

    run_company_expansion(
        parent_run_id=args.parent_run_id,
        limit=int(args.limit),
        company_ids=_parse_company_ids(args.company_ids),
        require_missing_enrichment=bool(args.require_missing_enrichment),
        restrict_to_parent_run_companies=bool(args.restrict_to_parent_run_companies),
    )


if __name__ == "__main__":
    main()