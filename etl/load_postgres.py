from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import os
import hashlib
from typing import Any, Optional

import psycopg2
from psycopg2.extras import execute_batch, Json


# =====================================================
# Dedupe key helper (MUST match resolver logic)
# =====================================================
def _dedupe_key(e: dict) -> str:
    domain = (e.get("domain") or e.get("website") or "").strip().lower()
    if domain:
        return domain

    name = (e.get("canonical_name") or "unknown").strip().lower()
    street = (e.get("mailing_street") or "").strip().lower()
    zip_code = (e.get("mailing_zip") or "").strip()

    base = f"{name}|{street}|{zip_code}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def _pick_dataset_id(entity: dict, dataset_id: Optional[str]) -> Optional[str]:
    if dataset_id:
        return str(dataset_id)

    ent_dataset_id = entity.get("dataset_id")
    if ent_dataset_id:
        return str(ent_dataset_id)

    return None


def _coerce_int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def load_clean_businesses(
    entities: list[dict],
    dataset_id: Optional[str] = None,
):
    """
    Legacy pipeline-side canonical company loader.

    Notes:
    - This is NOT the active Day 2 web ETL path.
    - It is still useful as a deterministic loader and should write dataset_id correctly.
    """

    dsn = (
        os.getenv("ACTIVE_POSTGRES_DSN")
        or os.getenv("SUPABASE_POSTGRES_URL")
        or os.getenv("POSTGRES_DSN")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "No Postgres DSN found. Set ACTIVE_POSTGRES_DSN or SUPABASE_POSTGRES_URL or POSTGRES_DSN or DATABASE_URL."
        )

    if not entities:
        print("No entities provided. Skipping Postgres load.")
        return {
            "inserted_or_updated": 0,
            "dataset_id": str(dataset_id) if dataset_id else None,
        }

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    try:
        rows = []

        for e in entities:
            dk = e.get("dedupe_key") or _dedupe_key(e)
            e["dedupe_key"] = dk

            row_dataset_id = _pick_dataset_id(e, dataset_id)

            rows.append((
                dk,                                             # 1 dedupe_key

                e.get("domain"),                                # 2
                e.get("canonical_name"),                        # 3
                e.get("alias_names"),                           # 4

                e.get("industry_type"),                         # 5
                e.get("sub_industry"),                          # 6

                e.get("mailing_address"),                       # 7
                e.get("mailing_street"),                        # 8
                e.get("mailing_city"),                          # 9
                e.get("mailing_state"),                         # 10
                e.get("mailing_zip"),                           # 11
                e.get("physical_address_guess") or e.get("physical_address"),  # 12
                e.get("region"),                                # 13
                e.get("lat"),                                   # 14
                e.get("lng"),                                   # 15

                e.get("phone_primary"),                         # 16
                e.get("phone_secondary"),                       # 17
                e.get("email_primary"),                         # 18
                e.get("email_secondary"),                       # 19

                e.get("website"),                               # 20
                _coerce_int_or_none(e.get("website_status")),   # 21
                Json(e.get("website_tech_stack") or {}),        # 22
                e.get("contact_form_url"),                      # 23

                e.get("facebook_url"),                          # 24
                e.get("facebook_followers"),                    # 25
                e.get("linkedin_url"),                          # 26
                e.get("linkedin_employee_count"),               # 27
                e.get("google_reviews_rating"),                 # 28
                e.get("google_reviews_count"),                  # 29

                e.get("business_status"),                       # 30
                e.get("business_start_date"),                   # 31
                e.get("registered_agent"),                      # 32
                e.get("entity_type"),                           # 33

                Json(e.get("behavioral_signals") or {}),        # 34
                Json(e.get("lead_quality") or {}),              # 35
                e.get("overall_lead_score"),                    # 36

                e.get("first_seen"),                            # 37
                e.get("last_seen"),                             # 38

                row_dataset_id,                                 # 39
            ))

        sql = """
        INSERT INTO public.companies (
            dedupe_key,

            domain,
            canonical_name,
            alias_names,

            industry_type,
            sub_industry,

            mailing_address,
            mailing_street,
            mailing_city,
            mailing_state,
            mailing_zip,
            physical_address,
            region,
            lat,
            lng,

            phone_primary,
            phone_secondary,
            email_primary,
            email_secondary,

            website,
            website_status,
            website_tech_stack,
            contact_form_url,

            facebook_url,
            facebook_followers,
            linkedin_url,
            linkedin_employee_count,
            google_reviews_rating,
            google_reviews_count,

            business_status,
            business_start_date,
            registered_agent,
            entity_type,

            behavioral_signals,
            lead_quality,
            overall_lead_score,

            first_seen,
            last_seen,

            dataset_id
        )
        VALUES (
            %s,

            %s, %s, %s,

            %s, %s,

            %s, %s, %s, %s, %s, %s, %s, %s, %s,

            %s, %s, %s, %s,

            %s, %s, %s, %s,

            %s, %s, %s, %s, %s, %s,

            %s, %s, %s, %s,

            %s, %s, %s,

            %s, %s,

            %s::uuid
        )
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            domain = COALESCE(EXCLUDED.domain, public.companies.domain),
            canonical_name = COALESCE(EXCLUDED.canonical_name, public.companies.canonical_name),

            alias_names = CASE
                WHEN EXCLUDED.alias_names IS NULL THEN public.companies.alias_names
                WHEN public.companies.alias_names IS NULL THEN EXCLUDED.alias_names
                ELSE (
                    SELECT array_agg(DISTINCT x)
                    FROM unnest(public.companies.alias_names || EXCLUDED.alias_names) AS t(x)
                )
            END,

            industry_type = COALESCE(EXCLUDED.industry_type, public.companies.industry_type),
            sub_industry = COALESCE(EXCLUDED.sub_industry, public.companies.sub_industry),

            mailing_address = COALESCE(EXCLUDED.mailing_address, public.companies.mailing_address),
            mailing_street = COALESCE(EXCLUDED.mailing_street, public.companies.mailing_street),
            mailing_city = COALESCE(EXCLUDED.mailing_city, public.companies.mailing_city),
            mailing_state = COALESCE(EXCLUDED.mailing_state, public.companies.mailing_state),
            mailing_zip = COALESCE(EXCLUDED.mailing_zip, public.companies.mailing_zip),
            physical_address = COALESCE(EXCLUDED.physical_address, public.companies.physical_address),
            region = COALESCE(EXCLUDED.region, public.companies.region),
            lat = COALESCE(EXCLUDED.lat, public.companies.lat),
            lng = COALESCE(EXCLUDED.lng, public.companies.lng),

            phone_primary = COALESCE(EXCLUDED.phone_primary, public.companies.phone_primary),
            phone_secondary = COALESCE(EXCLUDED.phone_secondary, public.companies.phone_secondary),
            email_primary = COALESCE(EXCLUDED.email_primary, public.companies.email_primary),
            email_secondary = COALESCE(EXCLUDED.email_secondary, public.companies.email_secondary),

            website = COALESCE(EXCLUDED.website, public.companies.website),
            website_status = COALESCE(EXCLUDED.website_status, public.companies.website_status),
            website_tech_stack = CASE
                WHEN EXCLUDED.website_tech_stack IS NULL THEN public.companies.website_tech_stack
                WHEN EXCLUDED.website_tech_stack = '{}'::jsonb AND public.companies.website_tech_stack IS NOT NULL THEN public.companies.website_tech_stack
                ELSE EXCLUDED.website_tech_stack
            END,
            contact_form_url = COALESCE(EXCLUDED.contact_form_url, public.companies.contact_form_url),

            facebook_url = COALESCE(EXCLUDED.facebook_url, public.companies.facebook_url),
            facebook_followers = COALESCE(EXCLUDED.facebook_followers, public.companies.facebook_followers),
            linkedin_url = COALESCE(EXCLUDED.linkedin_url, public.companies.linkedin_url),
            linkedin_employee_count = COALESCE(EXCLUDED.linkedin_employee_count, public.companies.linkedin_employee_count),
            google_reviews_rating = COALESCE(EXCLUDED.google_reviews_rating, public.companies.google_reviews_rating),
            google_reviews_count = COALESCE(EXCLUDED.google_reviews_count, public.companies.google_reviews_count),

            business_status = COALESCE(EXCLUDED.business_status, public.companies.business_status),
            business_start_date = COALESCE(EXCLUDED.business_start_date, public.companies.business_start_date),
            registered_agent = COALESCE(EXCLUDED.registered_agent, public.companies.registered_agent),
            entity_type = COALESCE(EXCLUDED.entity_type, public.companies.entity_type),

            behavioral_signals = CASE
                WHEN EXCLUDED.behavioral_signals IS NULL THEN public.companies.behavioral_signals
                WHEN EXCLUDED.behavioral_signals = '{}'::jsonb AND public.companies.behavioral_signals IS NOT NULL THEN public.companies.behavioral_signals
                ELSE EXCLUDED.behavioral_signals
            END,

            lead_quality = CASE
                WHEN EXCLUDED.lead_quality IS NULL THEN public.companies.lead_quality
                WHEN EXCLUDED.lead_quality = '{}'::jsonb AND public.companies.lead_quality IS NOT NULL THEN public.companies.lead_quality
                ELSE EXCLUDED.lead_quality
            END,

            overall_lead_score = COALESCE(EXCLUDED.overall_lead_score, public.companies.overall_lead_score),

            first_seen = COALESCE(public.companies.first_seen, EXCLUDED.first_seen),
            last_seen = COALESCE(EXCLUDED.last_seen, public.companies.last_seen),

            dataset_id = COALESCE(EXCLUDED.dataset_id, public.companies.dataset_id)
        """

        execute_batch(cur, sql, rows, page_size=500)
        conn.commit()

        print(
            f"Loaded {len(rows)} companies into public.companies "
            f"(dataset_id={str(dataset_id) if dataset_id else 'entity_or_null'})."
        )

        return {
            "inserted_or_updated": len(rows),
            "dataset_id": str(dataset_id) if dataset_id else None,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def finalize_dataset(dataset_id: str, record_count: int):
    dsn = (
        os.getenv("ACTIVE_POSTGRES_DSN")
        or os.getenv("SUPABASE_POSTGRES_URL")
        or os.getenv("POSTGRES_DSN")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "No Postgres DSN found. Set ACTIVE_POSTGRES_DSN or SUPABASE_POSTGRES_URL or POSTGRES_DSN or DATABASE_URL."
        )

    sql = """
    UPDATE public.datasets
    SET record_count = %s
    WHERE id = %s
    """

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    try:
        cur.execute(sql, (record_count, str(dataset_id)))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()