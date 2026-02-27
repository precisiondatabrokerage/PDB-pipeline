from dotenv import load_dotenv
load_dotenv()

import os
import hashlib
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


def load_clean_businesses(entities: list[dict]):
    """
    Load enriched + scored businesses into public.companies
    Canonical source for UI and API.
    """

    dsn = os.getenv("ACTIVE_POSTGRES_DSN")
    if not dsn:
        raise RuntimeError(
            "ACTIVE_POSTGRES_DSN not set. Use runners/run_pipeline_local.py or prod runner."
        )

    if not entities:
        print("⚠️ No entities provided — skipping Postgres load.")
        return

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    try:
        rows = []

        for e in entities:
            # -----------------------------
            # CRITICAL FIX: compute dedupe_key
            # -----------------------------
            dk = e.get("dedupe_key") or _dedupe_key(e)
            e["dedupe_key"] = dk

            rows.append((
                # dedupe key (UPSERt anchor)
                dk,

                # identity
                e.get("domain"),
                e.get("canonical_name"),
                e.get("alias_names"),

                # industry
                e.get("industry_type"),
                e.get("sub_industry"),

                # addresses
                e.get("mailing_address"),
                e.get("mailing_street"),
                e.get("mailing_city"),
                e.get("mailing_state"),
                e.get("mailing_zip"),
                e.get("physical_address_guess"),
                e.get("region"),
                e.get("lat"),
                e.get("lng"),

                # contact
                e.get("phone_primary"),
                e.get("phone_secondary"),
                e.get("email_primary"),
                e.get("email_secondary"),

                # website
                e.get("website"),
                (
                    e.get("website_status")
                    if isinstance(e.get("website_status"), int)
                    else None
                ),
                Json(e.get("website_tech_stack") or {}),
                e.get("contact_form_url"),

                # social / google
                e.get("facebook_url"),
                e.get("facebook_followers"),
                e.get("linkedin_url"),
                e.get("linkedin_employee_count"),
                e.get("google_reviews_rating"),
                e.get("google_reviews_count"),

                # business metadata
                e.get("business_status"),
                e.get("business_start_date"),
                e.get("registered_agent"),
                e.get("entity_type"),

                # behavioral intelligence
                Json(e.get("behavioral_signals") or {}),
                Json(e.get("lead_quality") or {}),

                # scalar score
                e.get("overall_lead_score"),

                # lifecycle
                e.get("first_seen"),
                e.get("last_seen"),
            ))

        sql = """
        INSERT INTO public.companies (
            dedupe_key,

            domain, canonical_name, alias_names,
            industry_type, sub_industry,

            mailing_address, mailing_street, mailing_city, mailing_state, mailing_zip,
            physical_address, region, lat, lng,

            phone_primary, phone_secondary, email_primary, email_secondary,

            website, website_status, website_tech_stack,
            contact_form_url,

            facebook_url, facebook_followers,
            linkedin_url, linkedin_employee_count,
            google_reviews_rating, google_reviews_count,

            business_status, business_start_date, registered_agent, entity_type,

            behavioral_signals,
            lead_quality,
            overall_lead_score,

            first_seen, last_seen
        )
        VALUES (
            %s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s
        )
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            domain = EXCLUDED.domain,
            canonical_name = EXCLUDED.canonical_name,
            alias_names = EXCLUDED.alias_names,
            industry_type = EXCLUDED.industry_type,
            sub_industry = EXCLUDED.sub_industry,

            mailing_address = EXCLUDED.mailing_address,
            mailing_street = EXCLUDED.mailing_street,
            mailing_city = EXCLUDED.mailing_city,
            mailing_state = EXCLUDED.mailing_state,
            mailing_zip = EXCLUDED.mailing_zip,
            physical_address = EXCLUDED.physical_address,
            region = EXCLUDED.region,
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng,

            phone_primary = EXCLUDED.phone_primary,
            phone_secondary = EXCLUDED.phone_secondary,
            email_primary = EXCLUDED.email_primary,
            email_secondary = EXCLUDED.email_secondary,

            website = EXCLUDED.website,
            website_status = EXCLUDED.website_status,
            website_tech_stack = EXCLUDED.website_tech_stack,
            contact_form_url = EXCLUDED.contact_form_url,

            facebook_url = EXCLUDED.facebook_url,
            facebook_followers = EXCLUDED.facebook_followers,
            linkedin_url = EXCLUDED.linkedin_url,
            linkedin_employee_count = EXCLUDED.linkedin_employee_count,
            google_reviews_rating = EXCLUDED.google_reviews_rating,
            google_reviews_count = EXCLUDED.google_reviews_count,

            business_status = EXCLUDED.business_status,
            business_start_date = EXCLUDED.business_start_date,
            registered_agent = EXCLUDED.registered_agent,
            entity_type = EXCLUDED.entity_type,

            behavioral_signals = EXCLUDED.behavioral_signals,
            lead_quality = EXCLUDED.lead_quality,
            overall_lead_score = EXCLUDED.overall_lead_score,

            last_seen = NOW();
        """

        execute_batch(cur, sql, rows, page_size=200)
        conn.commit()

        print(f"✅ Upserted {len(rows)} companies into public.companies")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
