import duckdb
import json
from datetime import datetime

def load_raw_to_duckdb(records: list[dict], db_path="pdb.duckdb"):
    """
    Loads raw (scraped) business records into DuckDB.
    """
    con = duckdb.connect(db_path)

    rows = []
    now = datetime.utcnow()

    for r in records:
        rows.append((
            None,                   # id autoincrement substitute
            r.get("source"),
            r.get("source_id"),
            now,
            r.get("raw_company_name"),
            r.get("raw_address"),
            r.get("raw_phone"),
            r.get("raw_email"),
            r.get("raw_website"),
            r.get("raw_city"),
            r.get("raw_state"),
            r.get("raw_zip"),
            r.get("lat"),
            r.get("lng"),
            json.dumps(r.get("raw_json") or {})
        ))

    con.execute("""
        INSERT INTO raw_businesses (
            id,
            source, source_id, scraped_at,
            raw_company_name, raw_address, raw_phone, raw_email, raw_website,
            raw_city, raw_state, raw_zip,
            lat, lng, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    con.close()


def load_clean_to_duckdb(entities: list[dict], db_path="pdb.duckdb"):
    """
    Loads unified clean business records into DuckDB.
    """
    con = duckdb.connect(db_path)

    rows = []

    for e in entities:
        rows.append((
            e["business_id"],
            e.get("canonical_name"),
            json.dumps(e.get("alias_names") or []),
            e.get("industry_type"),
            e.get("sub_industry"),

            e.get("mailing_address"),
            e.get("mailing_street"),
            e.get("mailing_city"),
            e.get("mailing_state"),
            e.get("mailing_zip"),
            e.get("physical_address_guess"),
            e.get("region"),
            e.get("lat"),
            e.get("lng"),

            e.get("phone_primary"),
            e.get("phone_secondary"),
            e.get("email_primary"),
            e.get("email_secondary"),

            e.get("website"),
            e.get("website_status"),
            json.dumps(e.get("website_tech_stack") or {}),
            e.get("contact_form_url"),

            e.get("facebook_url"),
            e.get("facebook_followers"),
            e.get("linkedin_url"),
            e.get("linkedin_employee_count"),
            e.get("google_reviews_rating"),
            e.get("google_reviews_count"),

            e.get("business_status"),
            e.get("business_start_date"),
            e.get("registered_agent"),
            e.get("entity_type"),

            json.dumps(e.get("industry_attributes") or {}),

            e.get("data_completeness_score"),
            e.get("authority_score"),
            e.get("engagement_score"),
            e.get("overall_lead_score"),

            e.get("first_seen"),
            e.get("last_seen"),
            json.dumps(e.get("enrichment_sources") or [])
        ))

    con.execute("""
        INSERT INTO clean_businesses VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?
        )
    """, rows)

    con.close()
