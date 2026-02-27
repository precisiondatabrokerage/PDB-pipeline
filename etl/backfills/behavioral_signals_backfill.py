import os
from datetime import datetime, timezone
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor, Json

load_dotenv()

# ======================================================
# CONFIG
# ======================================================

TEST_DOMAINS = [
    "elitepropertyknox.com",
    "midtownpm.com",
    "budgetrentalsllc.com",
    "legacytestco.com",
]

# ======================================================
# HELPERS
# ======================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def tier_from_score(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"

def compute_behavioral_signals(company: dict):
    """
    Behavioral Signal Engine v1
    Conservative, explainable, null-safe.
    """

    website = company.get("website")
    website_status = company.get("website_status")
    contact_form_url = company.get("contact_form_url")

    reviews = company.get("google_reviews_count") or 0
    rating = float(company.get("google_reviews_rating") or 0)

    facebook_url = company.get("facebook_url")

    # ----------------------------
    # Behavioral Signals
    # ----------------------------
    signals = {
        "version": "bs_v1",
        "computed_at": now_iso(),
        "website": {
            "has_website": bool(website),
            "website_status": website_status,
            "has_contact_form": bool(contact_form_url),
            "contact_form_url": contact_form_url,
        },
        "google": {
            "reviews_count": reviews,
            "rating": rating,
        },
        "social": {
            "facebook_url": facebook_url,
            "has_social": bool(facebook_url),
        },
    }

    # ----------------------------
    # Lead Quality Scoring
    # ----------------------------
    score = 0
    explanation = []

    if website:
        score += 15
        explanation.append("Website present")

    if contact_form_url:
        score += 20
        explanation.append("Website has a contact form")

    if isinstance(website_status, int) and 200 <= website_status < 400:
        score += 10
        explanation.append("Website responds successfully")

    if reviews >= 50:
        score += 20
        explanation.append("Strong Google review volume (50+)")
    elif reviews >= 10:
        score += 10
        explanation.append("Moderate Google review volume (10+)")
    elif reviews > 0:
        score += 5
        explanation.append("Some Google reviews present")

    if rating >= 4.5:
        score += 10
        explanation.append("Excellent Google rating (4.5+)")
    elif rating >= 4.0:
        score += 6
        explanation.append("Good Google rating (4.0+)")

    if facebook_url:
        score += 10
        explanation.append("Active Facebook presence")

    score = max(0, min(100, score))

    lead_quality = {
        "version": "lq_v1",
        "computed_at": signals["computed_at"],
        "score": score,
        "tier": tier_from_score(score),
        "explanation": explanation[:8],
    }

    return signals, lead_quality

# ======================================================
# MAIN
# ======================================================

def main():
    # 🚨 FORCE SUPABASE — NO FALLBACKS
    dsn = os.getenv("SUPABASE_POSTGRES_URL")

    if not dsn:
        raise RuntimeError(
            "SUPABASE_POSTGRES_URL is not set. "
            "Add it to PDB-pipeline/.env using the Supabase pooler URL."
        )

    conn = psycopg2.connect(
        dsn,
        sslmode="require",
        cursor_factory=RealDictCursor
    )

    try:
        with conn.cursor() as cur:
            # 🔍 Sanity check: show which DB we're connected to
            cur.execute(
                "SELECT current_database(), inet_server_addr(), inet_server_port();"
            )
            print("🧠 Connected to DB:", cur.fetchone())

            # --------------------------------------------------
            # Fetch test companies
            # --------------------------------------------------
            cur.execute(
                """
                SELECT
                    id,
                    domain,
                    website,
                    website_status,
                    contact_form_url,
                    google_reviews_count,
                    google_reviews_rating,
                    facebook_url
                FROM public.companies
                WHERE domain = ANY(%s)
                   OR replace(domain, 'www.', '') = ANY(%s)
                """,
                (TEST_DOMAINS, TEST_DOMAINS),
            )

            companies = cur.fetchall()

            if not companies:
                print("⚠️ No matching companies found in Supabase.")
                return

            updates = []

            for company in companies:
                signals, lead_quality = compute_behavioral_signals(company)
                updates.append((
                    Json(signals),
                    Json(lead_quality),
                    company["id"],
                ))

            # --------------------------------------------------
            # Update companies
            # --------------------------------------------------
            cur.executemany(
                """
                UPDATE public.companies
                SET
                    behavioral_signals = %s,
                    lead_quality = %s
                WHERE id = %s
                """,
                updates,
            )

            conn.commit()

            print(
                f"✅ Backfilled behavioral_signals + lead_quality "
                f"for {len(updates)} companies."
            )

    finally:
        conn.close()

# ======================================================
# ENTRY
# ======================================================

if __name__ == "__main__":
    main()
