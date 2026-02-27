-- RAW TABLE (same fields as Postgres)
CREATE TABLE IF NOT EXISTS raw_businesses (
    id BIGINT,
    source TEXT,
    source_id TEXT,
    scraped_at TIMESTAMP,
    raw_company_name TEXT,
    raw_address TEXT,
    raw_phone TEXT,
    raw_email TEXT,
    raw_website TEXT,
    raw_city TEXT,
    raw_state TEXT,
    raw_zip TEXT,
    lat DOUBLE,
    lng DOUBLE,
    raw_json TEXT
);

-- UNIFIED CLEAN TABLE (DuckDB-friendly types)
CREATE TABLE IF NOT EXISTS clean_businesses (
    business_id TEXT,
    canonical_name TEXT,
    alias_names TEXT,              -- JSON array as string
    industry_type TEXT,
    sub_industry TEXT,

    mailing_address TEXT,
    mailing_street TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    physical_address TEXT,
    region TEXT,
    lat DOUBLE,
    lng DOUBLE,

    phone_primary TEXT,
    phone_secondary TEXT,
    email_primary TEXT,
    email_secondary TEXT,

    website TEXT,
    website_status TEXT,
    website_tech_stack TEXT,       -- JSON as string
    contact_form_url TEXT,

    facebook_url TEXT,
    facebook_followers INTEGER,
    linkedin_url TEXT,
    linkedin_employee_count INTEGER,
    google_reviews_rating DOUBLE,
    google_reviews_count INTEGER,

    business_status TEXT,
    business_start_date DATE,
    registered_agent TEXT,
    entity_type TEXT,

    industry_attributes TEXT,      -- JSON as string

    data_completeness_score INTEGER,
    authority_score INTEGER,
    engagement_score INTEGER,
    overall_lead_score INTEGER,

    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    enrichment_sources TEXT         -- JSON as string
);
