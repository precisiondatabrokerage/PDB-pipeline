import uuid
import hashlib
from rapidfuzz import fuzz
from urllib.parse import urlparse
from math import radians, cos, sin, asin, sqrt


# =====================================================
# Tunable thresholds
# =====================================================
AUTO_MERGE_THRESHOLD = 85
SOFT_MERGE_THRESHOLD = 65

MAX_DISTANCE_KM_FOR_PHONE_MATCH = 5.0
MIN_NAME_SIMILARITY_FOR_DOMAIN_MATCH = 60


# =====================================================
# Normalizers
# =====================================================
def normalize_domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        host = urlparse(url if url.startswith("http") else f"https://{url}").hostname
        return host.replace("www.", "") if host else None
    except Exception:
        return None


def normalize_phone(phone: str | None) -> str | None:
    if not phone:
        return None
    digits = "".join(c for c in phone if c.isdigit())
    return digits if len(digits) >= 10 else None


def normalize_name(name: str | None) -> str | None:
    if not name:
        return None
    n = name.lower()
    for suffix in [" llc", " inc", " ltd", " co", " corporation", " company"]:
        n = n.replace(suffix, "")
    return n.strip()


# =====================================================
# Geo helpers
# =====================================================
def haversine_km(lat1, lon1, lat2, lon2) -> float | None:
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except Exception:
        return None

    r = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return r * c


# =====================================================
# Dedupe key (must match loader)
# =====================================================
def build_dedupe_key(entity: dict) -> str:
    domain = normalize_domain(entity.get("website") or entity.get("domain"))
    if domain:
        return domain

    name = normalize_name(entity.get("canonical_name")) or "unknown"
    street = (entity.get("mailing_street") or "").lower()
    zip_code = entity.get("mailing_zip") or ""

    base = f"{name}|{street}|{zip_code}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


# =====================================================
# Entity Resolution Engine (v2)
# =====================================================
def resolve_entities(records: list[dict]) -> list[dict]:
    """
    Production-safe entity resolution.

    Guarantees:
    - No blind domain merges
    - No phone merges across distance
    - Negative veto rules enforced
    - Stable dedupe_key per entity
    """

    entities: list[dict] = []

    for rec in records:
        raw_name = normalize_name(rec.get("raw_company_name"))
        raw_phone = normalize_phone(rec.get("raw_phone"))
        raw_domain = normalize_domain(rec.get("raw_website"))

        lat = rec.get("lat")
        lng = rec.get("lng")

        best_match = None
        best_score = 0
        best_explanation = []
        veto_reason = None

        for ent in entities:
            score = 0
            explanation = []

            ent_name = normalize_name(ent.get("canonical_name"))
            ent_phone = normalize_phone(ent.get("phone_primary"))
            ent_domain = normalize_domain(ent.get("website"))
            ent_lat = ent.get("lat")
            ent_lng = ent.get("lng")

            # ---------- NAME ----------
            if raw_name and ent_name:
                name_score = fuzz.token_sort_ratio(raw_name, ent_name)
                if name_score >= 85:
                    score += 30
                    explanation.append(f"name similarity {name_score}%")

            # ---------- DOMAIN (guarded) ----------
            if raw_domain and ent_domain and raw_domain == ent_domain:
                if raw_name and ent_name:
                    name_score = fuzz.token_sort_ratio(raw_name, ent_name)
                    if name_score < MIN_NAME_SIMILARITY_FOR_DOMAIN_MATCH:
                        veto_reason = "domain match but name conflict"
                        break
                score += 35
                explanation.append("exact domain match")

            # ---------- PHONE (guarded by distance) ----------
            if raw_phone and ent_phone and raw_phone == ent_phone:
                dist = haversine_km(lat, lng, ent_lat, ent_lng)
                if dist is not None and dist > MAX_DISTANCE_KM_FOR_PHONE_MATCH:
                    veto_reason = f"phone match but distance {dist:.1f}km"
                    break
                score += 30
                explanation.append("phone match")

            # ---------- ADDRESS ----------
            if (
                rec.get("mailing_zip")
                and rec.get("mailing_zip") == ent.get("mailing_zip")
            ):
                score += 10
                explanation.append("ZIP match")

            if score > best_score:
                best_score = score
                best_match = ent
                best_explanation = explanation

        # ---------- DECISION ----------
        if veto_reason:
            best_match = None

        if best_match and best_score >= SOFT_MERGE_THRESHOLD:
            best_match["records"].append(rec)
            best_match["merge_confidence"] = best_score
            best_match["merge_explanation"] = best_explanation

            if raw_name and raw_name not in best_match["alias_names"]:
                best_match["alias_names"].append(raw_name)

            if not best_match.get("phone_primary") and raw_phone:
                best_match["phone_primary"] = raw_phone

            if not best_match.get("website") and raw_domain:
                best_match["website"] = raw_domain

        else:
            entity = {
                "business_id": str(uuid.uuid4()),
                "canonical_name": rec.get("raw_company_name"),
                "alias_names": [rec.get("raw_company_name")] if rec.get("raw_company_name") else [],
                "phone_primary": raw_phone,
                "website": raw_domain,
                "mailing_street": rec.get("mailing_street"),
                "mailing_city": rec.get("mailing_city"),
                "mailing_state": rec.get("mailing_state"),
                "mailing_zip": rec.get("mailing_zip"),
                "lat": lat,
                "lng": lng,
                "records": [rec],
                "merge_confidence": 100,
                "merge_explanation": ["initial entity"],
            }

            entity["dedupe_key"] = build_dedupe_key(entity)
            entities.append(entity)

    return entities
