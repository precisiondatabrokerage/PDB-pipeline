from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


# =====================================================
# Helpers
# =====================================================

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(n)))

def _get(d: dict, path: str, default=None):
    """
    Safe nested getter: _get(obj, "website.has_contact_form")
    """
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def _is_web_reachable(status_code: Any) -> bool:
    try:
        code = int(status_code)
        return 200 <= code < 400
    except Exception:
        return False


# =====================================================
# v1 Scoring Rules (unchanged weights)
# =====================================================

def _score_contact_form(signals: dict) -> Tuple[int, List[str]]:
    status_code = _get(signals, "website.status_code")
    reachable = _is_web_reachable(status_code)
    has_form = bool(_get(signals, "website.has_contact_form", False))

    if not reachable:
        return 0, []
    if has_form:
        return 25, ["Website has an active contact form."]
    return 0, []

def _score_freshness(signals: dict) -> Tuple[int, List[str]]:
    status_code = _get(signals, "website.status_code")
    reachable = _is_web_reachable(status_code)
    days = _get(signals, "website.last_modified_days")

    if not reachable or days is None:
        return 0, []

    try:
        d = int(days)
    except Exception:
        return 0, []

    if d <= 30:
        return 15, ["Website appears recently updated (within 30 days)."]
    if d <= 90:
        return 10, ["Website updated within the last 90 days."]
    if d <= 180:
        return 5, ["Website updated within the last 180 days."]
    return 0, []

def _score_google_velocity(signals: dict) -> Tuple[int, List[str]]:
    v = _get(signals, "google.review_velocity_90d")
    if v is None:
        return 0, []

    try:
        v = int(v)
    except Exception:
        return 0, []

    if v >= 8:
        return 20, ["Strong recent Google review velocity (last 90 days)."]
    if v >= 4:
        return 15, ["Consistent recent Google review activity (last 90 days)."]
    if v >= 1:
        return 8, ["Some recent Google review activity (last 90 days)."]
    return 0, []

def _score_hours(signals: dict) -> Tuple[int, List[str]]:
    c = _get(signals, "profile.hours_completeness")
    if c is None:
        return 0, []

    try:
        c = float(c)
    except Exception:
        return 0, []

    if c >= 0.9:
        return 10, ["Business hours are fully configured."]
    if c >= 0.6:
        return 6, ["Business hours are mostly configured."]
    if c > 0:
        return 3, ["Business hours are partially configured."]
    return 0, []

def _score_social(signals: dict) -> Tuple[int, List[str]]:
    fb = _get(signals, "social.facebook_url")
    ig = _get(signals, "social.instagram_url")
    gmb_posts = bool(
        _get(signals, "social.gmb_posts_detected", False)
        or _get(signals, "google.has_recent_posts", False)
    )

    points = 0
    reasons = []

    if fb:
        points += 4
    if ig:
        points += 4
    if gmb_posts:
        points += 2

    points = _clamp(points, 0, 10)

    if points >= 8:
        reasons.append("Active presence on social platforms.")
    elif points >= 4:
        reasons.append("Some social presence detected.")
    elif points >= 2:
        reasons.append("Recent posting activity detected (social/GMB).")

    return points, reasons

def _score_domain_age(signals: dict) -> Tuple[int, List[str]]:
    age = _get(signals, "domain.age_years")
    if age is None:
        return 0, []

    try:
        age = float(age)
    except Exception:
        return 0, []

    if age >= 10:
        return 10, ["Established domain history (10+ years)."]
    if age >= 5:
        return 7, ["Established domain history (5+ years)."]
    if age >= 2:
        return 4, ["Domain history suggests stability (2+ years)."]
    return 2, ["Newer domain history detected."]

def _score_completeness_from_entity(entity: dict) -> Tuple[int, List[str]]:
    points = 0
    reasons = []

    if entity.get("phone_primary"):
        points += 2
    if entity.get("email_primary"):
        points += 2
    if entity.get("mailing_address") or entity.get("mailing_street"):
        points += 2
    if entity.get("website"):
        points += 2
    if entity.get("lat") is not None and entity.get("lng") is not None:
        points += 2

    points = _clamp(points, 0, 10)

    if points >= 8:
        reasons.append("Multiple verified contact fields available.")
    elif points >= 4:
        reasons.append("Some verified contact fields available.")

    return points, reasons


# =====================================================
# v2 Scoring + Explainability
# =====================================================

def score_business(entity: Dict[str, Any]) -> Dict[str, Any]:
    signals = entity.get("behavioral_signals") or {}

    factors: Dict[str, int] = {}
    explanation: List[str] = []

    breakdown = {
        "intent": [],
        "authority": [],
        "visibility_gaps": [],
        "negative": [],
    }

    attribution_sources = set(entity.get("enrichment_sources") or [])

    def add_breakdown(group, key, label, impact, sources):
        breakdown[group].append({
            "key": key,
            "label": label,
            "impact": impact,
            "source": sources,
        })

    # ---------------- Intent signals ----------------
    pts, reasons = _score_contact_form(signals)
    factors["website"] = pts
    explanation.extend(reasons)
    if pts > 0:
        add_breakdown(
            "intent",
            "contact_form",
            "Website has an active contact form",
            pts,
            ["website"],
        )
        attribution_sources.add("website")

    pts, reasons = _score_freshness(signals)
    factors["freshness"] = pts
    explanation.extend(reasons)
    if pts > 0:
        add_breakdown(
            "intent",
            "recent_updates",
            "Website recently updated",
            pts,
            ["website"],
        )
        attribution_sources.add("website")

    # ---------------- Authority signals ----------------
    pts, reasons = _score_domain_age(signals)
    factors["domain_age"] = pts
    explanation.extend(reasons)
    if pts > 0:
        add_breakdown(
            "authority",
            "domain_age",
            "Established domain history",
            pts,
            ["whois"],
        )

    pts, reasons = _score_google_velocity(signals)
    factors["google_velocity"] = pts
    explanation.extend(reasons)
    if pts > 0:
        add_breakdown(
            "authority",
            "google_review_velocity",
            "Recent Google review activity",
            pts,
            ["google_places"],
        )
        attribution_sources.add("google_places")

    # ---------------- Engagement / completeness ----------------
    pts, reasons = _score_hours(signals)
    factors["hours"] = pts
    explanation.extend(reasons)
    if pts > 0:
        add_breakdown(
            "visibility_gaps",
            "hours_configured",
            "Business hours configured",
            pts,
            ["google_places"],
        )

    pts, reasons = _score_social(signals)
    factors["social"] = pts
    explanation.extend(reasons)
    if pts > 0:
        add_breakdown(
            "visibility_gaps",
            "social_presence",
            "Social presence detected",
            pts,
            ["facebook", "instagram", "google_places"],
        )

    pts, reasons = _score_completeness_from_entity(entity)
    factors["completeness"] = pts
    explanation.extend(reasons)

    # ---------------- Negative signals (explicit) ----------------
    website_status = _get(signals, "website.status_code")
    if website_status is not None and not _is_web_reachable(website_status):
        add_breakdown(
            "negative",
            "website_unreachable",
            "Website unreachable during crawl",
            -5,
            ["website"],
        )

    review_velocity = _get(signals, "google.review_velocity_90d")
    if review_velocity is not None and int(review_velocity) == 0:
        add_breakdown(
            "negative",
            "low_review_velocity",
            "No recent Google review activity",
            -8,
            ["google_places"],
        )

    missing = _get(signals, "debug.missing_signals", []) or []
    for m in missing:
        add_breakdown(
            "negative",
            "missing_signal",
            f"Missing signal: {m.replace('.', ' ')}",
            0,
            [],
        )

    # ---------------- Final score ----------------
    score = _clamp(sum(factors.values()), 0, 100)

    if score >= 75:
        tier = "high"
    elif score >= 50:
        tier = "medium"
    else:
        tier = "low"

    lead_quality = {
        "version": "lq_v2",
        "score": score,
        "tier": tier,
        "explanation": explanation[:12],
        "factors": factors,
        "breakdown": breakdown,
        "attribution": {
            "summary": (
                "Derived from " + " + ".join(sorted(attribution_sources))
                if attribution_sources
                else None
            ),
            "sources": sorted(attribution_sources),
        },
        "scored_at": _now_iso(),
    }

    # ---------------- Persist ----------------
    entity["lead_quality"] = lead_quality
    entity["overall_lead_score"] = score
    entity["data_completeness_score"] = factors.get("completeness", 0)
    entity["authority_score"] = (
        factors.get("domain_age", 0) + factors.get("google_velocity", 0)
    )
    entity["engagement_score"] = (
        factors.get("website", 0)
        + factors.get("social", 0)
        + factors.get("freshness", 0)
    )

    return entity
