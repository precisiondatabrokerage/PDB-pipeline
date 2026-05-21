from __future__ import annotations

import os
from typing import Any, Dict, List

DEFAULT_ACTIVE_SCRAPER_PRESET = "default_tn_services"

SCRAPER_PRESETS: Dict[str, List[Dict[str, Any]]] = {
    "default_tn_services": [
        {
            "market": "Knoxville, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 2,
                    "max_scrolls": 4,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "HOA Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                    ],
                }
            },
        },
        {
            "market": "Maryville, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 3,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                        {"query": "Tree Service", "enabled": True},
                    ],
                }
            },
        },
        {
            "market": "Chattanooga, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 3,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                    ],
                }
            },
        },
        {
            "market": "Johnson City, TN",
            "enabled": False,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 2,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                    ],
                }
            },
        },
        {
            "market": "Pigeon Forge, TN",
            "enabled": False,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 2,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "Carpet Cleaning", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                        {"query": "AC Repair", "enabled": True},
                        {"query": "Garage Door Repair", "enabled": True},
                        {"query": "Tree Service", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                    ],
                }
            },
        },
    ],
    "nashville_real_estate_agents": [
        {
            "market": "Nashville, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 2,
                    "max_scrolls": 3,
                    "industries": [
                    {"query": "Real Estate Agents", "enabled": True},
                    {"query": "Real Estate Buyer Brokers", "enabled": True},
                    {"query": "Real Estate Consultants", "enabled": True},
                    {"query": "Commercial Real Estate", "enabled": True},
                    {"query": "Real Estate Referral & Information Service", "enabled": True},
                    ],
                }
            },
        }
    ],
    "memphis_tn_services": [
        {
            "market": "Memphis, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 2,
                    "max_scrolls": 4,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "HOA Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                    ],
                }
            },
        }
    ],
    "franklin_tn_services": [
        {
            "market": "Franklin, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 3,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "HOA Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                    ],
                }
            },
        }
    ],
    "jackson_tn_services": [
        {
            "market": "Jackson, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 3,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                    ],
                }
            },
        }
    ],
    "hendersonville_tn_services": [
        {
            "market": "Hendersonville, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 3,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "HOA Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                    ],
                }
            },
        }
    ],
    "smyrna_tn_services": [
        {
            "market": "Smyrna, TN",
            "enabled": True,
            "sources": {
                "yellowpages": {
                    "enabled": True,
                    "headless": True,
                    "max_pages": 1,
                    "max_scrolls": 3,
                    "industries": [
                        {"query": "Property Management", "enabled": True},
                        {"query": "HOA Management", "enabled": True},
                        {"query": "Commercial Real Estate", "enabled": True},
                        {"query": "Insurance Agencies", "enabled": True},
                        {"query": "Roofing", "enabled": True},
                        {"query": "Plumbers", "enabled": True},
                        {"query": "Electricians", "enabled": True},
                        {"query": "Contractors", "enabled": True},
                        {"query": "Pest Control", "enabled": True},
                    ],
                }
            },
        }
    ],
}


def get_active_scraper_preset_name() -> str:
    requested = (
        os.getenv("ACTIVE_SCRAPER_PRESET", DEFAULT_ACTIVE_SCRAPER_PRESET)
        .strip()
        .lower()
    )

    if requested in SCRAPER_PRESETS:
        return requested

    return DEFAULT_ACTIVE_SCRAPER_PRESET


def get_web_target_matrix() -> List[Dict[str, Any]]:
    preset_name = get_active_scraper_preset_name()
    return SCRAPER_PRESETS[preset_name]


def list_scraper_presets() -> List[str]:
    return sorted(SCRAPER_PRESETS.keys())


def _normalize_industry_entry(entry: Any) -> Dict[str, Any] | None:
    if isinstance(entry, str):
        query = entry.strip()
        if not query:
            return None
        return {"query": query, "enabled": True}

    if isinstance(entry, dict):
        query = str(entry.get("query", "")).strip()
        if not query:
            return None
        return {
            "query": query,
            "enabled": bool(entry.get("enabled", True)),
        }

    return None


def get_active_yellowpages_targets() -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []

    for market_cfg in get_web_target_matrix():
        if not market_cfg.get("enabled", True):
            continue

        market = market_cfg["market"]
        yp_cfg = (market_cfg.get("sources") or {}).get("yellowpages") or {}

        if not yp_cfg.get("enabled", True):
            continue

        industries = yp_cfg.get("industries") or []

        for raw_industry in industries:
            industry = _normalize_industry_entry(raw_industry)
            if not industry:
                continue
            if not industry.get("enabled", True):
                continue

            targets.append(
                {
                    "market": market,
                    "industry": industry["query"],
                    "headless": bool(yp_cfg.get("headless", True)),
                    "max_pages": int(yp_cfg.get("max_pages", 1)),
                    "max_scrolls": int(yp_cfg.get("max_scrolls", 2)),
                }
            )

    return targets


def count_active_yellowpages_query_pairs() -> int:
    return len(get_active_yellowpages_targets())


def _derive_default_locations() -> List[str]:
    seen = set()
    ordered: List[str] = []

    for target in get_active_yellowpages_targets():
        market = target["market"]
        if market in seen:
            continue
        seen.add(market)
        ordered.append(market)

    return ordered


def _derive_default_industries() -> List[str]:
    seen = set()
    ordered: List[str] = []

    for target in get_active_yellowpages_targets():
        industry = target["industry"]
        key = industry.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(industry)

    return ordered


DEFAULT_YELLOWPAGES_LOCATIONS: List[str] = _derive_default_locations()
DEFAULT_YELLOWPAGES_INDUSTRIES: List[str] = _derive_default_industries()


def get_default_yellowpages_locations() -> List[str]:
    return list(DEFAULT_YELLOWPAGES_LOCATIONS)


def get_default_yellowpages_industries() -> List[str]:
    return list(DEFAULT_YELLOWPAGES_INDUSTRIES)