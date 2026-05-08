from __future__ import annotations

from typing import Any, Dict, List

WEB_TARGET_MATRIX: List[Dict[str, Any]] = [
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
                    "Property Management",
                    "HOA Management",
                    "Commercial Real Estate",
                    "Insurance Agencies",
                    "Roofing",
                    "Plumbers",
                    "Electricians",
                    "Contractors",
                    "Pest Control",
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
                    "Property Management",
                    "Roofing",
                    "Plumbers",
                    "Electricians",
                    "Contractors",
                    "Pest Control",
                    "Tree Service",
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
                    "Property Management",
                    "Commercial Real Estate",
                    "Insurance Agencies",
                    "Roofing",
                    "Plumbers",
                    "Electricians",
                    "Contractors",
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
                    "Property Management",
                    "Roofing",
                    "Plumbers",
                    "Electricians",
                    "Contractors",
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
                    "Property Management",
                    "Carpet Cleaning",
                    "Pest Control",
                    "AC Repair",
                    "Garage Door Repair",
                    "Tree Service",
                    "Contractors",
                ],
            }
        },
    },
]


def get_web_target_matrix() -> List[Dict[str, Any]]:
    return WEB_TARGET_MATRIX


def get_active_yellowpages_targets() -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []

    for market_cfg in WEB_TARGET_MATRIX:
        if not market_cfg.get("enabled", True):
            continue

        market = market_cfg["market"]
        yp_cfg = (market_cfg.get("sources") or {}).get("yellowpages") or {}

        if not yp_cfg.get("enabled", True):
            continue

        industries = yp_cfg.get("industries") or []

        for industry in industries:
            targets.append(
                {
                    "market": market,
                    "industry": industry,
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