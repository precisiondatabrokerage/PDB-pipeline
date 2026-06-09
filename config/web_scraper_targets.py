from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from .query_plan import build_targets
from .industry_packs import INDUSTRY_PACKS
from .source_profiles import SOURCE_PROFILES

DEFAULT_ACTIVE_SCRAPER_PRESET = "default_tn_services"
DEFAULT_ACTIVE_MODE = "campaign"

# --------------------------------------------------
# Campaign presets
# These are curated filters over the registry.
# --------------------------------------------------

CAMPAIGN_PRESETS: Dict[str, Dict[str, Any]] = {
    # --------------------------------------------------
    # Default / legacy presets
    # --------------------------------------------------
    "default_tn_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": [
            "nashville-tn",
            "memphis-tn",
            "knoxville-tn",
            "chattanooga-tn",
            "clarksville-tn",
            "murfreesboro-tn",
            "franklin-tn",
            "jackson-tn",
            "johnson-city-tn",
            "hendersonville-tn",
            "smyrna-tn",
            "maryville-tn",
            "cleveland-tn",
            "oak-ridge-tn",
            "morristown-tn",
            "sevierville-tn",
            "cookeville-tn",
            "bartlett-tn",
        ],
        "max_query_pairs": 220,
    },
    "nashville_real_estate_agents": {
        "mode": "registry",
        "state": "TN",
        "pack": "real_estate_agents",
        "market_slugs": ["nashville-tn"],
        "max_query_pairs": 50,
    },
    "memphis_tn_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": ["memphis-tn"],
        "max_query_pairs": 50,
    },
    "franklin_tn_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": ["franklin-tn"],
        "max_query_pairs": 50,
    },
    "jackson_tn_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": ["jackson-tn"],
        "max_query_pairs": 50,
    },
    "hendersonville_tn_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": ["hendersonville-tn"],
        "max_query_pairs": 50,
    },
    "smyrna_tn_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": ["smyrna-tn"],
        "max_query_pairs": 50,
    },

    # --------------------------------------------------
    # Daily workflow presets
    # --------------------------------------------------
    "tn_core_home_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": [
            "knoxville-tn",
            "maryville-tn",
            "chattanooga-tn",
            "cleveland-tn",
            "oak-ridge-tn",
            "morristown-tn",
            "sevierville-tn",
            "cookeville-tn",
            "johnson-city-tn",
        ],
        "max_query_pairs": 140,
    },
    "tn_middle_west_home_services": {
        "mode": "registry",
        "state": "TN",
        "pack": "home_services",
        "market_slugs": [
            "nashville-tn",
            "franklin-tn",
            "murfreesboro-tn",
            "clarksville-tn",
            "hendersonville-tn",
            "smyrna-tn",
            "memphis-tn",
            "jackson-tn",
            "bartlett-tn",
        ],
        "max_query_pairs": 140,
    },
    "tn_real_estate_and_property": {
        "mode": "registry",
        "state": "TN",
        "pack": "real_estate_agents",
        "market_slugs": [
            "nashville-tn",
            "knoxville-tn",
            "chattanooga-tn",
            "memphis-tn",
            "franklin-tn",
            "murfreesboro-tn",
            "clarksville-tn",
            "johnson-city-tn",
        ],
        "max_query_pairs": 100,
    },

    # --------------------------------------------------
    # Southeast expansion presets
    # --------------------------------------------------
    "nc_home_services": {
        "mode": "registry",
        "state": "NC",
        "pack": "home_services",
        "market_slugs": [
            "charlotte-nc",
            "raleigh-nc",
            "greensboro-nc",
            "durham-nc",
            "winston-salem-nc",
            "fayetteville-nc",
            "cary-nc",
            "wilmington-nc",
            "high-point-nc",
            "asheville-nc",
        ],
        "max_query_pairs": 140,
    },
    "ga_home_services": {
        "mode": "registry",
        "state": "GA",
        "pack": "home_services",
        "market_slugs": [
            "atlanta-ga",
            "augusta-ga",
            "columbus-ga",
            "macon-ga",
            "savannah-ga",
            "athens-ga",
            "sandy-springs-ga",
            "roswell-ga",
            "alpharetta-ga",
            "marietta-ga",
        ],
        "max_query_pairs": 140,
    },
    "ky_home_services": {
        "mode": "registry",
        "state": "KY",
        "pack": "home_services",
        "market_slugs": [
            "louisville-ky",
            "lexington-ky",
            "bowling-green-ky",
            "owensboro-ky",
            "covington-ky",
            "richmond-ky",
            "georgetown-ky",
            "florence-ky",
            "hopkinsville-ky",
            "elizabethtown-ky",
        ],
        "max_query_pairs": 120,
    },
    "southeast_home_services": {
        "mode": "registry",
        "pack": "home_services",
        "market_slugs": [
            "charlotte-nc",
            "raleigh-nc",
            "greensboro-nc",
            "durham-nc",
            "winston-salem-nc",
            "asheville-nc",
            "atlanta-ga",
            "augusta-ga",
            "savannah-ga",
            "athens-ga",
            "louisville-ky",
            "lexington-ky",
            "bowling-green-ky",
            "owensboro-ky",
        ],
        "max_query_pairs": 180,
    },
    "southeast_real_estate_and_property": {
        "mode": "registry",
        "pack": "real_estate_agents",
        "market_slugs": [
            "charlotte-nc",
            "raleigh-nc",
            "asheville-nc",
            "atlanta-ga",
            "savannah-ga",
            "louisville-ky",
            "lexington-ky",
        ],
        "max_query_pairs": 100,
    },

    # --------------------------------------------------
    # Experimental / optional presets
    # --------------------------------------------------
    "northwestern_oregon_car_dealerships": {
        "mode": "registry",
        "state": "OR",
        "pack": "car_dealerships",
        "market_slugs": [
            "portland-or",
            "salem-or",
            "gresham-or",
            "hillsboro-or",
            "beaverton-or",
        ],
        "max_query_pairs": 40,
    },
}


# --------------------------------------------------
# Helpers
# --------------------------------------------------


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


def _norm_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def list_scraper_presets() -> List[str]:
    return sorted(CAMPAIGN_PRESETS.keys())


def get_active_scraper_preset_name() -> str:
    requested = (
        os.getenv("ACTIVE_SCRAPER_PRESET", DEFAULT_ACTIVE_SCRAPER_PRESET)
        .strip()
        .lower()
    )
    if requested in CAMPAIGN_PRESETS:
        return requested
    return DEFAULT_ACTIVE_SCRAPER_PRESET


def _build_targets_from_campaign_preset(preset_name: str) -> List[Dict[str, Any]]:
    preset = CAMPAIGN_PRESETS[preset_name]
    if preset.get("mode") != "registry":
        raise ValueError(f"Unsupported preset mode: {preset.get('mode')}")

    return build_targets(
        state=preset.get("state"),
        city=preset.get("city"),
        region=preset.get("region"),
        tier=preset.get("tier"),
        market_slugs=preset.get("market_slugs"),
        pack=preset.get("pack"),
        top_n=preset.get("top_n"),
        batch_index=preset.get("batch_index"),
        batch_size=preset.get("batch_size"),
        min_population_rank=preset.get("min_population_rank"),
        max_population_rank=preset.get("max_population_rank"),
        include_disabled_markets=bool(preset.get("include_disabled_markets", False)),
        max_query_pairs=preset.get("max_query_pairs"),
    )


def get_active_yellowpages_targets() -> List[Dict[str, Any]]:
    """
    Runtime modes:

    campaign mode:
      ACTIVE_MODE=campaign
      ACTIVE_SCRAPER_PRESET=default_tn_services

    registry mode:
      ACTIVE_MODE=registry
      ACTIVE_STATE=TN
      ACTIVE_PACK=home_services
      ACTIVE_TOP_N=25
      ACTIVE_BATCH_INDEX=0
      ACTIVE_BATCH_SIZE=25
      ACTIVE_MAX_QUERY_PAIRS=200
    """
    mode = (_norm_text(os.getenv("ACTIVE_MODE")) or DEFAULT_ACTIVE_MODE).strip().lower()

    if mode == "registry":
        market_slugs_raw = _norm_text(os.getenv("ACTIVE_MARKET_SLUGS"))
        market_slugs = (
            [s.strip().lower() for s in market_slugs_raw.split(",") if s.strip()]
            if market_slugs_raw
            else None
        )

        return build_targets(
            state=os.getenv("ACTIVE_STATE"),
            city=os.getenv("ACTIVE_CITY"),
            region=os.getenv("ACTIVE_REGION"),
            tier=os.getenv("ACTIVE_TIER"),
            market_slugs=market_slugs,
            pack=_norm_text(os.getenv("ACTIVE_PACK")) or None,
            top_n=_safe_int(os.getenv("ACTIVE_TOP_N")),
            batch_index=_safe_int(os.getenv("ACTIVE_BATCH_INDEX"), 0),
            batch_size=_safe_int(os.getenv("ACTIVE_BATCH_SIZE")),
            min_population_rank=_safe_int(os.getenv("ACTIVE_MIN_POP_RANK")),
            max_population_rank=_safe_int(os.getenv("ACTIVE_MAX_POP_RANK")),
            include_disabled_markets=_truthy(
                os.getenv("ACTIVE_INCLUDE_DISABLED_MARKETS"),
                False,
            ),
            max_query_pairs=_safe_int(os.getenv("ACTIVE_MAX_QUERY_PAIRS")),
        )

    preset_name = get_active_scraper_preset_name()
    return _build_targets_from_campaign_preset(preset_name)


def count_active_yellowpages_query_pairs() -> int:
    return len(get_active_yellowpages_targets())


def get_web_target_matrix() -> List[Dict[str, Any]]:
    """
    Backward-compatible alias.
    Returns the active target rows after registry/preset expansion.
    """
    return get_active_yellowpages_targets()


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


__all__ = [
    "INDUSTRY_PACKS",
    "SOURCE_PROFILES",
    "DEFAULT_ACTIVE_SCRAPER_PRESET",
    "DEFAULT_ACTIVE_MODE",
    "CAMPAIGN_PRESETS",
    "list_scraper_presets",
    "get_active_scraper_preset_name",
    "get_active_yellowpages_targets",
    "count_active_yellowpages_query_pairs",
    "get_web_target_matrix",
    "get_default_yellowpages_locations",
    "get_default_yellowpages_industries",
]