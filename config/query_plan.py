from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .industry_packs import INDUSTRY_PACKS
from .source_profiles import SOURCE_PROFILES

CONFIG_DIR = Path(__file__).resolve().parent
MARKET_REGISTRY_CSV = CONFIG_DIR / "market_registry.csv"


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


def _slugify_city_state(city: str, state: str) -> str:
    base = f"{city}-{state}".strip().lower()
    out: List[str] = []
    for ch in base:
        if ch.isalnum():
            out.append(ch)
        elif ch in {" ", "_", "-", ","}:
            out.append("-")
    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def normalize_industry_entry(entry: Any) -> Dict[str, Any] | None:
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


def load_market_registry(csv_path: Path = MARKET_REGISTRY_CSV) -> List[Dict[str, Any]]:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Market registry not found: {csv_path}. "
            "Create config/market_registry.csv first."
        )

    rows: List[Dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            city = _norm_text(raw.get("city"))
            state = (_norm_text(raw.get("state")) or "").upper()
            market = _norm_text(raw.get("market")) or (
                f"{city}, {state}" if city and state else None
            )

            if not city or not state or not market:
                continue

            market_slug = (
                _norm_text(raw.get("market_slug"))
                or _slugify_city_state(city, state)
            )

            rows.append(
                {
                    "market_slug": market_slug,
                    "state": state,
                    "city": city,
                    "market": market,
                    "population": _safe_int(raw.get("population")),
                    "population_rank": _safe_int(raw.get("population_rank")),
                    "enabled": _truthy(raw.get("enabled"), True),
                    "default_pack": _norm_text(raw.get("default_pack")) or "home_services",
                    "source_profile": _norm_text(raw.get("source_profile")) or "standard_city",
                    "region": _norm_text(raw.get("region")),
                    "tier": _norm_text(raw.get("tier")),
                    "notes": _norm_text(raw.get("notes")),
                }
            )

    return rows


def resolve_pack(pack_name: str) -> List[Dict[str, Any]]:
    if pack_name not in INDUSTRY_PACKS:
        raise KeyError(f"Unknown industry pack: {pack_name}")
    return INDUSTRY_PACKS[pack_name]


def resolve_profile(profile_name: str) -> Dict[str, Any]:
    if profile_name not in SOURCE_PROFILES:
        raise KeyError(f"Unknown source profile: {profile_name}")
    return SOURCE_PROFILES[profile_name]


def apply_market_filters(
    rows: Iterable[Dict[str, Any]],
    *,
    state: Optional[str] = None,
    city: Optional[str] = None,
    region: Optional[str] = None,
    tier: Optional[str] = None,
    market_slugs: Optional[List[str]] = None,
    top_n: Optional[int] = None,
    batch_index: Optional[int] = None,
    batch_size: Optional[int] = None,
    min_population_rank: Optional[int] = None,
    max_population_rank: Optional[int] = None,
    include_disabled_markets: bool = False,
) -> List[Dict[str, Any]]:
    state = (_norm_text(state) or "").upper() or None
    city = (_norm_text(city) or "").lower() or None
    region = (_norm_text(region) or "").lower() or None
    tier = (_norm_text(tier) or "").lower() or None
    slug_set = {s.strip().lower() for s in (market_slugs or []) if str(s).strip()}

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if not include_disabled_markets and not row.get("enabled", True):
            continue
        if state and row["state"] != state:
            continue
        if city and row["city"].strip().lower() != city:
            continue
        if region and (row.get("region") or "").strip().lower() != region:
            continue
        if tier and (row.get("tier") or "").strip().lower() != tier:
            continue
        if slug_set and row["market_slug"].lower() not in slug_set:
            continue

        rank = row.get("population_rank")
        if min_population_rank is not None and (rank is None or rank < min_population_rank):
            continue
        if max_population_rank is not None and (rank is None or rank > max_population_rank):
            continue

        filtered.append(row)

    filtered.sort(
        key=lambda r: (
            r.get("population_rank") is None,
            r.get("population_rank") or 10**9,
            r.get("market", ""),
        )
    )

    if top_n is not None:
        filtered = filtered[: max(0, top_n)]

    if batch_size is not None:
        idx = batch_index or 0
        start = idx * batch_size
        end = start + batch_size
        filtered = filtered[start:end]

    return filtered


def build_targets(
    *,
    registry_rows: Optional[List[Dict[str, Any]]] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    region: Optional[str] = None,
    tier: Optional[str] = None,
    market_slugs: Optional[List[str]] = None,
    pack: Optional[str] = None,
    top_n: Optional[int] = None,
    batch_index: Optional[int] = None,
    batch_size: Optional[int] = None,
    min_population_rank: Optional[int] = None,
    max_population_rank: Optional[int] = None,
    include_disabled_markets: bool = False,
    max_query_pairs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rows = registry_rows or load_market_registry()

    selected_rows = apply_market_filters(
        rows,
        state=state,
        city=city,
        region=region,
        tier=tier,
        market_slugs=market_slugs,
        top_n=top_n,
        batch_index=batch_index,
        batch_size=batch_size,
        min_population_rank=min_population_rank,
        max_population_rank=max_population_rank,
        include_disabled_markets=include_disabled_markets,
    )

    targets: List[Dict[str, Any]] = []
    for row in selected_rows:
        pack_name = pack or row.get("default_pack") or "home_services"
        industries = resolve_pack(pack_name)
        profile = resolve_profile(row.get("source_profile") or "standard_city")
        yp_cfg = profile.get("yellowpages") or {}

        if not yp_cfg.get("enabled", True):
            continue

        for raw_industry in industries:
            industry = normalize_industry_entry(raw_industry)
            if not industry or not industry.get("enabled", True):
                continue

            targets.append(
                {
                    "market_slug": row["market_slug"],
                    "market": row["market"],
                    "city": row["city"],
                    "state": row["state"],
                    "region": row.get("region"),
                    "tier": row.get("tier"),
                    "population_rank": row.get("population_rank"),
                    "industry": industry["query"],
                    "industry_pack": pack_name,
                    "headless": bool(yp_cfg.get("headless", True)),
                    "max_pages": int(yp_cfg.get("max_pages", 1)),
                    "max_scrolls": int(yp_cfg.get("max_scrolls", 2)),
                }
            )

    if max_query_pairs is not None:
        targets = targets[: max(0, max_query_pairs)]

    return targets


__all__ = [
    "MARKET_REGISTRY_CSV",
    "normalize_industry_entry",
    "load_market_registry",
    "resolve_pack",
    "resolve_profile",
    "apply_market_filters",
    "build_targets",
]