from __future__ import annotations

from typing import Any, Dict

SOURCE_PROFILES: Dict[str, Dict[str, Any]] = {
    "metro_heavy": {
        "yellowpages": {
            "enabled": True,
            "headless": True,
            "max_pages": 2,
            "max_scrolls": 4,
        }
    },
    "standard_city": {
        "yellowpages": {
            "enabled": True,
            "headless": True,
            "max_pages": 1,
            "max_scrolls": 3,
        }
    },
    "light_city": {
        "yellowpages": {
            "enabled": True,
            "headless": True,
            "max_pages": 1,
            "max_scrolls": 2,
        }
    },
}

__all__ = ["SOURCE_PROFILES"]