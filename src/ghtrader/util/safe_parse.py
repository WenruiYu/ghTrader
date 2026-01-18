"""
Safe type coercion utilities.
"""

from __future__ import annotations

from typing import Any


def safe_int(x: Any) -> int | None:
    """Safely convert to int, returning None on failure."""
    try:
        return int(x)
    except Exception:
        return None


def safe_float(x: Any) -> float | None:
    """Safely convert to float, returning None on failure or NaN."""
    try:
        v = float(x)
        if v != v:  # NaN check
            return None
        return v
    except Exception:
        return None
