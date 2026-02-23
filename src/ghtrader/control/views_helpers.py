from __future__ import annotations

from typing import Any


def safe_int(raw: Any, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        val = int(str(raw).strip())
    except Exception:
        val = int(default)
    if min_value is not None:
        val = max(int(min_value), val)
    if max_value is not None:
        val = min(int(max_value), val)
    return int(val)
