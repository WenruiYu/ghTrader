from __future__ import annotations

import re
from typing import Iterable

ALLOWED_VARIETIES: tuple[str, ...] = ("cu", "au", "ag")
DEFAULT_VARIETY: str = "cu"


def allowed_varieties() -> tuple[str, ...]:
    return ALLOWED_VARIETIES


def normalize_variety(raw: str | None, *, default: str | None = DEFAULT_VARIETY) -> str:
    v = str(raw or "").strip().lower()
    if v:
        return v
    if default is None:
        return ""
    return str(default).strip().lower()


def is_supported_variety(raw: str | None) -> bool:
    return normalize_variety(raw, default=None) in ALLOWED_VARIETIES


def require_supported_variety(raw: str | None) -> str:
    v = normalize_variety(raw, default=None)
    if v not in ALLOWED_VARIETIES:
        raise ValueError(f"unsupported variety: {raw}")
    return v


def default_variety() -> str:
    return DEFAULT_VARIETY


def derived_symbol_for_variety(variety: str, *, exchange: str = "SHFE") -> str:
    v = require_supported_variety(variety)
    ex = str(exchange).upper().strip() or "SHFE"
    return f"KQ.m@{ex}.{v}"


def parse_varieties(values: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    for x in values or []:
        v = normalize_variety(x, default=None)
        if v in ALLOWED_VARIETIES and v not in out:
            out.append(v)
    return out


def symbol_matches_variety(symbol: str | None, variety: str | None) -> bool:
    v = normalize_variety(variety, default=None)
    if not v or v not in ALLOWED_VARIETIES:
        return False
    s = str(symbol or "").strip().lower()
    if not s:
        return False
    # Fast path for continuous main alias like KQ.m@SHFE.cu
    if f"@shfe.{v}" in s:
        return True
    # Contract/code path like SHFE.cu2602 (or surrounded in a larger string)
    pat = rf"(^|[^a-z0-9])shfe\.{re.escape(v)}[0-9]*([^a-z0-9]|$)"
    return re.search(pat, s) is not None


def infer_variety_from_symbol(symbol: str | None) -> str | None:
    s = str(symbol or "").strip()
    if not s:
        return None
    for v in ALLOWED_VARIETIES:
        if symbol_matches_variety(s, v):
            return v
    return None
