from __future__ import annotations

import re
from typing import Any, Iterable

from ghtrader.control.job_command import extract_cli_subcommands, infer_job_kind
from ghtrader.control.variety_context import (
    allowed_varieties as _allowed_varieties,
    infer_variety_from_symbol,
    is_supported_variety,
    normalize_variety,
)


def _parts(argv: Iterable[object] | None) -> list[str]:
    return [str(x).strip() for x in (argv or []) if str(x).strip()]


def _arg_values(parts: list[str], flag: str) -> list[str]:
    out: list[str] = []
    pfx = f"{flag}="
    for i, tok in enumerate(parts):
        if tok == flag and i + 1 < len(parts):
            nxt = str(parts[i + 1]).strip()
            if nxt and not nxt.startswith("-"):
                out.append(nxt)
        elif tok.startswith(pfx):
            raw = str(tok[len(pfx) :]).strip()
            if raw:
                out.append(raw)
    return out


def _first_arg(parts: list[str], flag: str) -> str:
    vals = _arg_values(parts, flag)
    return vals[0] if vals else ""


def _unique_keep_order(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        s = str(raw).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _infer_exchange_from_symbol(symbol: str) -> str:
    s = str(symbol or "").strip()
    if not s:
        return ""
    if "@" in s and "." in s:
        # KQ.m@SHFE.cu -> SHFE
        try:
            tail = s.split("@", 1)[1]
            ex = str(tail.split(".", 1)[0]).upper().strip()
            if ex:
                return ex
        except Exception:
            pass
    if "." in s:
        ex = str(s.split(".", 1)[0]).upper().strip()
        if ex:
            return ex
    return ""


def _normalize_exchange(raw: str) -> str:
    ex = str(raw or "").upper().strip()
    if not ex:
        return ""
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", ex):
        return ex
    return ""


def _normalize_supported_variety(raw: str | None) -> str:
    v = normalize_variety(raw, default=None)
    if not v:
        return ""
    if not is_supported_variety(v):
        return ""
    return v


def _infer_variety_from_title(title: str) -> str:
    t = str(title or "").strip()
    if not t:
        return ""
    from_symbol = infer_variety_from_symbol(t)
    if from_symbol:
        return from_symbol
    allowed = "|".join(re.escape(v) for v in _allowed_varieties())
    if not allowed:
        return ""
    m = re.search(rf"(^|\s)({allowed})(\s|$)", t.lower())
    if not m:
        return ""
    return str(m.group(2) or "").strip().lower()


def infer_job_metadata(*, argv: Iterable[object] | None, title: str = "") -> dict[str, Any]:
    """
    Best-effort structured job metadata extracted from command argv/title.
    """
    parts = _parts(argv)
    out: dict[str, Any] = {}
    if not parts:
        return out

    kind = infer_job_kind(parts)
    if kind and kind != "unknown":
        out["kind"] = str(kind)

    sub1, sub2 = extract_cli_subcommands(parts)
    if sub1:
        out["subcommand"] = str(sub1)
    if sub2:
        out["subcommand2"] = str(sub2)

    exchange = _normalize_exchange(_first_arg(parts, "--exchange"))
    if exchange:
        out["exchange"] = exchange

    account_profile = _first_arg(parts, "--account")
    if account_profile:
        out["account_profile"] = str(account_profile).strip()

    symbols = _unique_keep_order([*_arg_values(parts, "--symbol"), *_arg_values(parts, "--symbols")])
    if symbols:
        out["symbols"] = list(symbols)
        if len(symbols) == 1:
            out["symbol"] = symbols[0]
        if not exchange:
            ex2 = _normalize_exchange(_infer_exchange_from_symbol(symbols[0]))
            if ex2:
                out["exchange"] = ex2

    variety = _normalize_supported_variety(_first_arg(parts, "--var"))
    if not variety and symbols:
        inferred = _unique_keep_order(
            [str(v) for v in [infer_variety_from_symbol(s) for s in symbols] if str(v or "").strip()]
        )
        if len(inferred) == 1 and is_supported_variety(inferred[0]):
            variety = inferred[0]
    if not variety:
        variety = _normalize_supported_variety(_infer_variety_from_title(title))
    if variety:
        out["variety"] = variety

    return out


def merge_job_metadata(*metas: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Merge metadata dicts left-to-right; later dicts override earlier values.
    """
    out: dict[str, Any] = {}
    for m in metas:
        if not isinstance(m, dict):
            continue
        for k, v in m.items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            if isinstance(v, list) and not v:
                continue
            out[str(k)] = v
    return out or None
