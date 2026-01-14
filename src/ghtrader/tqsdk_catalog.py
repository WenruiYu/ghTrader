from __future__ import annotations

import json
import lzma
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ghtrader.config import get_runs_dir, get_tqsdk_auth


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_root(*, runs_dir: Path | None = None) -> Path:
    rd = runs_dir or get_runs_dir()
    return rd / "control" / "cache" / "tqsdk_catalog"


def _cache_path(*, exchange: str, var: str, runs_dir: Path | None = None) -> Path:
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    return _cache_root(runs_dir=runs_dir) / f"contracts_exchange={ex}_var={v}.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _write_json_atomic(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".tmp-{uuid.uuid4().hex}")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(path)


def _is_fresh(obj: dict[str, Any], *, ttl_s: float) -> bool:
    try:
        ts = float(obj.get("cached_at_unix") or 0.0)
        return (time.time() - ts) <= float(ttl_s)
    except Exception:
        return False


def _sort_contract_symbols(symbols: list[str]) -> list[str]:
    """
    Sort symbols like 'SHFE.cu2003' by numeric suffix when possible.
    """

    def key(s: str) -> tuple[str, int]:
        st = str(s)
        # Keep exchange+var prefix stable, then numeric tail.
        tail = ""
        for i in range(len(st) - 1, -1, -1):
            if st[i].isdigit():
                tail = st[i] + tail
            else:
                break
        try:
            n = int(tail) if tail else 10**9
        except Exception:
            n = 10**9
        return (st[: -len(tail)] if tail else st, n)

    return sorted({str(s).strip() for s in symbols if str(s).strip()}, key=key)


def _maybe_epoch_seconds_to_iso(v: Any) -> str | None:
    """
    Best-effort convert epoch seconds to an ISO string.

    TqSdk `expire_datetime` is documented as a seconds timestamp.
    """
    if v in (None, ""):
        return None
    try:
        ts = float(v)
        if ts <= 0:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        # Keep a stable string fallback.
        try:
            return str(v)
        except Exception:
            return None


def _maybe_any_to_date_iso(v: Any) -> str | None:
    """
    Best-effort convert a value to an ISO date string (YYYY-MM-DD).

    TqSdk metadata may use:
    - ISO strings (YYYY-MM-DD or full datetimes)
    - YYYYMMDD strings
    - epoch seconds / ms / ns
    """
    if v in (None, ""):
        return None
    # String-ish
    try:
        s = str(v).strip()
    except Exception:
        s = ""
    if s:
        try:
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                # YYYY-MM-DD... (maybe datetime)
                return s[:10]
            if len(s) == 8 and s.isdigit():
                return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        except Exception:
            pass

    # Numeric-ish epoch: seconds / ms / ns
    try:
        x = float(v)
        if x <= 0:
            return None
        # Heuristics by magnitude.
        if x >= 1e14:
            dt = datetime.fromtimestamp(x / 1e9, tz=timezone.utc)
        elif x >= 1e12:
            dt = datetime.fromtimestamp(x / 1e3, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(x, tz=timezone.utc)
        return dt.date().isoformat()
    except Exception:
        return None


def _tqsdk_pre20_cache_path() -> Path | None:
    """
    Locate TqSdk's bundled pre-2020 instrument cache (expired_quotes.json.lzma).
    """
    try:
        import tqsdk  # type: ignore

        pkg_dir = Path(getattr(tqsdk, "__file__", "") or "").resolve().parent
        if not str(pkg_dir):
            return None
        p = pkg_dir / "expired_quotes.json.lzma"
        return p if p.exists() else None
    except Exception:
        return None


def _load_pre20_quotes_from_path(path: Path) -> dict[str, dict[str, Any]]:
    try:
        with lzma.open(str(path), "rt", encoding="utf-8") as f:
            obj = json.loads(f.read())
        if not isinstance(obj, dict):
            return {}
        out: dict[str, dict[str, Any]] = {}
        for k, v in obj.items():
            if isinstance(k, str) and isinstance(v, dict):
                out[str(k)] = dict(v)
        return out
    except Exception:
        return {}


def _pre20_contracts(*, exchange: str, var: str) -> dict[str, dict[str, Any]]:
    """
    Return a symbol->info mapping for older instruments from TqSdk's bundled cache.
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    p = _tqsdk_pre20_cache_path()
    if p is None:
        return {}
    quotes = _load_pre20_quotes_from_path(p)
    out: dict[str, dict[str, Any]] = {}
    for sym, q in quotes.items():
        try:
            ex_id = str(q.get("exchange_id") or "").upper().strip()
            ins_class = str(q.get("ins_class") or "").upper().strip()
            prod = str(q.get("product_id") or "").lower().strip()
            if not prod:
                # Fallback: infer from instrument id prefix (e.g. SHFE.cu2001 -> cu)
                if "." in sym:
                    tail = sym.split(".", 1)[-1]
                    letters = ""
                    for ch in tail:
                        if ch.isalpha():
                            letters += ch
                        else:
                            break
                    prod = letters.lower()

            if ex_id != ex:
                continue
            if ins_class != "FUTURE":
                continue
            if prod != v:
                continue

            expired = q.get("expired")
            exp_dt = _maybe_epoch_seconds_to_iso(q.get("expire_datetime"))
            out[sym] = {
                "symbol": sym,
                "instrument_id": sym,
                "expired": bool(expired) if expired is not None else None,
                "expire_datetime": exp_dt,
                "open_date": None,
                "catalog_source": "pre20_cache",
            }
        except Exception:
            continue
    return out


@dataclass(frozen=True)
class ContractInfo:
    symbol: str
    exchange: str
    variety: str
    instrument_id: str
    expired: bool | None
    expire_datetime: str | None
    open_date: str | None
    catalog_source: str


def fetch_tqsdk_contracts(
    *,
    exchange: str,
    var: str,
    timeout_s: float = 20.0,
) -> tuple[list[ContractInfo], dict[str, Any] | None]:
    """
    Fetch contract list + symbol info from TqSdk (network).

    Returns:
      (contracts, error_dict_or_none)
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    try:
        from tqsdk import TqApi  # type: ignore

        auth = get_tqsdk_auth()
        api = TqApi(auth=auth, disable_print=True)
    except Exception as e:
        return [], {"error": f"tqsdk_init_failed: {e}"}

    try:
        pre20 = _pre20_contracts(exchange=ex, var=v)

        # Query all futures for exchange+product.
        quotes = api.query_quotes(ins_class="FUTURE", exchange_id=ex, product_id=v)
        if not isinstance(quotes, list):
            try:
                quotes = list(quotes)  # type: ignore[arg-type]
            except Exception:
                quotes = []

        # Query symbol info in one shot when possible.
        info_map: dict[str, dict[str, Any]] = {}
        try:
            info_rows = api.query_symbol_info(quotes)
            # query_symbol_info returns a DataFrame-like object.
            try:
                import pandas as pd  # type: ignore

                if isinstance(info_rows, pd.DataFrame):
                    df = info_rows
                    if "instrument_id" not in df.columns:
                        df = df.reset_index()
                    if "instrument_id" in df.columns:
                        for _, rr in df.iterrows():
                            ins_id = str(rr.get("instrument_id") or "").strip()
                            if ins_id:
                                info_map[ins_id] = {k: rr.get(k) for k in df.columns}
            except Exception:
                pass
        except Exception:
            info_map = {}

        # Merge: start with pre-2020 cache, overlay contract-service results.
        merged: dict[str, dict[str, Any]] = dict(pre20)
        for sym in _sort_contract_symbols([str(s) for s in quotes]):
            ins_id = sym
            r = info_map.get(ins_id, {})
            expired = r.get("expired")
            exp_dt = _maybe_epoch_seconds_to_iso(r.get("expire_datetime"))
            open_dt: str | None = None
            for k in ("open_date", "open_datetime", "start_date", "start_datetime", "listed_date", "listing_date"):
                if k in r and r.get(k) not in (None, ""):
                    open_dt = _maybe_any_to_date_iso(r.get(k))
                    if open_dt:
                        break
            merged[sym] = {
                "symbol": sym,
                "instrument_id": ins_id,
                "expired": bool(expired) if expired is not None else None,
                "expire_datetime": exp_dt,
                "open_date": open_dt,
                "catalog_source": "contract_service",
            }

        out: list[ContractInfo] = []
        for sym in _sort_contract_symbols(list(merged.keys())):
            r = merged.get(sym) or {}
            out.append(
                ContractInfo(
                    symbol=sym,
                    exchange=ex,
                    variety=v,
                    instrument_id=str(r.get("instrument_id") or sym),
                    expired=(r.get("expired") if r.get("expired") is None else bool(r.get("expired"))),
                    expire_datetime=str(r.get("expire_datetime")) if r.get("expire_datetime") not in (None, "") else None,
                    open_date=str(r.get("open_date")) if r.get("open_date") not in (None, "") else None,
                    catalog_source=str(r.get("catalog_source") or "unknown"),
                )
            )

        return out, None
    finally:
        try:
            api.close()
        except Exception:
            pass


def get_contract_catalog(
    *,
    exchange: str,
    var: str,
    runs_dir: Path | None = None,
    ttl_s: float = 3600.0,
    refresh: bool = False,
) -> dict[str, Any]:
    """
    Return a cached contract catalog:
      {
        ok: bool,
        exchange: ...,
        var: ...,
        cached_at: iso,
        cached_at_unix: float,
        source: "cache"|"live",
        contracts: [ {symbol, expired, expire_datetime, ...}, ... ],
        error: str?
      }
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    p = _cache_path(exchange=ex, var=v, runs_dir=runs_dir)

    if not refresh:
        cached = _read_json(p)
        if cached and _is_fresh(cached, ttl_s=ttl_s):
            cached = dict(cached)
            cached.setdefault("source", "cache")
            cached.setdefault("exchange", ex)
            cached.setdefault("var", v)
            cached.setdefault("ok", bool(cached.get("ok", True)))
            return cached

    contracts, err = fetch_tqsdk_contracts(exchange=ex, var=v)
    if err is not None:
        # If live fetch fails but we have any cache, return stale cache with error.
        cached = _read_json(p)
        if cached:
            cached = dict(cached)
            cached["ok"] = False
            cached["source"] = "cache"
            cached["error"] = str(err.get("error") or "unknown_error")
            return cached
        return {"ok": False, "exchange": ex, "var": v, "contracts": [], "error": str(err.get("error") or "unknown_error")}

    payload = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "cached_at": _now_iso(),
        "cached_at_unix": float(time.time()),
        "source": "live",
        "contracts": [
            {
                "symbol": c.symbol,
                "instrument_id": c.instrument_id,
                "expired": c.expired,
                "expire_datetime": c.expire_datetime,
                "open_date": c.open_date,
                "catalog_source": c.catalog_source,
            }
            for c in contracts
        ],
    }
    try:
        _write_json_atomic(p, payload)
    except Exception:
        # Cache failures should not break callers.
        pass
    return payload

