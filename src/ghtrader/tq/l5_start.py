"""
TqSdk-based L5 start date discovery (hybrid: probe + cache + env override).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.config import get_runs_dir, get_tqsdk_auth
from ghtrader.data.trading_calendar import get_trading_days
from ghtrader.tq.catalog import get_contract_catalog
from ghtrader.util.json_io import read_json as _read_json, write_json_atomic as _write_json_atomic
from ghtrader.util.l5_detection import has_l5_df
from ghtrader.util.time import now_iso as _now_iso

log = structlog.get_logger()


@dataclass(frozen=True)
class L5StartResult:
    exchange: str
    variety: str
    l5_start_date: date
    l5_start_contract: str
    source: str
    created_at: str
    cached_at_unix: float
    contracts_checked: int
    probes_total: int


def _report_dir(runs_dir: Path) -> Path:
    return runs_dir / "control" / "reports" / "l5_start"


def _latest_path(runs_dir: Path, *, exchange: str, variety: str) -> Path:
    ex = str(exchange).upper().strip()
    v = str(variety).lower().strip()
    return _report_dir(runs_dir) / f"l5_start_exchange={ex}_var={v}_latest.json"


def _parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        s = str(value).strip()
        if not s:
            return None
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _infer_contract_start_from_symbol(symbol: str) -> date | None:
    """
    Best-effort fallback: infer YYYY-MM from suffix like cu2602 -> 2026-02-01.
    """
    try:
        s = str(symbol)
        if "." in s:
            s = s.split(".", 1)[-1]
        tail = ""
        for ch in reversed(s):
            if ch.isdigit():
                tail = ch + tail
            else:
                break
        if len(tail) != 4:
            return None
        yy = int(tail[:2])
        mm = int(tail[2:])
        year = 2000 + yy if yy < 70 else 1900 + yy
        if mm < 1 or mm > 12:
            return None
        return date(year, mm, 1)
    except Exception:
        return None


def _load_cached(
    *, runs_dir: Path, exchange: str, variety: str, ttl_s: float
) -> dict[str, Any] | None:
    p = _latest_path(runs_dir, exchange=exchange, variety=variety)
    obj = _read_json(p) if p.exists() else None
    if not isinstance(obj, dict):
        return None
    try:
        ts = float(obj.get("cached_at_unix") or 0.0)
        if ts > 0 and (time.time() - ts) <= float(ttl_s):
            return obj
    except Exception:
        return None
    return None


def _write_report(
    *, runs_dir: Path, exchange: str, variety: str, payload: dict[str, Any]
) -> Path | None:
    try:
        out_dir = _report_dir(runs_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_id = payload.get("run_id") or f"{int(time.time())}"
        path = out_dir / f"l5_start_exchange={exchange}_var={variety}_{run_id}.json"
        _write_json_atomic(path, payload)
        # Update latest pointer
        latest = _latest_path(runs_dir, exchange=exchange, variety=variety)
        _write_json_atomic(latest, payload)
        return path
    except Exception:
        return None


def _has_l5_on_day(api: Any, *, symbol: str, day: date) -> bool:
    try:
        df = api.get_tick_data_series(symbol=symbol, start_dt=day, end_dt=day + timedelta(days=1))
    except Exception:
        return False
    try:
        if df is None or df.empty:
            return False
        return bool(has_l5_df(df))
    except Exception:
        return False


def _probe_first_l5_day_for_contract(
    api: Any,
    *,
    symbol: str,
    trading_days: list[date],
    progress_every_s: float,
) -> tuple[date | None, int]:
    """
    Probe a contract's trading days to find the first L5 day.
    Returns (first_day_or_none, probes_used).
    """
    probes = 0
    if not trading_days:
        return None, probes

    # Quick check: if the last day has no L5, assume none for this contract.
    last_day = trading_days[-1]
    probes += 1
    if not _has_l5_on_day(api, symbol=symbol, day=last_day):
        return None, probes

    lo = 0
    hi = len(trading_days) - 1
    first_idx = hi
    last_log = time.time()
    while lo <= hi:
        mid = (lo + hi) // 2
        day = trading_days[mid]
        probes += 1
        has_l5 = _has_l5_on_day(api, symbol=symbol, day=day)
        if has_l5:
            first_idx = mid
            hi = mid - 1
        else:
            lo = mid + 1
        if (time.time() - last_log) >= progress_every_s:
            log.info(
                "l5_start.contract_probe",
                symbol=symbol,
                checked_day=day.isoformat(),
                probes=int(probes),
                range_start=trading_days[0].isoformat(),
                range_end=trading_days[-1].isoformat(),
            )
            last_log = time.time()

    if 0 <= first_idx < len(trading_days):
        return trading_days[first_idx], probes
    return None, probes


def resolve_l5_start_date(
    *,
    exchange: str,
    variety: str,
    end: date,
    data_dir: Path | None = None,
    runs_dir: Path | None = None,
    refresh: bool = False,
    refresh_catalog: bool = False,
) -> L5StartResult:
    """
    Resolve the earliest L5 trading day for a variety.

    Priority:
    1) Cached report (TTL)
    2) TqSdk probe
    """
    ex = str(exchange).upper().strip()
    v = str(variety).lower().strip()
    if data_dir is None:
        data_dir = Path("data")
    if runs_dir is None:
        runs_dir = get_runs_dir()

    try:
        ttl_s = float(os.environ.get("GHTRADER_L5_START_TTL_S", "86400") or "86400")
    except Exception:
        ttl_s = 86400.0
    ttl_s = max(60.0, float(ttl_s))

    if not refresh:
        cached = _load_cached(runs_dir=Path(runs_dir), exchange=ex, variety=v, ttl_s=ttl_s)
        if cached:
            d = _parse_date(cached.get("l5_start_date"))
            if d is not None:
                return L5StartResult(
                    exchange=ex,
                    variety=v,
                    l5_start_date=d,
                    l5_start_contract=str(cached.get("l5_start_contract") or ""),
                    source=str(cached.get("source") or "cache"),
                    created_at=str(cached.get("created_at") or ""),
                    cached_at_unix=float(cached.get("cached_at_unix") or 0.0),
                    contracts_checked=int(cached.get("contracts_checked") or 0),
                    probes_total=int(cached.get("probes_total") or 0),
                )

    # Probe via TqSdk
    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    try:
        progress_every_s = float(os.environ.get("GHTRADER_L5_PROBE_PROGRESS_S", "15") or "15")
    except Exception:
        progress_every_s = 15.0
    progress_every_s = max(5.0, float(progress_every_s))

    catalog = get_contract_catalog(exchange=ex, var=v, runs_dir=Path(runs_dir), refresh=bool(refresh_catalog))
    contracts = catalog.get("contracts") if isinstance(catalog, dict) else None
    if not isinstance(contracts, list):
        contracts = []
    if not contracts:
        raise RuntimeError(f"No contracts found for {ex}.{v} (catalog empty)")

    # Sort contracts by open_date or inferred symbol date
    def _contract_key(c: dict[str, Any]) -> tuple[date, str]:
        d0 = _parse_date(c.get("open_date")) or _infer_contract_start_from_symbol(str(c.get("symbol") or "")) or date(2099, 1, 1)
        return (d0, str(c.get("symbol") or ""))

    contracts_sorted = sorted([c for c in contracts if isinstance(c, dict)], key=_contract_key)

    auth = get_tqsdk_auth()
    api = TqApi(auth=auth, disable_print=True)
    probes_total = 0
    contracts_checked = 0
    l5_day: date | None = None
    l5_contract: str = ""
    try:
        for c in contracts_sorted:
            sym = str(c.get("symbol") or "").strip()
            if not sym:
                continue
            contracts_checked += 1
            c_start = _parse_date(c.get("open_date")) or _infer_contract_start_from_symbol(sym)
            c_end = _parse_date(c.get("expire_datetime")) or _parse_date(c.get("expire_date"))
            if c_end is None:
                c_end = end
            if c_start is None:
                # Fallback to 3-year window ending at c_end.
                c_start = max(date(2010, 1, 1), date(c_end.year - 3, 1, 1))
            if c_end < c_start:
                continue

            days = get_trading_days(market=ex, start=c_start, end=c_end, data_dir=Path(data_dir), refresh=False)
            if not days:
                continue

            log.info(
                "l5_start.contract_scan",
                symbol=sym,
                start=c_start.isoformat(),
                end=c_end.isoformat(),
                days=int(len(days)),
            )

            first_day, probes = _probe_first_l5_day_for_contract(
                api,
                symbol=sym,
                trading_days=days,
                progress_every_s=progress_every_s,
            )
            probes_total += int(probes)
            if first_day:
                l5_day = first_day
                l5_contract = sym
                break
    finally:
        try:
            api.close()
        except Exception:
            pass

    if l5_day is None:
        raise RuntimeError(f"Failed to detect L5 start date for {ex}.{v} (no L5 found).")

    payload = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "l5_start_date": l5_day.isoformat(),
        "l5_start_contract": l5_contract,
        "source": "probe",
        "created_at": _now_iso(),
        "cached_at_unix": float(time.time()),
        "contracts_checked": int(contracts_checked),
        "probes_total": int(probes_total),
    }
    _write_report(runs_dir=Path(runs_dir), exchange=ex, variety=v, payload=payload)
    return L5StartResult(
        exchange=ex,
        variety=v,
        l5_start_date=l5_day,
        l5_start_contract=l5_contract,
        source="probe",
        created_at=payload["created_at"],
        cached_at_unix=float(payload["cached_at_unix"]),
        contracts_checked=int(contracts_checked),
        probes_total=int(probes_total),
    )
