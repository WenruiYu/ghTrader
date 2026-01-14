from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from ghtrader.trading_calendar import get_trading_calendar, get_trading_days


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_yyyymmdd(s: str) -> date | None:
    try:
        return date.fromisoformat(str(s).strip())
    except Exception:
        return None


def _cache_root(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "cache" / "contract_status"


def _l5_cache_path(*, runs_dir: Path, lake_version: str, symbol: str) -> Path:
    lv = str(lake_version).strip().lower()
    sym = str(symbol).strip()
    safe = sym.replace("/", "_")
    return _cache_root(runs_dir=runs_dir) / "local_l5" / f"lake={lv}" / f"symbol={safe}.json"


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


def _ticks_symbol_dir(*, data_dir: Path, symbol: str, lake_version: str) -> Path:
    lv = str(lake_version).strip().lower()
    root = data_dir / ("lake_v2" if lv == "v2" else "lake")
    return root / "ticks" / f"symbol={symbol}"


def _no_data_dates_path(*, data_dir: Path, symbol: str, lake_version: str) -> Path:
    return _ticks_symbol_dir(data_dir=data_dir, symbol=symbol, lake_version=lake_version) / "_no_data_dates.json"


def _scan_date_dirs(symbol_dir: Path) -> tuple[set[str], str | None, str | None]:
    """
    Return (downloaded_dates_iso, min_date_iso, max_date_iso).
    """
    out: set[str] = set()
    min_s: str | None = None
    max_s: str | None = None
    if not symbol_dir.exists():
        return out, None, None
    try:
        for p in symbol_dir.iterdir():
            if not p.is_dir():
                continue
            name = p.name
            if not name.startswith("date="):
                continue
            ds = name.split("=", 1)[-1].strip()
            if not ds or len(ds) != 10:
                continue
            try:
                _ = date.fromisoformat(ds)
            except Exception:
                continue
            out.add(ds)
            if min_s is None or ds < min_s:
                min_s = ds
            if max_s is None or ds > max_s:
                max_s = ds
    except Exception:
        return out, None, None
    return out, min_s, max_s


def _read_json_list(path: Path) -> list[str]:
    try:
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, list):
            return []
        return [str(x) for x in obj]
    except Exception:
        return []


def _raw_contract_from_symbol(*, symbol: str, var: str) -> str | None:
    """
    'SHFE.cu2003' + 'cu' -> 'CU2003'
    """
    sym = str(symbol).strip()
    v = str(var).strip().lower()
    if not sym or not v:
        return None
    if "." not in sym:
        return None
    tail = sym.split(".", 1)[-1]
    if not tail.lower().startswith(v):
        return None
    yymm = tail[len(v) :].strip()
    if not (len(yymm) == 4 and yymm.isdigit()):
        # best-effort: take last 4 digits
        digits = "".join([c for c in tail if c.isdigit()])
        if len(digits) >= 4:
            yymm = digits[-4:]
        else:
            return None
    return f"{v.upper()}{yymm}"


def _last_trading_day_leq(cal: list[date], today: date) -> date:
    if not cal:
        # fallback approximation
        d = today
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d
    # calendar is sorted
    lo = 0
    hi = len(cal)
    while lo < hi:
        mid = (lo + hi) // 2
        if cal[mid] <= today:
            lo = mid + 1
        else:
            hi = mid
    idx = lo - 1
    return cal[idx] if idx >= 0 else today


def _parquet_file_has_local_l5(path: Path) -> bool:
    """
    Return True if a small sample shows level2-5 bid/ask price values that are not NaN.

    Important: our ingest pads missing depth columns with NaN (float), not NULL.
    """
    try:
        import numpy as np
        import pyarrow as pa  # noqa: F401
        import pyarrow.parquet as pq
    except Exception:
        return False

    try:
        pf = pq.ParquetFile(path)
        want = [
            "bid_price2",
            "ask_price2",
            "bid_price3",
            "ask_price3",
            "bid_price4",
            "ask_price4",
            "bid_price5",
            "ask_price5",
        ]
        cols = [c for c in want if c in pf.schema.names]
        if not cols:
            return False

        for batch in pf.iter_batches(batch_size=512, columns=cols):
            # Check only first batch for speed.
            for col in cols:
                arr = batch.column(col).to_numpy(zero_copy_only=False)  # type: ignore[attr-defined]
                if arr is None:
                    continue
                # L5-present should have finite positive prices.
                try:
                    if bool(np.isfinite(arr).any() and (arr > 0).any()):
                        return True
                except Exception:
                    # If dtype is odd, fall back to per-element check.
                    for x in arr:
                        try:
                            xf = float(x)
                            if xf > 0.0 and xf == xf:  # not NaN
                                return True
                        except Exception:
                            continue
            break
        return False
    except Exception:
        return False


def _pick_one_parquet(date_dir: Path) -> Path | None:
    try:
        files = sorted([p for p in date_dir.iterdir() if p.is_file() and p.suffix == ".parquet"])
        return files[0] if files else None
    except Exception:
        return None


@dataclass(frozen=True)
class LocalL5Summary:
    status: str  # ready|partial|missing_pyarrow|no_local_data
    l5_days: int
    scanned_days: int
    total_days: int
    l5_min: str | None
    l5_max: str | None


def compute_local_l5_summary(
    *,
    data_dir: Path,
    runs_dir: Path,
    symbol: str,
    lake_version: str,
    downloaded_dates: Iterable[str],
    refresh: bool = False,
    max_scan: int = 10,
) -> LocalL5Summary:
    """
    Incrementally scan local Parquet to detect L5 presence per date.

    Uses a persistent per-symbol cache file under runs/control/cache/.
    """
    ds_all = sorted({str(d).strip() for d in downloaded_dates if str(d).strip()})
    if not ds_all:
        return LocalL5Summary(status="no_local_data", l5_days=0, scanned_days=0, total_days=0, l5_min=None, l5_max=None)

    # If pyarrow isn't available, report gracefully.
    try:
        import pyarrow.parquet as pq  # noqa: F401
    except Exception:
        return LocalL5Summary(status="missing_pyarrow", l5_days=0, scanned_days=0, total_days=len(ds_all), l5_min=None, l5_max=None)

    p_cache = _l5_cache_path(runs_dir=runs_dir, lake_version=lake_version, symbol=symbol)
    cached = _read_json(p_cache) if not refresh else None
    dates_map: dict[str, bool] = {}
    if cached and isinstance(cached.get("dates"), dict):
        for k, v in cached["dates"].items():
            if isinstance(k, str):
                dates_map[k] = bool(v)

    missing = [d for d in ds_all if d not in dates_map]
    if refresh:
        missing = list(ds_all)

    # Scan a bounded number of missing dates to keep APIs responsive.
    scanned_now = 0
    sym_dir = _ticks_symbol_dir(data_dir=data_dir, symbol=symbol, lake_version=lake_version)
    for ds in missing:
        if scanned_now >= int(max_scan):
            break
        date_dir = sym_dir / f"date={ds}"
        p = _pick_one_parquet(date_dir)
        if p is None:
            # no parquet => treat as not l5 (but keep cache entry so we don't keep rescanning)
            dates_map[ds] = False
        else:
            dates_map[ds] = bool(_parquet_file_has_local_l5(p))
        scanned_now += 1

    try:
        payload = {
            "symbol": str(symbol),
            "lake_version": str(lake_version),
            "updated_at": _now_iso(),
            "updated_at_unix": float(time.time()),
            "dates": {k: bool(v) for k, v in sorted(dates_map.items())},
        }
        _write_json_atomic(p_cache, payload)
    except Exception:
        pass

    l5_dates = [d for d, has in dates_map.items() if has]
    l5_min = min(l5_dates) if l5_dates else None
    l5_max = max(l5_dates) if l5_dates else None
    status = "ready" if len(dates_map) >= len(ds_all) else "partial"
    return LocalL5Summary(
        status=status,
        l5_days=int(len(l5_dates)),
        scanned_days=int(len(dates_map)),
        total_days=int(len(ds_all)),
        l5_min=l5_min,
        l5_max=l5_max,
    )


@dataclass(frozen=True)
class ContractStatus:
    symbol: str
    raw_contract: str | None
    expired: bool | None
    expire_datetime: str | None
    local_days_downloaded: int
    local_min: str | None
    local_max: str | None
    days_expected: int | None
    days_done: int | None
    pct: float | None
    status: str  # not-downloaded|incomplete|complete|stale|unknown
    expected_first: str | None
    expected_last: str | None
    local_l5_status: str
    local_l5_days: int
    local_l5_min: str | None
    local_l5_max: str | None
    local_l5_scanned_days: int
    local_l5_total_days: int


def compute_contract_statuses(
    *,
    exchange: str,
    var: str,
    lake_version: str,
    data_dir: Path,
    runs_dir: Path,
    contracts: list[dict[str, Any]],
    refresh_l5: bool = False,
    max_l5_scan_per_symbol: int = 10,
) -> dict[str, Any]:
    """
    Compute per-contract local coverage/completeness + incremental local L5 scan.

    `contracts` is typically the `contracts` list from `ghtrader.tqsdk_catalog.get_contract_catalog()`.
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    lv = str(lake_version).lower().strip()
    lv = "v2" if lv == "v2" else "v1"

    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    today = datetime.now(timezone.utc).date()
    today_trading = _last_trading_day_leq(cal, today)

    out: list[dict[str, Any]] = []
    for c in contracts:
        sym = str(c.get("symbol") or "").strip()
        if not sym:
            continue
        expired = c.get("expired")
        expired_b = bool(expired) if expired is not None else None
        exp_dt = c.get("expire_datetime")
        exp_dt_s = str(exp_dt).strip() if exp_dt not in (None, "") else None

        raw = _raw_contract_from_symbol(symbol=sym, var=v)

        sym_dir = _ticks_symbol_dir(data_dir=data_dir, symbol=sym, lake_version=lv)
        downloaded_set, local_min, local_max = _scan_date_dirs(sym_dir)

        # Without akshare active-ranges, we compute a best-effort completeness over the
        # locally observed window:
        # - expected_first := local_min (if present)
        # - expected_last  := today_trading for active contracts, else local_max
        expected_first = date.fromisoformat(local_min) if local_min else None
        expected_last = None
        if expired_b is False:
            expected_last = today_trading if expected_first else None
        else:
            expected_last = date.fromisoformat(local_max) if local_max else None

        # Expected days + done days (downloaded + no-data within expected window).
        days_expected: int | None = None
        days_done: int | None = None
        pct: float | None = None

        if expected_first and expected_last and expected_last >= expected_first:
            expected_days = get_trading_days(market=ex, start=expected_first, end=expected_last, data_dir=data_dir, refresh=False)
            days_expected = int(len(expected_days))
            expected_iso0 = expected_first.isoformat()
            expected_iso1 = expected_last.isoformat()

            downloaded_in_range = {d for d in downloaded_set if expected_iso0 <= d <= expected_iso1}

            # No-data markers count as done (trading days only).
            no_data_raw = _read_json_list(_no_data_dates_path(data_dir=data_dir, symbol=sym, lake_version=lv))
            cal_set = set(cal) if cal else None
            no_data_effective = 0
            for ds in no_data_raw:
                ds_s = str(ds).strip()
                if not ds_s:
                    continue
                if ds_s < expected_iso0 or ds_s > expected_iso1:
                    continue
                d = _parse_yyyymmdd(ds_s)
                if d is None:
                    continue
                if cal_set is not None:
                    if d not in cal_set:
                        continue
                else:
                    if d.weekday() >= 5:
                        continue
                if ds_s in downloaded_in_range:
                    continue
                no_data_effective += 1

            done = int(len(downloaded_in_range) + no_data_effective)
            days_done = int(min(done, days_expected) if days_expected > 0 else done)
            pct = float(days_done / days_expected) if days_expected and days_expected > 0 else 0.0
            if pct < 0.0:
                pct = 0.0
            if pct > 1.0:
                pct = 1.0
        else:
            expected_iso0 = None
            expected_iso1 = None

        # Status
        status = "unknown"
        if not downloaded_set:
            status = "not-downloaded"
        elif days_expected is None or days_done is None:
            status = "unknown"
        else:
            if expired_b is False:
                # Active: allow distinct "stale" when it's behind today.
                if pct is not None and pct >= 1.0:
                    status = "complete"
                elif local_max and local_max < today_trading.isoformat():
                    status = "stale"
                else:
                    status = "incomplete"
            else:
                status = "complete" if pct is not None and pct >= 1.0 else "incomplete"

        # Local L5 detection (incremental cache)
        l5_sum = compute_local_l5_summary(
            data_dir=data_dir,
            runs_dir=runs_dir,
            symbol=sym,
            lake_version=lv,
            downloaded_dates=downloaded_set,
            refresh=bool(refresh_l5),
            max_scan=int(max_l5_scan_per_symbol),
        )

        out.append(
            {
                "symbol": sym,
                "raw_contract": raw,
                "expired": expired_b,
                "expire_datetime": exp_dt_s,
                "local_days_downloaded": int(len(downloaded_set)),
                "local_min": local_min,
                "local_max": local_max,
                "days_expected": days_expected,
                "days_done": days_done,
                "pct": pct,
                "status": status,
                "expected_first": expected_first.isoformat() if expected_first else None,
                "expected_last": expected_last.isoformat() if expected_last else None,
                "local_l5": {
                    "status": l5_sum.status,
                    "l5_days": int(l5_sum.l5_days),
                    "l5_min": l5_sum.l5_min,
                    "l5_max": l5_sum.l5_max,
                    "scanned_days": int(l5_sum.scanned_days),
                    "total_days": int(l5_sum.total_days),
                },
            }
        )

    # Sort by contract suffix when possible.
    def _sort_key(r: dict[str, Any]) -> tuple[int, str]:
        sym = str(r.get("symbol") or "")
        tail = ""
        for i in range(len(sym) - 1, -1, -1):
            if sym[i].isdigit():
                tail = sym[i] + tail
            else:
                break
        try:
            n = int(tail) if tail else 10**9
        except Exception:
            n = 10**9
        return (n, sym)

    out.sort(key=_sort_key)

    return {
        "ok": True,
        "exchange": ex,
        "var": v,
        "lake_version": lv,
        "generated_at": _now_iso(),
        "contracts": out,
        "active_ranges_available": False,
    }

