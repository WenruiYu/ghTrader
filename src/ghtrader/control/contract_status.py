from __future__ import annotations

from bisect import bisect_left, bisect_right
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from ghtrader.trading_calendar import get_trading_calendar


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
    lv = "v2"
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


def _tick_bounds_cache_path(*, runs_dir: Path, lake_version: str, symbol: str, day: str) -> Path:
    lv = "v2"
    sym = str(symbol).strip()
    safe = sym.replace("/", "_")
    ds = str(day).strip()
    return _cache_root(runs_dir=runs_dir) / "local_tick_bounds" / f"lake={lv}" / f"symbol={safe}" / f"date={ds}.json"


def _parquet_files_fingerprint(date_dir: Path) -> list[dict[str, Any]]:
    try:
        files = sorted([p for p in date_dir.iterdir() if p.is_file() and p.suffix == ".parquet"], key=lambda x: x.name)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for p in files:
        try:
            st = p.stat()
            out.append({"name": p.name, "mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))), "size": int(st.st_size)})
        except Exception:
            continue
    return out


def _ns_to_iso_utc(ns: int | None) -> str | None:
    if ns is None:
        return None
    try:
        if int(ns) <= 0:
            return None
        return datetime.fromtimestamp(float(int(ns)) / 1_000_000_000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _parquet_datetime_ns_bounds(path: Path) -> tuple[int | None, int | None]:
    """
    Best-effort (min_ns, max_ns) for the `datetime` column of a Parquet file.

    Prefer Parquet row-group statistics; fall back to reading only the first and last row groups.
    """
    try:
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except Exception:
        return None, None

    try:
        pf = pq.ParquetFile(path)
        if "datetime" not in pf.schema.names:
            return None, None

        # 1) Prefer row-group statistics (fast).
        try:
            idx = list(pf.schema.names).index("datetime")
            md = pf.metadata
            mn: int | None = None
            mx: int | None = None
            if md is not None:
                for i in range(md.num_row_groups):
                    st = md.row_group(i).column(idx).statistics
                    if not st:
                        continue
                    try:
                        a = st.min
                        b = st.max
                    except Exception:
                        continue
                    try:
                        ai = int(a) if a is not None else None
                        bi = int(b) if b is not None else None
                    except Exception:
                        continue
                    if ai is not None:
                        mn = ai if mn is None else min(mn, ai)
                    if bi is not None:
                        mx = bi if mx is None else max(mx, bi)
            if mn is not None or mx is not None:
                return mn, mx
        except Exception:
            pass

        # 2) Fallback: read only first+last row group for `datetime`.
        nrg = int(getattr(pf, "num_row_groups", 0) or 0)
        if nrg <= 0:
            return None, None
        try:
            t0 = pf.read_row_group(0, columns=["datetime"])
            a0 = t0.column(0).combine_chunks()
            mn0 = pc.min(a0).as_py() if len(a0) else None
        except Exception:
            mn0 = None
        try:
            t1 = pf.read_row_group(nrg - 1, columns=["datetime"])
            a1 = t1.column(0).combine_chunks()
            mx1 = pc.max(a1).as_py() if len(a1) else None
        except Exception:
            mx1 = None
        try:
            return (int(mn0) if mn0 is not None else None), (int(mx1) if mx1 is not None else None)
        except Exception:
            return None, None
    except Exception:
        return None, None


def compute_local_tick_bounds_for_day(
    *,
    data_dir: Path,
    runs_dir: Path,
    symbol: str,
    lake_version: str,
    day: str,
    refresh: bool,
) -> dict[str, Any] | None:
    """
    Return best-effort local min/max tick timestamps for (symbol, day).

    Uses a small cache keyed by Parquet file fingerprints.
    """
    lv = "v2"
    _ = lake_version
    sym = str(symbol).strip()
    ds = str(day).strip()
    if not sym or not ds:
        return None

    date_dir = _ticks_symbol_dir(data_dir=data_dir, symbol=sym, lake_version=lv) / f"date={ds}"
    if not date_dir.exists():
        return None

    fp = _parquet_files_fingerprint(date_dir)
    if not fp:
        return None

    cache_path = _tick_bounds_cache_path(runs_dir=runs_dir, lake_version=lv, symbol=sym, day=ds)
    if not refresh:
        cached = _read_json(cache_path)
        try:
            if cached and cached.get("files") == fp and (cached.get("min_datetime_ns") is not None or cached.get("max_datetime_ns") is not None):
                return cached
        except Exception:
            pass

    # Compute across all parquet parts for this day.
    mn: int | None = None
    mx: int | None = None
    for item in fp:
        try:
            p = date_dir / str(item.get("name") or "")
            a, b = _parquet_datetime_ns_bounds(p)
            if a is not None:
                mn = a if mn is None else min(mn, a)
            if b is not None:
                mx = b if mx is None else max(mx, b)
        except Exception:
            continue

    payload = {
        "symbol": sym,
        "trading_day": ds,
        "lake_version": lv,
        "files": fp,
        "min_datetime_ns": mn,
        "max_datetime_ns": mx,
        "min_datetime_ts": _ns_to_iso_utc(mn),
        "max_datetime_ts": _ns_to_iso_utc(mx),
        "computed_at": _now_iso(),
    }
    try:
        _write_json_atomic(cache_path, payload)
    except Exception:
        pass
    return payload


def _ticks_symbol_dir(*, data_dir: Path, symbol: str, lake_version: str) -> Path:
    _ = lake_version  # v2-only
    return data_dir / "lake_v2" / "ticks" / f"symbol={symbol}"


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


def _count_weekdays_between(start: date, end: date) -> int:
    """
    Count weekdays in [start, end] inclusive (Mon-Fri).
    """
    if end < start:
        return 0
    n = (end - start).days + 1
    full_weeks, rem = divmod(n, 7)
    count = full_weeks * 5
    for i in range(rem):
        if (start.weekday() + i) % 7 < 5:
            count += 1
    return int(count)


def _count_trading_days_between(*, cal: list[date], start: date, end: date) -> int:
    """
    Count trading days in [start, end] using cached calendar when available,
    otherwise fall back to weekday-only.
    """
    if end < start:
        return 0
    if cal:
        i0 = bisect_left(cal, start)
        i1 = bisect_right(cal, end)
        return int(max(0, i1 - i0))
    return _count_weekdays_between(start, end)


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
    questdb_cov_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Compute per-contract local coverage/completeness + incremental local L5 scan.

    `contracts` is typically the `contracts` list from `ghtrader.tqsdk_catalog.get_contract_catalog()`.
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    _ = lake_version  # v2-only
    lv = "v2"

    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    today = datetime.now(timezone.utc).date()
    today_trading = _last_trading_day_leq(cal, today)
    checked_at = _now_iso()
    now_s = time.time()
    # Active contract freshness: if today's partition exists but the last tick is too old, mark stale.
    try:
        lag_min = float(os.environ.get("GHTRADER_CONTRACTS_LOCAL_TICK_LAG_STALE_MINUTES", "120") or "120")
    except Exception:
        lag_min = 120.0
    lag_threshold_sec = float(max(0.0, lag_min * 60.0))

    out: list[dict[str, Any]] = []
    for c in contracts:
        sym = str(c.get("symbol") or "").strip()
        if not sym:
            continue
        expired = c.get("expired")
        expired_b = bool(expired) if expired is not None else None
        exp_dt = c.get("expire_datetime")
        exp_dt_s = str(exp_dt).strip() if exp_dt not in (None, "") else None
        open_dt = c.get("open_date")
        open_dt_s = str(open_dt).strip() if open_dt not in (None, "") else None

        raw = _raw_contract_from_symbol(symbol=sym, var=v)

        sym_dir = _ticks_symbol_dir(data_dir=data_dir, symbol=sym, lake_version=lv)
        downloaded_set, local_min, local_max = _scan_date_dirs(sym_dir)

        # Minute-level local bounds (best-effort; cached by file fingerprints).
        bounds_first = compute_local_tick_bounds_for_day(
            data_dir=data_dir,
            runs_dir=runs_dir,
            symbol=sym,
            lake_version=lv,
            day=str(local_min or ""),
            refresh=False,
        ) if local_min else None
        bounds_last = compute_local_tick_bounds_for_day(
            data_dir=data_dir,
            runs_dir=runs_dir,
            symbol=sym,
            lake_version=lv,
            day=str(local_max or ""),
            refresh=False,
        ) if local_max else None
        local_first_tick_ns = int(bounds_first.get("min_datetime_ns")) if isinstance(bounds_first, dict) and bounds_first.get("min_datetime_ns") is not None else None
        local_last_tick_ns = int(bounds_last.get("max_datetime_ns")) if isinstance(bounds_last, dict) and bounds_last.get("max_datetime_ns") is not None else None
        local_first_tick_ts = str(bounds_first.get("min_datetime_ts") or "") if isinstance(bounds_first, dict) else ""
        local_last_tick_ts = str(bounds_last.get("max_datetime_ts") or "") if isinstance(bounds_last, dict) else ""
        local_last_tick_age_sec: float | None = None
        if local_last_tick_ns is not None and local_last_tick_ns > 0:
            try:
                local_last_tick_age_sec = float(max(0.0, now_s - (float(local_last_tick_ns) / 1_000_000_000.0)))
            except Exception:
                local_last_tick_age_sec = None

        # Best-effort expected window:
        # - prefer remote metadata (open_date / expire_datetime when available)
        # - otherwise fall back to QuestDB canonical bounds (when provided)
        # - otherwise fall back to locally observed min/max
        expected_first: date | None = None
        if open_dt_s:
            try:
                expected_first = date.fromisoformat(open_dt_s[:10])
            except Exception:
                expected_first = None

        # Compute expected_last from "activeness" + expire_datetime if known.
        expire_trading: date | None = None
        if exp_dt_s:
            try:
                expire_day = date.fromisoformat(exp_dt_s[:10])
                expire_trading = _last_trading_day_leq(cal, expire_day)
            except Exception:
                expire_trading = None

        expected_last: date | None = None
        if expired_b is False:
            # Resolve after expected_first is known.
            expected_last = None
        else:
            expected_last = expire_trading if expire_trading else None

        # Prefer QuestDB canonical coverage bounds (when provided).
        if questdb_cov_by_symbol is not None:
            qc = questdb_cov_by_symbol.get(sym) or {}
            db_first_s = str(qc.get("first_tick_day") or "").strip()
            db_last_s = str(qc.get("last_tick_day") or "").strip()
            try:
                db_first = date.fromisoformat(db_first_s) if db_first_s else None
            except Exception:
                db_first = None
            try:
                db_last = date.fromisoformat(db_last_s) if db_last_s else None
            except Exception:
                db_last = None

            # Use QuestDB bounds when remote metadata is missing:
            # - open_date missing -> allow db_first to extend expected range backwards
            # - expire_datetime missing -> allow db_last to extend expected range forwards (expired only)
            if expected_first is None and db_first is not None:
                expected_first = db_first
            if expired_b is not False:
                if expire_trading is None and db_last is not None:
                    expected_last = db_last

        # Final local fallbacks.
        if expected_first is None and local_min:
            try:
                expected_first = date.fromisoformat(local_min)
            except Exception:
                expected_first = None
        if expired_b is False:
            expected_last = today_trading if expected_first else None
        else:
            if expected_last is None and local_max:
                try:
                    expected_last = date.fromisoformat(local_max) if expected_first else None
                except Exception:
                    expected_last = None

        # Expected days + done days (downloaded + no-data within expected window).
        days_expected: int | None = None
        days_done: int | None = None
        pct: float | None = None

        if expected_first and expected_last and expected_last >= expected_first:
            expected_iso0 = expected_first.isoformat()
            expected_iso1 = expected_last.isoformat()
            days_expected = _count_trading_days_between(cal=cal, start=expected_first, end=expected_last)

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
        stale_reason: str | None = None
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
                    stale_reason = "missing_trading_day"
                else:
                    status = "incomplete"
            else:
                status = "complete" if pct is not None and pct >= 1.0 else "incomplete"

        # Within-day lag: a contract can be day-complete but still behind in minutes/hours.
        if expired_b is False and status == "complete":
            if local_max and local_max == today_trading.isoformat():
                if local_last_tick_age_sec is not None and local_last_tick_age_sec > lag_threshold_sec:
                    status = "stale"
                    stale_reason = "tick_lag"

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
                "catalog_source": str(c.get("catalog_source") or ""),
                "expired": expired_b,
                "expire_datetime": exp_dt_s,
                "open_date": open_dt_s,
                "local_days_downloaded": int(len(downloaded_set)),
                "local_min": local_min,
                "local_max": local_max,
                "local_first_tick_ns": local_first_tick_ns,
                "local_last_tick_ns": local_last_tick_ns,
                "local_first_tick_ts": local_first_tick_ts or None,
                "local_last_tick_ts": local_last_tick_ts or None,
                "local_last_tick_age_sec": local_last_tick_age_sec,
                "local_checked_at": checked_at,
                "stale_reason": stale_reason,
                "tick_lag_stale_threshold_sec": lag_threshold_sec if expired_b is False else None,
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
        "generated_at": checked_at,
        "local_checked_at": checked_at,
        "contracts": out,
        "active_ranges_available": False,
    }

