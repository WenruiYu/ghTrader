"""
TqSdk integration: historical tick download and live tick recorder.

Uses TqSdk Pro (tq_dl) for bulk historical download and get_tick_serial for live.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import structlog

# tqsdk is optional in unit tests; import only when needed.
import sys

from ghtrader.config import get_tqsdk_auth  # noqa: E402
from ghtrader.lake import (  # noqa: E402
    LakeVersion,
    TICK_COLUMN_NAMES,
    list_available_dates,
    write_manifest,
    write_ticks_partition,
)

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Historical download
# ---------------------------------------------------------------------------

def _infer_market_from_symbol(symbol: str) -> str | None:
    """
    Best-effort market inference.

    Examples:
    - SHFE.cu2602 -> SHFE
    - KQ.m@SHFE.cu -> SHFE
    """
    s = str(symbol).strip()
    if "@" in s:
        s = s.split("@", 1)[1]
    if "." in s:
        return s.split(".", 1)[0].upper()
    return None


def _no_data_dates_path(data_dir: Path, symbol: str) -> Path:
    # Backwards compatible default is lake_v1; callers may override via helpers below.
    return data_dir / "lake" / "ticks" / f"symbol={symbol}" / "_no_data_dates.json"


def _no_data_dates_path_in_lake(data_dir: Path, symbol: str, *, lake_version: LakeVersion) -> Path:
    from ghtrader.lake import ticks_symbol_dir

    return ticks_symbol_dir(data_dir, symbol, ticks_lake="raw", lake_version=lake_version) / "_no_data_dates.json"


def _load_no_data_dates(data_dir: Path, symbol: str, *, lake_version: LakeVersion = "v1") -> set[date]:
    p = _no_data_dates_path_in_lake(data_dir, symbol, lake_version=lake_version)
    if not p.exists():
        return set()
    try:
        with open(p, "r") as f:
            raw = json.load(f)
        if not isinstance(raw, list):
            return set()
        out: set[date] = set()
        for s in raw:
            try:
                out.add(date.fromisoformat(str(s)))
            except Exception:
                continue
        return out
    except Exception as e:
        log.warning("tq_ingest.no_data_read_failed", symbol=symbol, path=str(p), error=str(e))
        return set()


def _write_no_data_dates(data_dir: Path, symbol: str, dates_set: set[date], *, lake_version: LakeVersion = "v1") -> None:
    p = _no_data_dates_path_in_lake(data_dir, symbol, lake_version=lake_version)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = sorted({d.isoformat() for d in dates_set})
    with open(p, "w") as f:
        json.dump(payload, f, indent=2)


def _mark_no_data_dates(data_dir: Path, symbol: str, dates_to_add: set[date], *, lake_version: LakeVersion = "v1") -> None:
    if not dates_to_add:
        return
    cur = _load_no_data_dates(data_dir, symbol, lake_version=lake_version)
    before = len(cur)
    cur |= dates_to_add
    if len(cur) != before:
        _write_no_data_dates(data_dir, symbol, cur, lake_version=lake_version)
        log.info("tq_ingest.no_data_updated", symbol=symbol, added=len(dates_to_add), total=len(cur))


def download_historical_ticks(
    symbol: str,
    start_date: date,
    end_date: date,
    data_dir: Path,
    chunk_days: int = 5,
    *,
    lake_version: LakeVersion = "v1",
) -> None:
    """
    Download historical L5 ticks for a symbol and write to Parquet lake.
    
    Uses TqSdk Pro `get_tick_data_series()` which requires `tq_dl` permission.
    
    Downloads in chunks to avoid memory issues and provides resume capability
    by checking existing partitions.
    
    Args:
        symbol: Instrument code (e.g., "SHFE.cu2502")
        start_date: Start date (trading day)
        end_date: End date (trading day)
        data_dir: Data directory root
        chunk_days: Number of days per download chunk
    """
    log.info("tq_ingest.download_start", symbol=symbol, start=str(start_date), end=str(end_date))
    
    # Check which dates already exist in the selected lake version
    existing_dates = set(list_available_dates(data_dir, symbol, lake_version=lake_version))
    from ghtrader.trading_calendar import get_trading_days

    market = _infer_market_from_symbol(symbol)
    all_dates = get_trading_days(market=market, start=start_date, end=end_date, data_dir=data_dir)
    no_data_dates = _load_no_data_dates(data_dir, symbol, lake_version=lake_version)
    missing_dates = [d for d in all_dates if d not in existing_dates and d not in no_data_dates]
    
    if not missing_dates:
        log.info("tq_ingest.already_complete", symbol=symbol)
        return
    
    log.info(
        "tq_ingest.dates_to_download",
        symbol=symbol,
        total=len(all_dates),
        existing=len(existing_dates),
        missing=len(missing_dates),
    )
    
    # Create TqApi (outside backtest mode, not in coroutine)
    # Add vendored tqsdk to path if present (repo-local install pattern).
    _TQSDK_PATH = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
    if _TQSDK_PATH.exists() and str(_TQSDK_PATH) not in sys.path:
        sys.path.insert(0, str(_TQSDK_PATH))

    from tqsdk import TqApi  # type: ignore

    auth = get_tqsdk_auth()
    api = TqApi(auth=auth)
    
    total_rows = 0
    row_counts: dict[str, int] = {}
    
    try:
        # Download in chunks
        for i in range(0, len(missing_dates), chunk_days):
            chunk = missing_dates[i : i + chunk_days]
            chunk_start = chunk[0]
            chunk_end = chunk[-1]
            
            log.info(
                "tq_ingest.chunk_download",
                symbol=symbol,
                chunk_start=str(chunk_start),
                chunk_end=str(chunk_end),
                chunk_idx=i // chunk_days + 1,
            )
            
            # Download using TqSdk Pro API
            # Note: get_tick_data_series returns a static DataFrame
            # Date params are trading days (date) or datetime for exact time
            try:
                df = api.get_tick_data_series(
                    symbol=symbol,
                    start_dt=chunk_start,
                    end_dt=chunk_end + timedelta(days=1),  # end is exclusive
                )
            except Exception as e:
                log.warning("tq_ingest.chunk_failed", symbol=symbol, chunk_start=str(chunk_start), error=str(e))
                continue
            
            if df.empty:
                log.warning("tq_ingest.empty_chunk", symbol=symbol, chunk_start=str(chunk_start))
                _mark_no_data_dates(data_dir, symbol, set(chunk) - existing_dates, lake_version=lake_version)
                continue
            
            # Rename columns to match our schema
            # TqSdk uses 'datetime' as epoch-ns, which matches our schema
            df = df.rename(columns={"id": "_tq_id"})
            
            # Add symbol column
            df["symbol"] = symbol
            
            # Ensure all L5 columns exist (pad with NaN if needed for non-SHFE)
            for col in TICK_COLUMN_NAMES:
                if col not in df.columns:
                    df[col] = float("nan")
            
            # Group by date and write partitions.
            # - v1: calendar date (legacy behavior)
            # - v2: trading day (night-session aware)
            dt_series = pd.to_datetime(df["datetime"], unit="ns")
            cal_dates = dt_series.dt.date
            if lake_version == "v2":
                from bisect import bisect_right
                from ghtrader.trading_calendar import get_trading_calendar

                cal = get_trading_calendar(data_dir=data_dir, refresh=False)
                cal_list = cal if cal else []

                def _next_trading_day(d: date) -> date:
                    if cal_list:
                        j = bisect_right(cal_list, d)
                        if j < len(cal_list):
                            return cal_list[j]
                    return d + timedelta(days=1)

                mask = dt_series.dt.hour >= 18
                if mask.any():
                    # Map only the (small) set of unique calendar dates needing +1 trading-day shift.
                    uniq = sorted({d for d in cal_dates[mask].tolist() if isinstance(d, date)})
                    next_map = {d: _next_trading_day(d) for d in uniq}
                    trading_dates = cal_dates.copy()
                    trading_dates.loc[mask] = cal_dates.loc[mask].map(next_map)  # type: ignore[assignment]
                    df["_date"] = trading_dates
                else:
                    df["_date"] = cal_dates
            else:
                df["_date"] = cal_dates
            
            written_dates: set[date] = set()
            for dt, group_df in df.groupby("_date"):
                if dt in existing_dates:
                    continue
                
                # Drop temp columns
                partition_df = group_df.drop(columns=["_date", "_tq_id"], errors="ignore")
                
                # Write partition
                write_ticks_partition(partition_df, data_dir, symbol, dt, lake_version=lake_version)
                
                row_counts[str(dt)] = len(partition_df)
                total_rows += len(partition_df)
                written_dates.add(dt)
                
                log.debug("tq_ingest.partition_written", symbol=symbol, date=str(dt), rows=len(partition_df))

            # Mark any requested trading dates in this chunk that produced no ticks.
            missing_in_chunk = set(chunk) - written_dates - existing_dates
            _mark_no_data_dates(data_dir, symbol, missing_in_chunk, lake_version=lake_version)
    
    finally:
        api.close()
    
    # Write manifest
    write_manifest(
        data_dir=data_dir,
        symbols=[symbol],
        start_date=start_date,
        end_date=end_date,
        source="tq_dl",
        row_counts=row_counts,
    )
    
    log.info("tq_ingest.download_complete", symbol=symbol, total_rows=total_rows)


# ---------------------------------------------------------------------------
# Contract-range download (exhaustive backfill)
# ---------------------------------------------------------------------------

def _parse_contract_yymm(yymm: str) -> tuple[int, int]:
    s = str(yymm).strip()
    if len(s) != 4 or not s.isdigit():
        raise ValueError(f"Invalid contract code (expected YYMM): {yymm!r}")
    yy = int(s[:2])
    mm = int(s[2:])
    if mm < 1 or mm > 12:
        raise ValueError(f"Invalid contract month in {yymm!r}")
    # ghTrader scope is modern SHFE data; interpret YY as 20YY.
    return 2000 + yy, mm


def _iter_contract_yymms(start_contract: str, end_contract: str) -> list[str]:
    sy, sm = _parse_contract_yymm(start_contract)
    ey, em = _parse_contract_yymm(end_contract)
    cur = date(sy, sm, 1)
    end_dt = date(ey, em, 1)
    out: list[str] = []
    while cur <= end_dt:
        out.append(f"{cur.year % 100:02d}{cur.month:02d}")
        # next month
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return out


def _infer_active_ranges_from_daily(
    daily: pd.DataFrame,
    *,
    var_upper: str,
) -> dict[str, tuple[date, date]]:
    """
    Infer first/last active trading day per contract from daily data.

    Heuristic: a day is considered active if (open_interest > 0) OR (volume > 0).
    """
    if daily.empty:
        return {}

    df = daily.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["open_interest"] = pd.to_numeric(df.get("open_interest"), errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")

    if "variety" in df.columns:
        df = df[df["variety"].astype(str).str.upper() == var_upper]
    else:
        df = df[df["symbol"].astype(str).str.upper().str.startswith(var_upper)]

    df = df.dropna(subset=["date", "symbol"])
    active = df[(df["open_interest"].fillna(0) > 0) | (df["volume"].fillna(0) > 0)]
    if active.empty:
        return {}

    ranges = active.groupby(active["symbol"].astype(str))["date"].agg(["min", "max"]).to_dict(orient="index")
    return {sym: (v["min"], v["max"]) for sym, v in ranges.items()}


def _update_active_ranges_from_daily_frame(
    ranges: dict[str, tuple[date, date]],
    daily_df: pd.DataFrame,
    *,
    var_upper: str,
) -> None:
    """
    Streaming active-range updater (in-place).

    This matches `_infer_active_ranges_from_daily` semantics:
    active day := (open_interest > 0) OR (volume > 0)
    """
    if daily_df is None or daily_df.empty:
        return

    df = daily_df.copy()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.date
    df["open_interest"] = pd.to_numeric(df.get("open_interest"), errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")

    if "variety" in df.columns:
        df = df[df["variety"].astype(str).str.upper() == var_upper]
    else:
        df = df[df["symbol"].astype(str).str.upper().str.startswith(var_upper)]

    df = df.dropna(subset=["date", "symbol"])
    active = df[(df["open_interest"].fillna(0) > 0) | (df["volume"].fillna(0) > 0)]
    if active.empty:
        return

    for _, row in active.iterrows():
        sym = str(row["symbol"])
        d = row["date"]
        if not isinstance(d, date):
            continue
        if sym not in ranges:
            ranges[sym] = (d, d)
        else:
            s, e = ranges[sym]
            if d < s:
                s = d
            if d > e:
                e = d
            ranges[sym] = (s, e)


def _active_ranges_cache_dir(data_dir: Path, exchange: str, var: str) -> Path:
    return data_dir / "akshare" / "active_ranges" / f"market={exchange.upper()}" / f"var={var.lower()}"


def _read_active_ranges_cache(data_dir: Path, exchange: str, var: str) -> tuple[pd.DataFrame | None, dict | None]:
    root = _active_ranges_cache_dir(data_dir, exchange, var)
    p_ranges = root / "active_ranges.parquet"
    p_manifest = root / "manifest.json"
    if not p_ranges.exists() or not p_manifest.exists():
        return None, None
    try:
        df = pd.read_parquet(p_ranges)
        with open(p_manifest, "r") as f:
            manifest = json.load(f)
        return df, manifest
    except Exception as e:
        log.warning("tq_ingest.active_ranges_cache_read_failed", root=str(root), error=str(e))
        return None, None


def _write_active_ranges_cache(
    *,
    data_dir: Path,
    exchange: str,
    var: str,
    ranges: dict[str, tuple[date, date]],
    scanned_start: date,
    scanned_end: date,
) -> Path:
    root = _active_ranges_cache_dir(data_dir, exchange, var)
    root.mkdir(parents=True, exist_ok=True)

    rows = [
        {"symbol": sym, "first_active": s.isoformat(), "last_active": e.isoformat()}
        for sym, (s, e) in sorted(ranges.items())
    ]
    df = pd.DataFrame(rows)
    out_path = root / "active_ranges.parquet"
    df.to_parquet(out_path, index=False)

    manifest = {
        "exchange": exchange.upper(),
        "var": var.lower(),
        "scanned_start": scanned_start.isoformat(),
        "scanned_end": scanned_end.isoformat(),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "n_symbols": len(df),
    }
    with open(root / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    return out_path


def _detect_latest_contract_yymm_from_daily(daily: pd.DataFrame, *, var_upper: str) -> str:
    """
    Detect the latest listed contract YYMM from akshare daily data.

    This uses the max contract month found in recent daily rows for the variety.
    """
    if daily is None or daily.empty:
        raise RuntimeError("Cannot auto-detect end_contract: daily data is empty")

    df = daily.copy()
    if "variety" in df.columns:
        df = df[df["variety"].astype(str).str.upper() == var_upper]
    else:
        df = df[df["symbol"].astype(str).str.upper().str.startswith(var_upper)]

    if df.empty:
        raise RuntimeError(f"Cannot auto-detect end_contract: no rows for variety={var_upper}")

    sym = df["symbol"].astype(str).str.upper().str.strip()
    # Expect symbols like CU2602; extract trailing digits and keep last 4 (YYMM).
    yymm = sym.str.extract(r"(\d+)$", expand=False).dropna().astype(str).str[-4:]
    yymm = yymm[yymm.str.fullmatch(r"\d{4}")]
    if yymm.empty:
        raise RuntimeError(f"Cannot auto-detect end_contract: no parsable YYMM in symbols for {var_upper}")

    best_key = -1
    best_yymm = ""
    for s in sorted(set(yymm.tolist())):
        yy, mm = _parse_contract_yymm(s)
        key = yy * 100 + mm
        if key > best_key:
            best_key = key
            best_yymm = s

    if not best_yymm:
        raise RuntimeError("Cannot auto-detect end_contract: no candidates")
    return best_yymm


def download_contract_range(
    *,
    exchange: str,
    var: str,
    start_contract: str,
    end_contract: str,
    data_dir: Path,
    chunk_days: int = 5,
    refresh_akshare: bool = False,
    lake_version: LakeVersion = "v1",
) -> None:
    """
    Exhaustively backfill L5 ticks for a full contract YYMM range.

    Example:
      exchange=SHFE, var=cu, start_contract=1601, end_contract=2701

    We infer each contract's active trading date range using akshare daily data,
    then call `download_historical_ticks()` for only that inferred window.
    """
    from ghtrader.akshare_daily import fetch_futures_daily_range, iter_futures_daily_range

    ex = exchange.upper().strip()
    var_l = var.lower().strip()
    var_u = var_l.upper()

    # Allow end_contract auto-detection from recent daily data.
    if str(end_contract).strip().lower() in {"auto", "latest"}:
        today = date.today()
        detect_start = today - timedelta(days=120)
        detect_daily = fetch_futures_daily_range(
            data_dir=data_dir,
            market=ex,
            start=detect_start,
            end=today,
            refresh=refresh_akshare,
        )
        end_contract = _detect_latest_contract_yymm_from_daily(detect_daily, var_upper=var_u)
        log.info("tq_ingest.detected_end_contract", var=var_l, end_contract=end_contract)

    yymms = _iter_contract_yymms(start_contract, end_contract)
    raw_contracts = [f"{var_u}{yymm}" for yymm in yymms]

    # Fetch a conservative daily window around the contract range.
    sy, sm = _parse_contract_yymm(start_contract)
    fetch_start = date(sy, sm, 1) - timedelta(days=450)
    # We only need daily data up to *today* to infer historical active ranges.
    # Avoid wasting time caching far-future empty days.
    fetch_end = date.today()

    log.info(
        "tq_ingest.range_start",
        exchange=ex,
        var=var_l,
        start_contract=start_contract,
        end_contract=end_contract,
        n_contracts=len(raw_contracts),
        daily_start=str(fetch_start),
        daily_end=str(fetch_end),
    )

    # Active ranges cache: reuse if it covers the scan window.
    cached_df, cached_manifest = _read_active_ranges_cache(data_dir, ex, var_l)
    ranges: dict[str, tuple[date, date]] = {}
    reused_cache = False
    if (
        cached_df is not None
        and cached_manifest is not None
        and not refresh_akshare
        and str(cached_manifest.get("scanned_start")) <= fetch_start.isoformat()
        and str(cached_manifest.get("scanned_end")) >= fetch_end.isoformat()
    ):
        try:
            for _, r in cached_df.iterrows():
                ranges[str(r["symbol"])] = (
                    date.fromisoformat(str(r["first_active"])),
                    date.fromisoformat(str(r["last_active"])),
                )
            reused_cache = True
            log.info("tq_ingest.active_ranges_cache_reused", exchange=ex, var=var_l, n=len(ranges))
        except Exception as e:
            log.warning("tq_ingest.active_ranges_cache_bad", error=str(e))
            ranges = {}

    if not reused_cache:
        # Stream daily frames (trading days only) to build ranges without huge in-memory DataFrames.
        for df_day in iter_futures_daily_range(
            data_dir=data_dir,
            market=ex,
            start=fetch_start,
            end=fetch_end,
            refresh=refresh_akshare,
            skip_errors=True,
        ):
            _update_active_ranges_from_daily_frame(ranges, df_day, var_upper=var_u)

        _write_active_ranges_cache(
            data_dir=data_dir,
            exchange=ex,
            var=var_l,
            ranges=ranges,
            scanned_start=fetch_start,
            scanned_end=fetch_end,
        )
        log.info("tq_ingest.active_ranges_cache_written", exchange=ex, var=var_l, n=len(ranges))

    n_skipped = 0
    for raw in raw_contracts:
        if raw not in ranges:
            log.warning("tq_ingest.no_active_range", contract=raw)
            n_skipped += 1
            continue
        c_start, c_end = ranges[raw]
        symbol = f"{ex}.{var_l}{raw[len(var_u):]}"
        log.info("tq_ingest.contract_download", symbol=symbol, start=str(c_start), end=str(c_end))
        try:
            download_historical_ticks(
                symbol=symbol,
                start_date=c_start,
                end_date=c_end,
                data_dir=data_dir,
                chunk_days=chunk_days,
                lake_version=lake_version,
            )
        except Exception as e:
            # Keep the long-running backfill resilient: one bad contract should not
            # terminate the entire range job.
            log.warning("tq_ingest.contract_failed", symbol=symbol, error=str(e))
            continue

    log.info("tq_ingest.range_done", exchange=ex, var=var_l, n_contracts=len(raw_contracts), skipped=n_skipped)


# ---------------------------------------------------------------------------
# Live recorder
# ---------------------------------------------------------------------------

def run_live_recorder(
    symbols: list[str],
    data_dir: Path,
    flush_interval_seconds: int = 60,
    *,
    lake_version: LakeVersion = "v1",
) -> None:
    """
    Run live tick recorder that subscribes to symbols and appends to Parquet lake.
    
    This runs indefinitely (until interrupted) and periodically flushes accumulated
    ticks to Parquet partitions.
    
    Args:
        symbols: List of instrument codes to record
        data_dir: Data directory root
        flush_interval_seconds: How often to flush accumulated ticks to disk
    """
    import time
    
    log.info("tq_ingest.live_recorder_start", symbols=symbols)
    
    # Add vendored tqsdk to path if present (repo-local install pattern).
    _TQSDK_PATH = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
    if _TQSDK_PATH.exists() and str(_TQSDK_PATH) not in sys.path:
        sys.path.insert(0, str(_TQSDK_PATH))

    from tqsdk import TqApi  # type: ignore

    auth = get_tqsdk_auth()
    api = TqApi(auth=auth)
    
    # Subscribe to tick serials for each symbol
    tick_serials = {s: api.get_tick_serial(s) for s in symbols}
    
    # Buffers for accumulating ticks
    buffers: dict[str, list[dict]] = {s: [] for s in symbols}
    last_flush = time.time()
    
    # Track last seen datetime per symbol to avoid duplicates
    last_seen: dict[str, int] = {s: 0 for s in symbols}
    
    try:
        while True:
            api.wait_update()
            
            # Collect new ticks from each serial
            for symbol, serial in tick_serials.items():
                if api.is_changing(serial):
                    # Get the latest tick (last row)
                    if len(serial) > 0:
                        latest = serial.iloc[-1]
                        dt_ns = int(latest["datetime"])
                        
                        # Skip if already seen
                        if dt_ns <= last_seen[symbol]:
                            continue
                        
                        last_seen[symbol] = dt_ns
                        
                        # Build tick record
                        record = {
                            "symbol": symbol,
                            "datetime": dt_ns,
                            "last_price": float(latest.get("last_price", float("nan"))),
                            "average": float(latest.get("average", float("nan"))),
                            "highest": float(latest.get("highest", float("nan"))),
                            "lowest": float(latest.get("lowest", float("nan"))),
                            "volume": float(latest.get("volume", float("nan"))),
                            "amount": float(latest.get("amount", float("nan"))),
                            "open_interest": float(latest.get("open_interest", float("nan"))),
                        }
                        
                        # Add L5 book data
                        for i in range(1, 6):
                            record[f"bid_price{i}"] = float(latest.get(f"bid_price{i}", float("nan")))
                            record[f"bid_volume{i}"] = float(latest.get(f"bid_volume{i}", float("nan")))
                            record[f"ask_price{i}"] = float(latest.get(f"ask_price{i}", float("nan")))
                            record[f"ask_volume{i}"] = float(latest.get(f"ask_volume{i}", float("nan")))
                        
                        buffers[symbol].append(record)
            
            # Flush periodically
            now = time.time()
            if now - last_flush >= flush_interval_seconds:
                _flush_buffers(buffers, data_dir, lake_version=lake_version)
                last_flush = now
    
    except KeyboardInterrupt:
        log.info("tq_ingest.live_recorder_interrupted")
    finally:
        # Final flush
        _flush_buffers(buffers, data_dir, lake_version=lake_version)
        api.close()
        log.info("tq_ingest.live_recorder_stopped")


def _flush_buffers(buffers: dict[str, list[dict]], data_dir: Path, *, lake_version: LakeVersion = "v1") -> None:
    """Flush accumulated tick buffers to Parquet partitions."""
    for symbol, records in buffers.items():
        if not records:
            continue
        
        df = pd.DataFrame(records)
        
        # Group by date
        dt_series = pd.to_datetime(df["datetime"], unit="ns")
        cal_dates = dt_series.dt.date
        if lake_version == "v2":
            from bisect import bisect_right
            from ghtrader.trading_calendar import get_trading_calendar

            cal = get_trading_calendar(data_dir=data_dir, refresh=False)
            cal_list = cal if cal else []

            def _next_trading_day(d: date) -> date:
                if cal_list:
                    j = bisect_right(cal_list, d)
                    if j < len(cal_list):
                        return cal_list[j]
                return d + timedelta(days=1)

            mask = dt_series.dt.hour >= 18
            if mask.any():
                uniq = sorted({d for d in cal_dates[mask].tolist() if isinstance(d, date)})
                next_map = {d: _next_trading_day(d) for d in uniq}
                trading_dates = cal_dates.copy()
                trading_dates.loc[mask] = cal_dates.loc[mask].map(next_map)  # type: ignore[assignment]
                df["_date"] = trading_dates
            else:
                df["_date"] = cal_dates
        else:
            df["_date"] = cal_dates
        
        for dt, group_df in df.groupby("_date"):
            partition_df = group_df.drop(columns=["_date"])
            write_ticks_partition(partition_df, data_dir, symbol, dt, lake_version=lake_version)
            log.debug("tq_ingest.flush", symbol=symbol, date=str(dt), rows=len(partition_df))
        
        # Clear buffer
        records.clear()
