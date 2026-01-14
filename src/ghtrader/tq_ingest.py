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


def _detect_latest_contract_yymm_from_tqsdk(*, exchange: str, var: str) -> str:
    """
    Detect the latest listed contract YYMM using TqSdk's contract catalog.

    This replaces the old akshare-based "auto end_contract" behavior.
    """
    from ghtrader.config import get_runs_dir
    from ghtrader.tqsdk_catalog import get_contract_catalog

    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    cat = get_contract_catalog(exchange=ex, var=v, runs_dir=get_runs_dir(), refresh=False)
    if not bool(cat.get("ok", False)):
        raise RuntimeError(str(cat.get("error") or "tqsdk_catalog_failed"))
    contracts = list(cat.get("contracts") or [])

    best_key = -1
    best_yymm = ""
    for c in contracts:
        sym = str((c or {}).get("symbol") or "").strip()
        if not sym:
            continue
        # Expect SHFE.cu2602 -> tail digits -> YYMM
        tail = sym.split(".", 1)[-1]
        digits = "".join([ch for ch in tail if ch.isdigit()])
        if len(digits) < 4:
            continue
        yymm = digits[-4:]
        if not yymm.isdigit():
            continue
        yy, mm = _parse_contract_yymm(yymm)
        key = yy * 100 + mm
        if key > best_key:
            best_key = key
            best_yymm = yymm

    if not best_yymm:
        raise RuntimeError(f"Cannot auto-detect end_contract via TqSdk catalog: {ex}.{v}")
    return best_yymm


def download_contract_range(
    *,
    exchange: str,
    var: str,
    start_contract: str,
    end_contract: str,
    data_dir: Path,
    chunk_days: int = 5,
    start_date: date | None = None,
    end_date: date | None = None,
    lake_version: LakeVersion = "v1",
) -> None:
    """
    Exhaustively backfill L5 ticks for a full contract YYMM range.

    Example:
      exchange=SHFE, var=cu, start_contract=1601, end_contract=2701

    Note:
    - akshare has been removed. This command no longer infers active ranges.
    - It instead downloads the requested contract set over a configurable date window,
      relying on no-data markers to remain idempotent for pre-listing days.
    """
    ex = exchange.upper().strip()
    var_l = var.lower().strip()
    var_u = var_l.upper()

    # Allow end_contract auto-detection from TqSdk catalog.
    if str(end_contract).strip().lower() in {"auto", "latest"}:
        end_contract = _detect_latest_contract_yymm_from_tqsdk(exchange=ex, var=var_l)
        log.info("tq_ingest.detected_end_contract", var=var_l, end_contract=end_contract)

    yymms = _iter_contract_yymms(start_contract, end_contract)
    raw_contracts = [f"{var_u}{yymm}" for yymm in yymms]

    # Default to a practical backfill window if not specified.
    # (Avoid extremely early years unless the operator explicitly requests it.)
    s_date = start_date or date(2015, 1, 1)
    e_date = end_date or date.today()
    if e_date < s_date:
        raise ValueError("end_date must be >= start_date")

    log.info(
        "tq_ingest.range_start",
        exchange=ex,
        var=var_l,
        start_contract=start_contract,
        end_contract=end_contract,
        n_contracts=len(raw_contracts),
        start_date=str(s_date),
        end_date=str(e_date),
    )

    n_skipped = 0
    for raw in raw_contracts:
        symbol = f"{ex}.{var_l}{raw[len(var_u):]}"
        log.info("tq_ingest.contract_download", symbol=symbol, start=str(s_date), end=str(e_date))
        try:
            download_historical_ticks(
                symbol=symbol,
                start_date=s_date,
                end_date=e_date,
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
