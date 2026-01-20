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

from ghtrader.config import get_tqsdk_auth  # noqa: E402
from ghtrader.data.manifest import write_manifest  # noqa: E402
from ghtrader.data.ticks_schema import DatasetVersion, TICK_COLUMN_NAMES  # noqa: E402

log = structlog.get_logger()

_QUESTDB_TICKS_RAW_TABLE = "ghtrader_ticks_raw_v2"
_TICK_NUMERIC_COLS = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]


def _maybe_make_questdb_backend():
    """
    Best-effort QuestDB backend for ingest double-write.

    - If questdb extras are not installed, returns None.
    - If QuestDB is unreachable, returns None.
    """
    try:
        from ghtrader.config import (
            get_questdb_host,
            get_questdb_ilp_port,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

        cfg = ServingDBConfig(
            backend="questdb",
            host=str(get_questdb_host()),
            questdb_ilp_port=int(get_questdb_ilp_port()),
            questdb_pg_port=int(get_questdb_pg_port()),
            questdb_pg_user=str(get_questdb_pg_user()),
            questdb_pg_password=str(get_questdb_pg_password()),
            questdb_pg_dbname=str(get_questdb_pg_dbname()),
        )
        return make_serving_backend(cfg)
    except Exception as e:
        # Keep ingest usable even when questdb extras aren't installed.
        log.debug("tq_ingest.questdb_backend_unavailable", error=str(e))
        return None


def _make_questdb_backend_required():
    """
    QuestDB-first ingest requires QuestDB to be configured + reachable.
    """
    b = _maybe_make_questdb_backend()
    if b is None:
        raise RuntimeError(
            "QuestDB backend is required for tick ingest. Ensure QuestDB is running and install extras: pip install -e '.[questdb]'"
        )
    return b


def _make_questdb_query_cfg():
    from ghtrader.questdb.client import make_questdb_query_config_from_env

    return make_questdb_query_config_from_env()


def _questdb_df_for_ticks(*, df: pd.DataFrame, trading_day: date, ticks_kind: str, dataset_version: str) -> pd.DataFrame:
    """
    Prepare a tick dataframe for QuestDB ILP ingestion, matching the shape used by serving_db.sync_ticks_to_serving_db().
    """
    dt_ns = pd.to_numeric(df.get("datetime"), errors="coerce").fillna(0).astype("int64")
    out = pd.DataFrame({"symbol": df["symbol"].astype(str), "datetime_ns": dt_ns})
    # Keep tz-naive wall-clock timestamp (QuestDB uses ts as designated timestamp).
    out["ts"] = pd.to_datetime(dt_ns, unit="ns")
    out["trading_day"] = str(trading_day.isoformat())

    for col in _TICK_NUMERIC_COLS:
        out[col] = pd.to_numeric(df.get(col), errors="coerce")

    out["dataset_version"] = str(dataset_version)
    out["ticks_kind"] = str(ticks_kind)

    # Deterministic row identity hash for idempotent sync + safe dedup.
    try:
        from ghtrader.data.ticks_schema import row_hash_from_ticks_df

        out["row_hash"] = row_hash_from_ticks_df(df)
    except Exception:
        out["row_hash"] = dt_ns

    return out


def _df_has_true_l5(df: pd.DataFrame) -> bool:
    """
    True L5 means at least one of levels 2–5 has positive, non-NaN values.

    Note: This is a thin wrapper around the unified l5_detection module.
    """
    from ghtrader.util.l5_detection import has_l5_df

    return has_l5_df(df)


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


# Used by download_historical_ticks and unit tests.
def _compute_missing_dates(
    *,
    all_dates: list[date],
    existing_dates: set[date],
    no_data_dates: set[date],
    force_dates: set[date] | None = None,
) -> list[date]:
    ed = set(existing_dates)
    nd = set(no_data_dates)
    fd = set(force_dates or set())
    if fd:
        ed -= fd
        nd -= fd
    return [d for d in all_dates if d not in ed and d not in nd]


# All no-data tracking is handled via QuestDB ghtrader_no_data_days_v2 table.


def download_historical_ticks(
    symbol: str,
    start_date: date,
    end_date: date,
    data_dir: Path,
    chunk_days: int = 5,
    *,
    dataset_version: DatasetVersion = "v2",
    auto_bootstrap_index: bool = True,
    force: bool = False,
) -> None:
    """
    Download historical L5 ticks for a symbol and ingest to QuestDB (canonical).

    QuestDB is the single source of truth. Parquet export has been removed
    from the primary ingest path.

    Uses TqSdk Pro `get_tick_data_series()` which requires `tq_dl` permission.

    Downloads in chunks to avoid memory issues and provides resume capability
    by checking the QuestDB index table.

    Args:
        symbol: Instrument code (e.g., "SHFE.cu2502")
        start_date: Start date (trading day)
        end_date: End date (trading day)
        data_dir: Data directory root
        chunk_days: Number of days per download chunk
    """
    log.info(
        "tq_ingest.download_start",
        symbol=symbol,
        start=str(start_date),
        end=str(end_date),
        dataset_version=str(dataset_version),
        force=bool(force),
    )

    # QuestDB-first: canonical tick storage + index/no-data tracking.
    questdb_backend = _make_questdb_backend_required()
    try:
        # Ensure the canonical tick table supports derived-ticks provenance columns as well.
        questdb_backend.ensure_table(table=_QUESTDB_TICKS_RAW_TABLE, include_segment_metadata=True)
    except Exception as e:
        raise RuntimeError(f"QuestDB table init failed for {_QUESTDB_TICKS_RAW_TABLE}: {e}") from e

    from ghtrader.questdb.index import (
        INDEX_TABLE_V2,
        SymbolDayIndexRow,
        ensure_index_tables,
        list_no_data_trading_days,
        list_present_trading_days,
        upsert_no_data_days,
        upsert_symbol_day_index_rows,
        bootstrap_symbol_day_index_from_ticks,
    )
    from ghtrader.questdb.queries import query_symbol_latest
    from ghtrader.data.trading_calendar import get_trading_calendar, get_trading_days

    qcfg = _make_questdb_query_cfg()
    ensure_index_tables(cfg=qcfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)

    market = _infer_market_from_symbol(symbol)
    all_dates = get_trading_days(market=market, start=start_date, end=end_date, data_dir=data_dir)

    # Determine today's trading day (best-effort) so we can avoid treating it as final no-data.
    today_trading = None
    try:
        cal = get_trading_calendar(data_dir=data_dir, refresh=False)
        today0 = date.today()
        if cal:
            from bisect import bisect_right

            j = bisect_right(cal, today0)
            today_trading = cal[j - 1] if j > 0 else today0
        else:
            d = today0
            while d.weekday() >= 5:
                d -= timedelta(days=1)
            today_trading = d
    except Exception:
        today_trading = None

    # Query QuestDB for existing data (single source of truth).
    existing_dates = set(
        list_present_trading_days(
            cfg=qcfg,
            symbol=symbol,
            start_day=start_date,
            end_day=end_date,
            dataset_version=str(dataset_version),
            ticks_kind="raw",
        )
    )
    no_data_dates = set(
        list_no_data_trading_days(
            cfg=qcfg,
            symbol=symbol,
            start_day=start_date,
            end_day=end_date,
            dataset_version=str(dataset_version),
            ticks_kind="raw",
        )
    )
    # Don't let a premature no-data marker for today's trading day block retries.
    try:
        if today_trading is not None and end_date >= today_trading:
            no_data_dates.discard(today_trading)
    except Exception:
        pass

    existing_dates_orig = set(existing_dates)
    no_data_dates_orig = set(no_data_dates)

    if force:
        # Allow a re-download even if the index or no-data table already has the day.
        try:
            existing_dates -= set(all_dates)
            no_data_dates -= set(all_dates)
        except Exception:
            pass

    # If index is empty but ticks exist, bootstrap the index once for resume correctness.
    if auto_bootstrap_index and not existing_dates:
        try:
            latest = query_symbol_latest(
                cfg=qcfg, table=_QUESTDB_TICKS_RAW_TABLE, symbols=[symbol], dataset_version=str(dataset_version), ticks_kind="raw"
            )
            if latest.get(symbol):
                bootstrap_symbol_day_index_from_ticks(
                    cfg=qcfg,
                    ticks_table=_QUESTDB_TICKS_RAW_TABLE,
                    symbols=[symbol],
                    dataset_version=str(dataset_version),
                    ticks_kind="raw",
                    index_table=INDEX_TABLE_V2,
                    batch_symbols=1,
                )
                existing_dates = set(
                    list_present_trading_days(
                        cfg=qcfg,
                        symbol=symbol,
                        start_day=start_date,
                        end_day=end_date,
                        dataset_version=str(dataset_version),
                        ticks_kind="raw",
                    )
                )
        except Exception:
            pass

    missing_dates = _compute_missing_dates(
        all_dates=all_dates,
        existing_dates=existing_dates,
        no_data_dates=no_data_dates,
        force_dates=(set(all_dates) if force else None),
    )

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
    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

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
                missing_in_chunk = set(chunk) - existing_dates - no_data_dates
                if force:
                    missing_in_chunk = {d for d in missing_in_chunk if d not in existing_dates_orig}
                # Do not mark today's trading day as no-data (keep it retriable).
                try:
                    if today_trading is not None:
                        missing_in_chunk = {d for d in missing_in_chunk if d != today_trading}
                except Exception:
                    pass
                if missing_in_chunk:
                    try:
                        upsert_no_data_days(
                            cfg=qcfg,
                            symbol=symbol,
                            trading_days=[d.isoformat() for d in sorted(missing_in_chunk)],
                            ticks_kind="raw",
                            dataset_version=str(dataset_version),
                            reason="tqsdk_empty_chunk",
                        )
                        no_data_dates |= set(missing_in_chunk)
                    except Exception as e:
                        log.warning("tq_ingest.questdb_no_data_upsert_failed", symbol=symbol, error=str(e))
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

            # Group by trading day and write partitions (night-session aware, v2-only).
            dt_series = pd.to_datetime(df["datetime"], unit="ns")
            cal_dates = dt_series.dt.date
            from bisect import bisect_right
            from ghtrader.data.trading_calendar import get_trading_calendar

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

            written_dates: set[date] = set()
            idx_rows: list[SymbolDayIndexRow] = []
            for dt, group_df in df.groupby("_date"):
                if dt in existing_dates:
                    continue

                # Drop temp columns
                partition_df = group_df.drop(columns=["_date", "_tq_id"], errors="ignore")

                # QuestDB canonical write (required).
                qdf = _questdb_df_for_ticks(
                    df=partition_df, trading_day=dt, ticks_kind="raw", dataset_version=str(dataset_version)
                )
                try:
                    questdb_backend.ingest_df(table=_QUESTDB_TICKS_RAW_TABLE, df=qdf)
                except Exception as e:
                    log.error("tq_ingest.questdb_ingest_failed", symbol=symbol, date=str(dt), error=str(e))
                    raise

                # Update per-day index row (QuestDB).
                try:
                    dt_ns = pd.to_numeric(partition_df.get("datetime"), errors="coerce").dropna().astype("int64")
                    first_ns = int(dt_ns.min()) if not dt_ns.empty else None
                    last_ns = int(dt_ns.max()) if not dt_ns.empty else None
                    from ghtrader.data.ticks_schema import row_hash_aggregates

                    rh_agg = row_hash_aggregates(qdf.get("row_hash"))
                    idx_rows.append(
                        SymbolDayIndexRow(
                            symbol=str(symbol),
                            trading_day=str(dt.isoformat()),
                            ticks_kind="raw",
                            dataset_version=str(dataset_version),
                            rows_total=int(len(partition_df)),
                            first_datetime_ns=first_ns,
                            last_datetime_ns=last_ns,
                            l5_present=bool(_df_has_true_l5(partition_df)),
                            row_hash_min=rh_agg["row_hash_min"],
                            row_hash_max=rh_agg["row_hash_max"],
                            row_hash_sum=rh_agg["row_hash_sum"],
                            row_hash_sum_abs=rh_agg["row_hash_sum_abs"],
                        )
                    )
                except Exception:
                    pass

                row_counts[str(dt)] = len(partition_df)
                total_rows += len(partition_df)
                written_dates.add(dt)
                existing_dates.add(dt)

                log.debug("tq_ingest.partition_written", symbol=symbol, date=str(dt), rows=len(partition_df))

            if idx_rows:
                try:
                    upsert_symbol_day_index_rows(cfg=qcfg, rows=idx_rows, index_table=INDEX_TABLE_V2)
                except Exception as e:
                    log.warning("tq_ingest.questdb_index_upsert_failed", symbol=symbol, error=str(e))

            # Mark any requested trading dates in this chunk that produced no ticks (QuestDB only).
            missing_in_chunk = set(chunk) - written_dates - existing_dates - no_data_dates
            if force:
                missing_in_chunk = {d for d in missing_in_chunk if d not in existing_dates_orig}
            # Do not mark today's trading day as no-data (keep it retriable).
            try:
                if today_trading is not None:
                    missing_in_chunk = {d for d in missing_in_chunk if d != today_trading}
            except Exception:
                pass
            if missing_in_chunk:
                try:
                    upsert_no_data_days(
                        cfg=qcfg,
                        symbol=symbol,
                        trading_days=[d.isoformat() for d in sorted(missing_in_chunk)],
                        ticks_kind="raw",
                        dataset_version=str(dataset_version),
                        reason="tqsdk_chunk_missing_days",
                    )
                    no_data_dates |= set(missing_in_chunk)
                except Exception as e:
                    log.warning("tq_ingest.questdb_no_data_upsert_failed", symbol=symbol, error=str(e))

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

    from .catalog import get_contract_catalog

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
    dataset_version: DatasetVersion = "v2",
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
                dataset_version=dataset_version,
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
    dataset_version: DatasetVersion = "v2",
) -> None:
    """
    Run live tick recorder that subscribes to symbols and ingests to QuestDB (canonical).

    QuestDB is the single source of truth. Parquet export has been removed.

    This runs indefinitely (until interrupted) and periodically flushes accumulated
    ticks to QuestDB.

    Args:
        symbols: List of instrument codes to record
        data_dir: Data directory root
        flush_interval_seconds: How often to flush accumulated ticks to disk
    """
    import time

    log.info("tq_ingest.live_recorder_start", symbols=symbols)

    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    auth = get_tqsdk_auth()
    api = TqApi(auth=auth)

    # Subscribe to tick serials for each symbol
    tick_serials = {s: api.get_tick_serial(s) for s in symbols}

    # Buffers for accumulating ticks
    buffers: dict[str, list[dict]] = {s: [] for s in symbols}
    last_flush = time.time()

    # Track last seen datetime per symbol to avoid duplicates
    last_seen: dict[str, int] = {s: 0 for s in symbols}

    # QuestDB-first: canonical tick storage + index maintenance.
    questdb_backend = _make_questdb_backend_required()
    # Ensure the canonical tick table supports derived-ticks provenance columns as well.
    questdb_backend.ensure_table(table=_QUESTDB_TICKS_RAW_TABLE, include_segment_metadata=True)
    qcfg = _make_questdb_query_cfg()
    try:
        from ghtrader.questdb.index import INDEX_TABLE_V2, ensure_index_tables

        ensure_index_tables(cfg=qcfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
    except Exception as e:
        raise RuntimeError(f"QuestDB index init failed: {e}") from e

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
                _flush_buffers(
                    buffers,
                    data_dir,
                    dataset_version=dataset_version,
                    questdb_backend=questdb_backend,
                    questdb_qcfg=qcfg,
                )
                last_flush = now

    except KeyboardInterrupt:
        log.info("tq_ingest.live_recorder_interrupted")
    finally:
        # Final flush
        _flush_buffers(
            buffers,
            data_dir,
            dataset_version=dataset_version,
            questdb_backend=questdb_backend,
            questdb_qcfg=qcfg,
        )
        api.close()
        log.info("tq_ingest.live_recorder_stopped")


def _flush_buffers(
    buffers: dict[str, list[dict]],
    data_dir: Path,
    *,
    dataset_version: DatasetVersion = "v2",
    questdb_backend,
    questdb_qcfg,
) -> None:
    """Flush accumulated tick buffers to QuestDB (canonical)."""
    from ghtrader.questdb.index import INDEX_TABLE_V2, SymbolDayIndexRow, get_symbol_day_index_row, upsert_symbol_day_index_rows

    for symbol, records in buffers.items():
        if not records:
            continue

        df = pd.DataFrame(records)

        # Group by trading day (night-session aware, v2-only)
        dt_series = pd.to_datetime(df["datetime"], unit="ns")
        cal_dates = dt_series.dt.date
        from bisect import bisect_right
        from ghtrader.data.trading_calendar import get_trading_calendar

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

        for dt, group_df in df.groupby("_date"):
            partition_df = group_df.drop(columns=["_date"])
            qdf = _questdb_df_for_ticks(df=partition_df, trading_day=dt, ticks_kind="raw", dataset_version=str(dataset_version))
            questdb_backend.ingest_df(table=_QUESTDB_TICKS_RAW_TABLE, df=qdf)

            # Incremental index maintenance (merge with existing row).
            td = str(dt.isoformat())
            dt_ns = pd.to_numeric(partition_df.get("datetime"), errors="coerce").dropna().astype("int64")
            new_first = int(dt_ns.min()) if not dt_ns.empty else None
            new_last = int(dt_ns.max()) if not dt_ns.empty else None
            new_rows = int(len(partition_df))
            new_l5 = bool(_df_has_true_l5(partition_df))
            from ghtrader.data.ticks_schema import row_hash_aggregates

            new_rh = row_hash_aggregates(qdf.get("row_hash"))
            new_rh_min = new_rh["row_hash_min"]
            new_rh_max = new_rh["row_hash_max"]
            new_rh_sum = new_rh["row_hash_sum"]
            new_rh_sum_abs = new_rh["row_hash_sum_abs"]

            prev = get_symbol_day_index_row(
                cfg=questdb_qcfg,
                symbol=symbol,
                trading_day=td,
                dataset_version=str(dataset_version),
                ticks_kind="raw",
                index_table=INDEX_TABLE_V2,
            )
            prev_rows = int(prev.get("rows_total") or 0) if isinstance(prev, dict) else 0
            prev_first = int(prev.get("first_datetime_ns")) if isinstance(prev, dict) and prev.get("first_datetime_ns") is not None else None
            prev_last = int(prev.get("last_datetime_ns")) if isinstance(prev, dict) and prev.get("last_datetime_ns") is not None else None
            prev_l5 = bool(prev.get("l5_present", False)) if isinstance(prev, dict) else False
            prev_rh_min = int(prev.get("row_hash_min")) if isinstance(prev, dict) and prev.get("row_hash_min") is not None else None
            prev_rh_max = int(prev.get("row_hash_max")) if isinstance(prev, dict) and prev.get("row_hash_max") is not None else None
            prev_rh_sum = int(prev.get("row_hash_sum")) if isinstance(prev, dict) and prev.get("row_hash_sum") is not None else None
            prev_rh_sum_abs = int(prev.get("row_hash_sum_abs")) if isinstance(prev, dict) and prev.get("row_hash_sum_abs") is not None else None

            merged_rows = prev_rows + new_rows
            merged_first = new_first if prev_first is None else (prev_first if new_first is None else min(prev_first, new_first))
            merged_last = new_last if prev_last is None else (prev_last if new_last is None else max(prev_last, new_last))
            merged_l5 = bool(prev_l5 or new_l5)
            merged_rh_min = new_rh_min if prev_rh_min is None else (prev_rh_min if new_rh_min is None else min(prev_rh_min, new_rh_min))
            merged_rh_max = new_rh_max if prev_rh_max is None else (prev_rh_max if new_rh_max is None else max(prev_rh_max, new_rh_max))
            try:
                import numpy as np

                def _merge_i64_sum(a: int | None, b: int | None) -> int | None:
                    if a is None and b is None:
                        return None
                    return int(np.int64(a or 0) + np.int64(b or 0))

                merged_rh_sum = _merge_i64_sum(prev_rh_sum, new_rh_sum)
                merged_rh_sum_abs = _merge_i64_sum(prev_rh_sum_abs, new_rh_sum_abs)
            except Exception:
                merged_rh_sum = None if (prev_rh_sum is None and new_rh_sum is None) else int((prev_rh_sum or 0) + (new_rh_sum or 0))
                merged_rh_sum_abs = None if (prev_rh_sum_abs is None and new_rh_sum_abs is None) else int((prev_rh_sum_abs or 0) + (new_rh_sum_abs or 0))

            try:
                upsert_symbol_day_index_rows(
                    cfg=questdb_qcfg,
                    rows=[
                        SymbolDayIndexRow(
                            symbol=str(symbol),
                            trading_day=str(td),
                            ticks_kind="raw",
                            dataset_version=str(dataset_version),
                            rows_total=int(merged_rows),
                            first_datetime_ns=merged_first,
                            last_datetime_ns=merged_last,
                            l5_present=bool(merged_l5),
                            row_hash_min=merged_rh_min,
                            row_hash_max=merged_rh_max,
                            row_hash_sum=merged_rh_sum,
                            row_hash_sum_abs=merged_rh_sum_abs,
                        )
                    ],
                    index_table=INDEX_TABLE_V2,
                )
            except Exception as e:
                log.warning("tq_ingest.questdb_index_upsert_failed", symbol=symbol, date=str(td), error=str(e))

            log.debug("tq_ingest.flush", symbol=symbol, date=str(dt), rows=len(partition_df))

        # Clear buffer
        records.clear()

