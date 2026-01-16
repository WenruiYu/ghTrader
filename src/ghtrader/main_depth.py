"""
QuestDB-first materialization of derived "main-with-depth" ticks.

Given:
- A roll schedule (date -> underlying specific contract; produced by `main-schedule`)
- Raw ticks in QuestDB (canonical)

We build:
- Derived ticks in QuestDB table `ghtrader_ticks_main_l5_v2` with ticks_lake='main_l5'
  and provenance columns (`underlying_contract`, `segment_id`, `schedule_hash`).

Parquet-based materialization has been removed (QuestDB-only system).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

log = structlog.get_logger()

# QuestDB table names
_QUESTDB_TICKS_RAW_TABLE = "ghtrader_ticks_raw_v2"
_QUESTDB_TICKS_MAIN_L5_TABLE = "ghtrader_ticks_main_l5_v2"


def _stable_hash_df(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


# ---------------------------------------------------------------------------
# QuestDB-first materialization (canonical)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MainDepthQuestDBResult:
    """Result from QuestDB-first main_l5 materialization."""

    derived_symbol: str
    schedule_hash: str
    rows_total: int
    days_processed: int
    days_skipped: int
    manifest: dict[str, Any]


def _make_questdb_backend():
    """Build QuestDB ILP backend for writes."""
    from ghtrader.config import (
        get_questdb_host,
        get_questdb_ilp_port,
        get_questdb_pg_dbname,
        get_questdb_pg_password,
        get_questdb_pg_port,
        get_questdb_pg_user,
    )
    from ghtrader.serving_db import ServingDBConfig, make_serving_backend

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


def _prepare_main_l5_df(
    *,
    df: pd.DataFrame,
    derived_symbol: str,
    trading_day: date,
    underlying: str,
    segment_id: int,
    schedule_hash: str,
    lake_version: str,
) -> pd.DataFrame:
    """
    Prepare a tick DataFrame for QuestDB main_l5 ingestion.
    """
    from ghtrader.ticks_schema import TICK_COLUMN_NAMES, row_hash_from_ticks_df

    tick_numeric_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]

    dt_ns = pd.to_numeric(df.get("datetime"), errors="coerce").fillna(0).astype("int64")
    out = pd.DataFrame({"symbol": derived_symbol, "datetime_ns": dt_ns})
    out["ts"] = pd.to_datetime(dt_ns, unit="ns")
    out["trading_day"] = str(trading_day.isoformat())

    for col in tick_numeric_cols:
        out[col] = pd.to_numeric(df.get(col), errors="coerce")

    out["lake_version"] = str(lake_version)
    out["ticks_lake"] = "main_l5"
    out["underlying_contract"] = str(underlying)
    out["segment_id"] = int(segment_id)
    out["schedule_hash"] = str(schedule_hash)

    # Compute row_hash for idempotent sync
    try:
        out["row_hash"] = row_hash_from_ticks_df(df)
    except Exception:
        out["row_hash"] = dt_ns

    return out


def materialize_main_with_depth(
    *,
    derived_symbol: str,
    schedule: pd.DataFrame,
    schedule_hash: str,
    lake_version: str = "v2",
    overwrite: bool = False,
) -> MainDepthQuestDBResult:
    """
    Materialize main-with-depth ticks directly to QuestDB (canonical path).

    Args:
        derived_symbol: continuous-style symbol name (e.g. 'KQ.m@SHFE.cu')
        schedule: DataFrame with columns:
          - `trading_day` (date) or `date` (date)
          - `main_contract` (str)
          - `segment_id` (int) (recommended; will be derived if missing)
        schedule_hash: schedule hash from `ghtrader_main_schedule_v2`
        lake_version: v2-only
        overwrite: if True, re-process days that already exist
    """
    from ghtrader.l5_detection import has_l5_df
    from ghtrader.questdb_client import make_questdb_query_config_from_env
    from ghtrader.questdb_index import (
        INDEX_TABLE_V2,
        SymbolDayIndexRow,
        ensure_index_tables,
        list_present_trading_days,
        upsert_symbol_day_index_rows,
    )
    from ghtrader.questdb_queries import fetch_ticks_for_symbol_day

    df = schedule.copy() if schedule is not None else pd.DataFrame()
    if df.empty:
        raise ValueError("Schedule is empty")

    if "trading_day" in df.columns:
        df["date"] = pd.to_datetime(df["trading_day"], errors="coerce").dt.date
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    else:
        raise ValueError(f"Schedule missing required column: trading_day/date ({list(df.columns)})")

    if "main_contract" not in df.columns:
        raise ValueError(f"Schedule missing required column: main_contract ({list(df.columns)})")

    df["main_contract"] = df["main_contract"].astype(str)
    df = df.dropna(subset=["date", "main_contract"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        raise ValueError("Schedule has no usable rows after normalization")

    # Ensure segment_id exists.
    if "segment_id" not in df.columns:
        seg_ids: list[int] = []
        seg = 0
        prev: str | None = None
        for mc in df["main_contract"].astype(str).tolist():
            if prev is None:
                seg = 0
            elif mc != prev:
                seg += 1
            seg_ids.append(int(seg))
            prev = mc
        df["segment_id"] = seg_ids
    df["segment_id"] = pd.to_numeric(df["segment_id"], errors="coerce").fillna(0).astype("int64")

    # Set up QuestDB
    qcfg = make_questdb_query_config_from_env()
    backend = _make_questdb_backend()

    # Ensure tables exist
    backend.ensure_table(table=_QUESTDB_TICKS_MAIN_L5_TABLE, include_segment_metadata=True)
    ensure_index_tables(cfg=qcfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)

    lv = str(lake_version).lower().strip() or "v2"

    # Check which days already exist for derived symbol
    if not overwrite:
        all_dates = [d for d in df["date"].tolist() if isinstance(d, date)]
        if all_dates:
            existing_dates = list_present_trading_days(
                cfg=qcfg,
                symbol=derived_symbol,
                start_day=min(all_dates),
                end_day=max(all_dates),
                lake_version=lv,
                ticks_lake="main_l5",
            )
        else:
            existing_dates = set()
    else:
        existing_dates = set()

    row_counts: dict[str, int] = {}
    total_rows = 0
    days_processed = 0
    days_skipped = 0

    for _, row in df.iterrows():
        dt: date = row["date"]
        underlying: str = str(row["main_contract"])
        seg_id: int = int(row.get("segment_id", 0) or 0)

        if dt in existing_dates:
            days_skipped += 1
            continue

        # Fetch raw ticks from QuestDB
        raw = fetch_ticks_for_symbol_day(
            cfg=qcfg,
            table=_QUESTDB_TICKS_RAW_TABLE,
            symbol=underlying,
            trading_day=dt.isoformat(),
            lake_version=lv,
            ticks_lake="raw",
            include_provenance=False,
        )
        if raw.empty:
            log.warning("main_l5.questdb.missing_ticks", underlying=underlying, date=dt.isoformat())
            continue

        # Prepare for main_l5 ingestion
        out_df = _prepare_main_l5_df(
            df=raw,
            derived_symbol=derived_symbol,
            trading_day=dt,
            underlying=underlying,
            segment_id=seg_id,
            schedule_hash=str(schedule_hash),
            lake_version=lv,
        )

        # Ingest to QuestDB
        backend.ingest_df(table=_QUESTDB_TICKS_MAIN_L5_TABLE, df=out_df)

        # Update index
        dt_ns = pd.to_numeric(out_df.get("datetime_ns"), errors="coerce").dropna().astype("int64")
        first_ns = int(dt_ns.min()) if not dt_ns.empty else None
        last_ns = int(dt_ns.max()) if not dt_ns.empty else None
        try:
            from ghtrader.ticks_schema import row_hash_aggregates

            rh_agg = row_hash_aggregates(out_df.get("row_hash"))
            upsert_symbol_day_index_rows(
                cfg=qcfg,
                rows=[
                    SymbolDayIndexRow(
                        symbol=derived_symbol,
                        trading_day=dt.isoformat(),
                        ticks_lake="main_l5",
                        lake_version=lv,
                        rows_total=len(out_df),
                        first_datetime_ns=first_ns,
                        last_datetime_ns=last_ns,
                        l5_present=bool(has_l5_df(raw)),
                        row_hash_min=rh_agg["row_hash_min"],
                        row_hash_max=rh_agg["row_hash_max"],
                        row_hash_sum=rh_agg["row_hash_sum"],
                        row_hash_sum_abs=rh_agg["row_hash_sum_abs"],
                    )
                ],
                index_table=INDEX_TABLE_V2,
            )
        except Exception as e:
            log.warning("main_l5.questdb.index_upsert_failed", date=dt.isoformat(), error=str(e))

        row_counts[dt.isoformat()] = len(out_df)
        total_rows += len(out_df)
        days_processed += 1

        log.debug("main_l5.questdb.day_done", date=dt.isoformat(), rows=len(out_df))

    manifest: dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "derived_symbol": derived_symbol,
        "schedule_hash": str(schedule_hash),
        "lake_version": lv,
        "ticks_lake": "main_l5",
        "rows_total": int(total_rows),
        "days_processed": int(days_processed),
        "days_skipped": int(days_skipped),
        "row_counts": row_counts,
        "backend": "questdb",
        "table": _QUESTDB_TICKS_MAIN_L5_TABLE,
        "schedule_rows": int(len(df)),
    }

    log.info(
        "main_l5.questdb.materialized",
        derived_symbol=derived_symbol,
        schedule_hash=str(schedule_hash),
        rows_total=int(total_rows),
        days_processed=int(days_processed),
        days_skipped=int(days_skipped),
    )

    return MainDepthQuestDBResult(
        derived_symbol=derived_symbol,
        schedule_hash=str(schedule_hash),
        rows_total=int(total_rows),
        days_processed=int(days_processed),
        days_skipped=int(days_skipped),
        manifest=manifest,
    )

