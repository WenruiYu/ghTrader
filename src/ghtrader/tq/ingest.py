"""
TqSdk integration: download L5 ticks and write main_l5 directly to QuestDB.
"""

from __future__ import annotations

from datetime import date, timedelta
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from ghtrader.config import get_tqsdk_auth
from ghtrader.data.manifest import write_manifest
from ghtrader.data.ticks_schema import DatasetVersion, TICK_COLUMN_NAMES, row_hash_from_ticks_df

log = structlog.get_logger()

_QUESTDB_TICKS_MAIN_L5_TABLE = "ghtrader_ticks_main_l5_v2"
_TICK_NUMERIC_COLS = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]


def _make_questdb_backend_required():
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


def _questdb_df_for_main_l5(
    *,
    df: pd.DataFrame,
    derived_symbol: str,
    trading_day: date,
    underlying_contract: str,
    segment_id: int,
    schedule_hash: str,
    dataset_version: str,
) -> pd.DataFrame:
    dt_ns = pd.to_numeric(df.get("datetime"), errors="coerce").fillna(0).astype("int64")
    out = pd.DataFrame({"symbol": derived_symbol, "datetime_ns": dt_ns})
    out["ts"] = pd.to_datetime(dt_ns, unit="ns")
    out["trading_day"] = str(trading_day.isoformat())

    for col in _TICK_NUMERIC_COLS:
        out[col] = pd.to_numeric(df.get(col), errors="coerce")

    out["dataset_version"] = str(dataset_version)
    out["ticks_kind"] = "main_l5"
    out["underlying_contract"] = str(underlying_contract)
    out["segment_id"] = int(segment_id)
    out["schedule_hash"] = str(schedule_hash)

    try:
        out["row_hash"] = row_hash_from_ticks_df(df)
    except Exception:
        out["row_hash"] = dt_ns

    return out


def download_main_l5_for_days(
    *,
    underlying_symbol: str,
    derived_symbol: str,
    trading_days: list[date],
    segment_id: int,
    schedule_hash: str,
    data_dir: Path,
    dataset_version: DatasetVersion = "v2",
) -> dict[str, Any]:
    """
    Download L5 ticks for an underlying contract on specific trading days
    and write directly to QuestDB main_l5 table.
    """
    if not trading_days:
        return {"rows_total": 0, "row_counts": {}}

    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    backend = _make_questdb_backend_required()
    backend.ensure_table(table=_QUESTDB_TICKS_MAIN_L5_TABLE, include_segment_metadata=True)

    auth = get_tqsdk_auth()
    api = TqApi(auth=auth)

    row_counts: dict[str, int] = {}
    unique_days = sorted(set(trading_days))
    days_total = int(len(unique_days))
    total_rows = 0
    last_progress_ts = time.time()
    try:
        progress_every_s = float(os.environ.get("GHTRADER_PROGRESS_EVERY_S", "15") or "15")
    except Exception:
        progress_every_s = 15.0
    progress_every_s = max(5.0, float(progress_every_s))
    try:
        progress_every_n = int(os.environ.get("GHTRADER_PROGRESS_EVERY_N", "25") or "25")
    except Exception:
        progress_every_n = 25
    progress_every_n = max(1, int(progress_every_n))
    log.info(
        "tq_main_l5.download_start",
        underlying_symbol=underlying_symbol,
        derived_symbol=derived_symbol,
        segment_id=int(segment_id),
        days_total=int(days_total),
        schedule_hash=str(schedule_hash),
    )

    try:
        for idx, day in enumerate(unique_days, start=1):
            log.debug(
                "tq_main_l5.download_day_start",
                symbol=underlying_symbol,
                day=day.isoformat(),
                day_index=int(idx),
                days_total=int(days_total),
            )
            df = api.get_tick_data_series(
                symbol=underlying_symbol,
                start_dt=day,
                end_dt=day + timedelta(days=1),
            )
            if df.empty:
                row_counts[day.isoformat()] = 0
                log.info(
                    "tq_main_l5.download_day_empty",
                    symbol=underlying_symbol,
                    day=day.isoformat(),
                    day_index=int(idx),
                    days_total=int(days_total),
                )
                continue

            df = df.rename(columns={"id": "_tq_id"})
            df["symbol"] = underlying_symbol

            for col in TICK_COLUMN_NAMES:
                if col not in df.columns:
                    df[col] = float("nan")

            out = _questdb_df_for_main_l5(
                df=df,
                derived_symbol=derived_symbol,
                trading_day=day,
                underlying_contract=underlying_symbol,
                segment_id=int(segment_id),
                schedule_hash=str(schedule_hash),
                dataset_version=str(dataset_version),
            )
            backend.ingest_df(table=_QUESTDB_TICKS_MAIN_L5_TABLE, df=out)
            row_counts[day.isoformat()] = int(len(out))
            total_rows += int(len(out))
            log.debug(
                "tq_main_l5.download_day_done",
                symbol=underlying_symbol,
                day=day.isoformat(),
                day_index=int(idx),
                days_total=int(days_total),
                rows=int(len(out)),
                rows_total=int(total_rows),
            )

            if idx == 1 or idx == days_total or idx % progress_every_n == 0 or (time.time() - last_progress_ts) >= progress_every_s:
                log.info(
                    "tq_main_l5.progress",
                    symbol=underlying_symbol,
                    day_index=int(idx),
                    days_total=int(days_total),
                    rows_total=int(total_rows),
                    last_day=day.isoformat(),
                )
                last_progress_ts = time.time()
    finally:
        api.close()

    write_manifest(
        data_dir=data_dir,
        symbols=[derived_symbol],
        start_date=min(trading_days),
        end_date=max(trading_days),
        source="tq_main_l5",
        row_counts=row_counts,
    )

    return {"rows_total": int(total_rows), "row_counts": row_counts}


def download_historical_ticks(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("download_historical_ticks is deprecated (main_l5-only pipeline)")


def download_contract_range(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("download_contract_range is deprecated (main_l5-only pipeline)")


def run_live_recorder(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("run_live_recorder is deferred (Phase-1/2)")
