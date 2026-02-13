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

    def _is_maintenance_error(text: str) -> bool:
        if not text:
            return False
        lowered = text.lower()
        return "maintenance" in lowered or "运维" in text or "维护" in text

    def _new_api() -> TqApi:
        return TqApi(auth=auth)

    def _reset_api(reason: str, err: str) -> None:
        nonlocal api
        try:
            api.close()
        except Exception:
            pass
        api = _new_api()
        log.info("tq_main_l5.api_reset", reason=reason, error=str(err))

    try:
        retry_max = int(os.environ.get("GHTRADER_TQ_RETRY_MAX", "3") or "3")
    except Exception:
        retry_max = 3
    retry_max = max(0, int(retry_max))
    try:
        retry_base_s = float(os.environ.get("GHTRADER_TQ_RETRY_BASE_S", "10") or "10")
    except Exception:
        retry_base_s = 10.0
    retry_base_s = max(0.0, float(retry_base_s))
    try:
        retry_max_s = float(os.environ.get("GHTRADER_TQ_RETRY_MAX_S", "300") or "300")
    except Exception:
        retry_max_s = 300.0
    retry_max_s = max(retry_base_s, float(retry_max_s))
    try:
        maintenance_wait_s = float(os.environ.get("GHTRADER_TQ_MAINTENANCE_WAIT_S", "2100") or "2100")
    except Exception:
        maintenance_wait_s = 2100.0
    maintenance_wait_s = max(0.0, float(maintenance_wait_s))

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
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from ghtrader.util.worker_policy import resolve_worker_count

        # Use unified worker policy (env-driven). No hard-coded worker cap.
        requested_workers: int | None = None
        raw_workers = str(os.environ.get("GHTRADER_INGEST_WORKERS", "") or "").strip()
        if raw_workers:
            try:
                requested_workers = int(raw_workers)
            except Exception:
                requested_workers = None
        max_workers = resolve_worker_count(kind="download", requested=requested_workers)
        log.info(
            "tq_main_l5.worker_policy",
            requested=(int(requested_workers) if requested_workers is not None else None),
            selected=int(max_workers),
        )

        def _download_day_worker(
            idx: int,
            day: date,
            underlying_symbol: str,
            derived_symbol: str,
            segment_id: int,
            schedule_hash: str,
            dataset_version: str,
            days_total: int,
            auth: Any,
            backend: Any,
            retry_max: int,
            retry_base_s: float,
            retry_max_s: float,
            maintenance_wait_s: float,
        ) -> tuple[str, int]:
            """Worker function for downloading a single day."""
            # Create per-thread TqApi instance
            api = TqApi(auth=auth)
            
            def _is_maintenance_error(text: str) -> bool:
                if not text:
                    return False
                lowered = text.lower()
                return "maintenance" in lowered or "运维" in text or "维护" in text

            def _reset_api(reason: str, err: str) -> None:
                nonlocal api
                try:
                    api.close()
                except Exception:
                    pass
                api = TqApi(auth=auth)
                log.info("tq_main_l5.api_reset", reason=reason, error=str(err))

            try:
                attempts = 0
                while True:
                    try:
                        df = api.get_tick_data_series(
                            symbol=underlying_symbol,
                            start_dt=day,
                            end_dt=day + timedelta(days=1),
                        )
                        break
                    except Exception as e:
                        err = str(e)
                        if maintenance_wait_s > 0 and _is_maintenance_error(err):
                            log.warning(
                                "tq_main_l5.maintenance_wait",
                                symbol=underlying_symbol,
                                day=day.isoformat(),
                                wait_s=float(maintenance_wait_s),
                                error=err,
                            )
                            _reset_api("maintenance", err)
                            time.sleep(float(maintenance_wait_s))
                            continue
                        attempts += 1
                        if attempts > retry_max:
                            log.error(
                                "tq_main_l5.download_day_failed",
                                symbol=underlying_symbol,
                                day=day.isoformat(),
                                attempts=int(attempts),
                                error=err,
                            )
                            raise
                        wait_s = min(retry_max_s, retry_base_s * (2 ** (attempts - 1)))
                        log.warning(
                            "tq_main_l5.download_day_retry",
                            symbol=underlying_symbol,
                            day=day.isoformat(),
                            attempt=int(attempts),
                            wait_s=float(wait_s),
                            error=err,
                        )
                        _reset_api("retry", err)
                        if wait_s > 0:
                            time.sleep(float(wait_s))
                
                if df.empty:
                    log.info(
                        "tq_main_l5.download_day_empty",
                        symbol=underlying_symbol,
                        day=day.isoformat(),
                        day_index=int(idx),
                        days_total=int(days_total),
                    )
                    return day.isoformat(), 0

                df = df.rename(columns={"id": "_tq_id"})
                df["symbol"] = underlying_symbol

                for col in TICK_COLUMN_NAMES:
                    if col not in df.columns:
                        df[col] = float("nan")

                rows_before = int(len(df))
                try:
                    from ghtrader.util.l5_detection import l5_mask_df
                    mask = l5_mask_df(df)
                except Exception:
                    mask = None

                if mask is not None:
                    l5_rows = int(mask.sum())
                    if l5_rows <= 0:
                        log.info(
                            "tq_main_l5.download_day_skipped_l1",
                            symbol=underlying_symbol,
                            day=day.isoformat(),
                            day_index=int(idx),
                            days_total=int(days_total),
                            rows_total=0, # Worker doesn't know global total
                        )
                        return day.isoformat(), 0
                    df = df.loc[mask].copy()
                    log.debug(
                        "tq_main_l5.download_day_filtered",
                        symbol=underlying_symbol,
                        day=day.isoformat(),
                        l5_rows=int(l5_rows),
                        rows_before=int(rows_before),
                    )

                out = _questdb_df_for_main_l5(
                    df=df,
                    derived_symbol=derived_symbol,
                    trading_day=day,
                    underlying_contract=underlying_symbol,
                    segment_id=int(segment_id),
                    schedule_hash=str(schedule_hash),
                    dataset_version=str(dataset_version),
                )
                
                # Write to QuestDB (thread-safe if backend handles connection pooling/new connections)
                backend.ingest_df(table=_QUESTDB_TICKS_MAIN_L5_TABLE, df=out)
                
                count = int(len(out))
                log.debug(
                    "tq_main_l5.download_day_done",
                    symbol=underlying_symbol,
                    day=day.isoformat(),
                    day_index=int(idx),
                    days_total=int(days_total),
                    rows=count,
                )
                return day.isoformat(), count
            finally:
                api.close()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _download_day_worker,
                    idx,
                    day,
                    underlying_symbol,
                    derived_symbol,
                    segment_id,
                    schedule_hash,
                    dataset_version,
                    days_total,
                    auth,
                    backend,
                    retry_max,
                    retry_base_s,
                    retry_max_s,
                    maintenance_wait_s,
                ): day
                for idx, day in enumerate(unique_days, start=1)
            }
            
            for future in as_completed(futures):
                day_iso, count = future.result()
                row_counts[day_iso] = count
                total_rows += count
                
                if (time.time() - last_progress_ts) >= progress_every_s:
                    log.info(
                        "tq_main_l5.progress",
                        symbol=underlying_symbol,
                        days_done=len(row_counts),
                        days_total=int(days_total),
                        rows_total=int(total_rows),
                        last_day=day_iso,
                    )
                    last_progress_ts = time.time()

    finally:
        # Main thread API is not used in loop anymore, but kept for structure compatibility if needed
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

