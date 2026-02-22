"""
TqSdk integration: download L5 ticks and write main_l5 directly to QuestDB.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
import os
import threading
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
_WEBSOCKETS_INCOMPAT_WARNED = False


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


def _parse_hms_to_seconds(value: Any) -> int | None:
    try:
        s = str(value or "").strip()
        if not s:
            return None
        parts = s.split(":")
        if len(parts) < 2:
            return None
        h = int(parts[0])
        m = int(parts[1])
        sec = int(parts[2]) if len(parts) > 2 else 0
        return int(h * 3600 + m * 60 + sec)
    except Exception:
        return None


def _intervals_for_trading_day(
    *,
    day: date,
    sessions: list[dict[str, Any]],
    tz: timezone,
    prev_trading_day: dict[date, date] | None = None,
) -> list[dict[str, Any]]:
    intervals: list[dict[str, Any]] = []
    for s in sessions:
        sess = str(s.get("session") or "day")
        start_sec = _parse_hms_to_seconds(s.get("start"))
        end_sec = _parse_hms_to_seconds(s.get("end"))
        if start_sec is None or end_sec is None:
            continue
        if sess == "night":
            base_day = prev_trading_day.get(day) if prev_trading_day and day in prev_trading_day else (day - timedelta(days=1))
        else:
            base_day = day
        base_dt = datetime(base_day.year, base_day.month, base_day.day, tzinfo=tz)
        start_dt = base_dt + timedelta(seconds=int(start_sec))
        end_dt = base_dt + timedelta(seconds=int(end_sec))
        if end_dt <= start_dt:
            end_dt = end_dt + timedelta(days=1)
        start_utc = start_dt.astimezone(timezone.utc)
        end_utc = end_dt.astimezone(timezone.utc)
        intervals.append(
            {
                "session": sess,
                "start_sec": int(start_utc.timestamp()),
                "end_sec": int(end_utc.timestamp()),
            }
        )
    intervals.sort(key=lambda r: (r["start_sec"], r["end_sec"]))
    return intervals


def _apply_exchange_events(
    *,
    day: date,
    intervals: list[dict[str, Any]],
    tz: timezone,
    prev_trading_day: dict[date, date] | None,
    events: list[Any],
) -> list[dict[str, Any]]:
    if not events:
        return intervals

    def _base_day(sess: str) -> date:
        if sess == "night":
            if prev_trading_day and day in prev_trading_day:
                return prev_trading_day[day]
            return day - timedelta(days=1)
        return day

    out = list(intervals)
    for ev in events:
        action = str(getattr(ev, "action", "") or "")
        session = str(getattr(ev, "session", "") or "")
        if action == "skip":
            out = [it for it in out if str(it.get("session") or "") != session]
            continue
        if action not in {"shift_start", "shift_end"}:
            continue
        time_value = getattr(ev, "start_time", None) if action == "shift_start" else getattr(ev, "end_time", None)
        sec_val = _parse_hms_to_seconds(time_value)
        if sec_val is None:
            continue
        idxs = [i for i, it in enumerate(out) if str(it.get("session") or "") == session]
        if not idxs:
            continue
        idx = idxs[0] if action == "shift_start" else idxs[-1]
        it = dict(out[idx])
        base = _base_day(session)
        base_dt = datetime(base.year, base.month, base.day, tzinfo=tz)
        new_sec = int((base_dt + timedelta(seconds=int(sec_val))).astimezone(timezone.utc).timestamp())
        if action == "shift_start":
            if new_sec >= int(it.get("end_sec") or 0):
                continue
            it["start_sec"] = int(new_sec)
        else:
            if new_sec <= int(it.get("start_sec") or 0):
                continue
            it["end_sec"] = int(new_sec)
        out[idx] = it
    out.sort(key=lambda r: (r["start_sec"], r["end_sec"]))
    return out


def _derive_trading_day_series(*, dt_ns: pd.Series, data_dir: Path) -> pd.Series:
    ns = pd.to_numeric(dt_ns, errors="coerce").astype("Int64")
    dt_utc = pd.to_datetime(ns, unit="ns", utc=True, errors="coerce")
    dt_local = dt_utc.dt.tz_convert("Asia/Shanghai")
    base_days = pd.Series(dt_local.dt.date, index=dt_ns.index, dtype="object")
    after18 = pd.Series((dt_local.dt.hour >= 18), index=dt_ns.index).fillna(False).astype(bool)
    trading_days = base_days.copy()

    try:
        from ghtrader.data.trading_calendar import get_trading_calendar

        cal = sorted(set(get_trading_calendar(data_dir=Path(data_dir), refresh=False, allow_download=True)))
    except Exception:
        cal = []
    cal_set = set(cal)

    early_morning = pd.Series((dt_local.dt.hour < 6), index=dt_ns.index).fillna(False).astype(bool)
    early_on_non_trading = pd.Series(False, index=dt_ns.index)
    if cal_set and bool(early_morning.any()):
        early_on_non_trading = early_morning & base_days.map(
            lambda d: isinstance(d, date) and d not in cal_set
        ).fillna(False).astype(bool)

    needs_shift = after18 | early_on_non_trading
    if bool(needs_shift.any()):
        unique_dates = sorted({d for d in trading_days.loc[needs_shift].tolist() if isinstance(d, date)})
        next_map: dict[date, date] = {}
        for d in unique_dates:
            nd: date
            if cal:
                i = bisect_right(cal, d)
                nd = cal[i] if i < len(cal) else (d + timedelta(days=1))
            else:
                nd = d + timedelta(days=1)
            next_map[d] = nd
        shifted = trading_days.loc[needs_shift].map(next_map)
        fallback = trading_days.loc[needs_shift].map(lambda d: (d + timedelta(days=1)) if isinstance(d, date) else d)
        trading_days.loc[needs_shift] = shifted.where(shifted.notna(), fallback)
    return trading_days


def _trim_to_target_trading_day(*, df: pd.DataFrame, day: date, data_dir: Path) -> tuple[pd.DataFrame, int]:
    if df.empty or "datetime" not in df.columns:
        return df.copy(), 0
    trading_days = _derive_trading_day_series(dt_ns=df["datetime"], data_dir=Path(data_dir))
    mask = trading_days == day
    kept = int(mask.sum())
    if kept <= 0:
        return df.iloc[0:0].copy(), int(len(df))
    out = df.loc[mask].copy()
    return out, int(len(df) - kept)


def _trim_to_session_windows(*, df: pd.DataFrame, windows: list[dict[str, Any]]) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df.copy(), 0
    if not windows:
        return df.iloc[0:0].copy(), int(len(df))
    sec = (pd.to_numeric(df.get("datetime"), errors="coerce").fillna(0).astype("int64") // 1_000_000_000)
    mask = pd.Series(False, index=df.index)
    for it in windows:
        start_sec = int(it.get("start_sec") or 0)
        end_sec = int(it.get("end_sec") or 0)
        mask = mask | ((sec >= start_sec) & (sec <= end_sec))
    kept = int(mask.sum())
    if kept <= 0:
        return df.iloc[0:0].copy(), int(len(df))
    out = df.loc[mask].copy()
    return out, int(len(df) - kept)


def _build_session_windows_by_day(
    *,
    trading_days: list[date],
    data_dir: Path,
    exchange: str | None,
    variety: str | None,
) -> dict[date, list[dict[str, Any]]]:
    ex = str(exchange or "").upper().strip()
    var = str(variety or "").lower().strip()
    if not ex or not var or not trading_days:
        return {}
    try:
        from zoneinfo import ZoneInfo

        tz: timezone = ZoneInfo("Asia/Shanghai")
    except Exception:
        tz = timezone.utc
    try:
        from ghtrader.data.trading_sessions import read_trading_sessions_cache

        payload = read_trading_sessions_cache(data_dir=Path(data_dir), exchange=ex, variety=var) or {}
        sessions = payload.get("sessions") if isinstance(payload, dict) else None
    except Exception:
        sessions = None
    if not isinstance(sessions, list) or not sessions:
        return {}

    unique_days = sorted(set(trading_days))
    prev_trading_day: dict[date, date] = {}
    try:
        from ghtrader.data.trading_calendar import get_trading_calendar

        full_cal = sorted(set(get_trading_calendar(data_dir=Path(data_dir), refresh=False, allow_download=True)))
    except Exception:
        full_cal = []
    if full_cal:
        cal_set = set(full_cal)
        for d in unique_days:
            i = bisect_right(full_cal, d) - 1
            if i > 0:
                prev_trading_day[d] = full_cal[i - 1] if full_cal[i] == d else full_cal[i]
            elif i == 0 and full_cal[0] == d and len(full_cal) > 1:
                pass
    if not prev_trading_day:
        for idx, d in enumerate(unique_days):
            if idx > 0:
                prev_trading_day[d] = unique_days[idx - 1]

    holiday_gap_days: set[date] = set()
    try:
        from ghtrader.data.trading_calendar import get_holidays

        holidays = set(get_holidays(data_dir=Path(data_dir), refresh=False, allow_download=True))
    except Exception:
        holidays = set()
    if holidays and len(unique_days) > 1:
        for idx in range(1, len(unique_days)):
            prev_day = unique_days[idx - 1]
            cur_day = unique_days[idx]
            check_day = prev_day + timedelta(days=1)
            while check_day < cur_day:
                if check_day in holidays:
                    holiday_gap_days.add(cur_day)
                    break
                check_day = check_day + timedelta(days=1)

    try:
        from ghtrader.data.exchange_events import events_for_day, load_exchange_events

        loaded_events = load_exchange_events(data_dir=Path(data_dir), exchange=ex, variety=var)
    except Exception:
        loaded_events = []

    windows: dict[date, list[dict[str, Any]]] = {}
    for d in unique_days:
        intervals = _intervals_for_trading_day(day=d, sessions=sessions, tz=tz, prev_trading_day=prev_trading_day)
        if d in holiday_gap_days:
            intervals = [it for it in intervals if str(it.get("session") or "") != "night"]
        try:
            day_events = events_for_day(loaded_events, d) if loaded_events else []
        except Exception:
            day_events = []
        intervals = _apply_exchange_events(
            day=d,
            intervals=intervals,
            tz=tz,
            prev_trading_day=prev_trading_day,
            events=day_events,
        )
        windows[d] = intervals
    return windows


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
    exchange: str | None = None,
    variety: str | None = None,
    dataset_version: DatasetVersion = "v2",
    write_manifest_file: bool = True,
    workers: int | None = None,
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
    global _WEBSOCKETS_INCOMPAT_WARNED
    websockets_version_s = ""
    websockets_incompat = False
    try:
        import websockets  # type: ignore
        from packaging import version as _version

        websockets_version_s = str(getattr(websockets, "__version__", "") or "")
        websockets_incompat = _version.parse(websockets_version_s) >= _version.parse("16.0")
    except Exception:
        websockets_incompat = False
    if websockets_incompat and not _WEBSOCKETS_INCOMPAT_WARNED:
        _WEBSOCKETS_INCOMPAT_WARNED = True
        log.warning(
            "tq_main_l5.websockets_compat_risk",
            websockets_version=websockets_version_s,
            suggestion="Pin websockets<16 for stable TqSdk reconnect behavior.",
        )

    backend = _make_questdb_backend_required()
    backend.ensure_table(table=_QUESTDB_TICKS_MAIN_L5_TABLE, include_segment_metadata=True)

    auth = get_tqsdk_auth()

    def _make_data_api() -> Any:
        # Keep TqSdk transport logs quiet in job output.
        try:
            return TqApi(auth=auth, disable_print=True)
        except TypeError:
            # Backward compatibility for older signatures.
            return TqApi(auth=auth)

    def _is_maintenance_error(text: str) -> bool:
        if not text:
            return False
        lowered = text.lower()
        return "maintenance" in lowered or "运维" in text or "维护" in text

    def _is_timeout_error(text: str) -> bool:
        if not text:
            return False
        lowered = text.lower()
        return "timeout" in lowered or "timed out" in lowered or "超时" in text

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
    try:
        maintenance_max_retries = int(os.environ.get("GHTRADER_TQ_MAINTENANCE_MAX_RETRIES", "3") or "3")
    except Exception:
        maintenance_max_retries = 3
    maintenance_max_retries = max(0, int(maintenance_max_retries))
    try:
        maintenance_max_total_wait_s = float(
            os.environ.get("GHTRADER_TQ_MAINTENANCE_MAX_TOTAL_WAIT_S", "7200") or "7200"
        )
    except Exception:
        maintenance_max_total_wait_s = 7200.0
    maintenance_max_total_wait_s = max(0.0, float(maintenance_max_total_wait_s))
    try:
        timeout_reset_after = int(os.environ.get("GHTRADER_TQ_TIMEOUT_RESET_AFTER", "2") or "2")
    except Exception:
        timeout_reset_after = 2
    timeout_reset_after = max(1, int(timeout_reset_after))

    row_counts: dict[str, int] = {}
    unique_days = sorted(set(trading_days))
    session_windows_by_day = _build_session_windows_by_day(
        trading_days=unique_days,
        data_dir=Path(data_dir),
        exchange=exchange,
        variety=variety,
    )
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
        session_window_filter=bool(session_windows_by_day),
    )

    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from ghtrader.util.worker_policy import resolve_worker_count

        # Use unified worker policy (env-driven). No hard-coded worker cap.
        requested_workers: int | None = None
        if workers is not None:
            try:
                requested_workers = int(workers)
            except Exception:
                requested_workers = None
        if requested_workers is None:
            raw_workers = str(os.environ.get("GHTRADER_INGEST_WORKERS", "") or "").strip()
            if raw_workers:
                try:
                    requested_workers = int(raw_workers)
                except Exception:
                    requested_workers = None
        max_workers = resolve_worker_count(kind="download", requested=requested_workers)
        if websockets_incompat:
            # websockets>=16 triggers unstable callbacks inside current TqSdk.
            # Force single-worker mode to avoid connection storms.
            max_workers = 1
        log.info(
            "tq_main_l5.worker_policy",
            requested=(int(requested_workers) if requested_workers is not None else None),
            selected=int(max_workers),
            websockets_version=websockets_version_s or None,
            websockets_incompat=bool(websockets_incompat),
        )

        # Keep one TqApi per executor thread to reduce connection churn.
        api_tls = threading.local()
        api_registry_lock = threading.Lock()
        api_registry: list[Any] = []
        api_registry_ids: set[int] = set()

        def _register_api(api_obj: Any) -> None:
            api_id = int(id(api_obj))
            with api_registry_lock:
                if api_id in api_registry_ids:
                    return
                api_registry_ids.add(api_id)
                api_registry.append(api_obj)

        def _get_thread_api() -> Any:
            api_obj = getattr(api_tls, "api", None)
            if api_obj is None:
                api_obj = _make_data_api()
                api_tls.api = api_obj
                _register_api(api_obj)
            return api_obj

        def _reset_thread_api(*, reason: str, err: str) -> Any:
            api_obj = getattr(api_tls, "api", None)
            if api_obj is not None:
                try:
                    api_obj.close()
                except Exception:
                    pass
            api_obj = _make_data_api()
            api_tls.api = api_obj
            _register_api(api_obj)
            log.info("tq_main_l5.api_reset", reason=reason, error=str(err))
            return api_obj

        def _close_all_thread_apis() -> None:
            with api_registry_lock:
                apis = list(api_registry)
                api_registry.clear()
                api_registry_ids.clear()
            for api_obj in apis:
                try:
                    api_obj.close()
                except Exception:
                    pass

        def _download_day_worker(
            idx: int,
            day: date,
            underlying_symbol: str,
            derived_symbol: str,
            segment_id: int,
            schedule_hash: str,
            dataset_version: str,
            days_total: int,
            backend: Any,
            retry_max: int,
            retry_base_s: float,
            retry_max_s: float,
            maintenance_wait_s: float,
            maintenance_max_retries: int,
            maintenance_max_total_wait_s: float,
            day_session_windows: list[dict[str, Any]] | None,
        ) -> tuple[str, int]:
            """Worker function for downloading a single day."""
            api = _get_thread_api()
            attempts = 0
            maintenance_attempts = 0
            maintenance_wait_total = 0.0
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
                        if maintenance_attempts >= maintenance_max_retries:
                            log.error(
                                "tq_main_l5.maintenance_exhausted",
                                symbol=underlying_symbol,
                                day=day.isoformat(),
                                attempts=int(maintenance_attempts),
                                max_attempts=int(maintenance_max_retries),
                                wait_total_s=float(maintenance_wait_total),
                                max_total_wait_s=float(maintenance_max_total_wait_s),
                                error=err,
                            )
                            raise RuntimeError(
                                f"TqSdk maintenance retry exhausted for {underlying_symbol} {day.isoformat()}: {err}"
                            )
                        wait_s = float(maintenance_wait_s)
                        if maintenance_max_total_wait_s > 0:
                            remaining_wait = float(maintenance_max_total_wait_s) - float(maintenance_wait_total)
                            if remaining_wait <= 0:
                                log.error(
                                    "tq_main_l5.maintenance_wait_budget_exhausted",
                                    symbol=underlying_symbol,
                                    day=day.isoformat(),
                                    attempts=int(maintenance_attempts),
                                    max_attempts=int(maintenance_max_retries),
                                    wait_total_s=float(maintenance_wait_total),
                                    max_total_wait_s=float(maintenance_max_total_wait_s),
                                    error=err,
                                )
                                raise RuntimeError(
                                    f"TqSdk maintenance wait budget exhausted for {underlying_symbol} {day.isoformat()}: {err}"
                                )
                            wait_s = min(wait_s, float(remaining_wait))
                        maintenance_attempts += 1
                        maintenance_wait_total += float(wait_s)
                        log.warning(
                            "tq_main_l5.maintenance_wait",
                            symbol=underlying_symbol,
                            day=day.isoformat(),
                            attempt=int(maintenance_attempts),
                            max_attempts=int(maintenance_max_retries),
                            wait_s=float(wait_s),
                            error=err,
                        )
                        api = _reset_thread_api(reason="maintenance", err=err)
                        if wait_s > 0:
                            time.sleep(float(wait_s))
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
                    if (not _is_timeout_error(err)) or attempts >= timeout_reset_after:
                        api = _reset_thread_api(reason="retry", err=err)
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

            if day_session_windows is not None:
                before_trim = int(len(df))
                df, dropped_outside = _trim_to_session_windows(df=df, windows=day_session_windows)
                if df.empty:
                    log.info(
                        "tq_main_l5.download_day_empty_after_session_trim",
                        symbol=underlying_symbol,
                        day=day.isoformat(),
                        day_index=int(idx),
                        days_total=int(days_total),
                        dropped_rows=int(before_trim),
                    )
                    return day.isoformat(), 0
                if dropped_outside > 0:
                    log.info(
                        "tq_main_l5.download_day_trimmed_outside_sessions",
                        symbol=underlying_symbol,
                        day=day.isoformat(),
                        dropped_rows=int(dropped_outside),
                        kept_rows=int(len(df)),
                    )
            else:
                df, dropped_cross_day = _trim_to_target_trading_day(df=df, day=day, data_dir=Path(data_dir))
                if df.empty:
                    log.info(
                        "tq_main_l5.download_day_empty_after_day_trim",
                        symbol=underlying_symbol,
                        day=day.isoformat(),
                        day_index=int(idx),
                        days_total=int(days_total),
                        dropped_rows=int(rows_before),
                    )
                    return day.isoformat(), 0
                if dropped_cross_day > 0:
                    log.info(
                        "tq_main_l5.download_day_trimmed_cross_day_rows",
                        symbol=underlying_symbol,
                        day=day.isoformat(),
                        dropped_rows=int(dropped_cross_day),
                        kept_rows=int(len(df)),
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
                    backend,
                    retry_max,
                    retry_base_s,
                    retry_max_s,
                    maintenance_wait_s,
                    maintenance_max_retries,
                    maintenance_max_total_wait_s,
                    session_windows_by_day.get(day) if session_windows_by_day else None,
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
        try:
            _close_all_thread_apis()
        except Exception:
            pass

    if write_manifest_file:
        write_manifest(
            data_dir=data_dir,
            symbols=[derived_symbol],
            start_date=min(trading_days),
            end_date=max(trading_days),
            source="tq_main_l5",
            row_counts=row_counts,
        )

    return {"rows_total": int(total_rows), "row_counts": row_counts}

