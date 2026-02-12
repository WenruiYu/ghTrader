from __future__ import annotations

import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.config import get_data_dir, get_runs_dir
from ghtrader.data.exchange_events import ExchangeEvent, events_for_day, load_exchange_events
from ghtrader.data.manifest import list_manifests, read_manifest
from ghtrader.data.trading_sessions import read_trading_sessions_cache
from ghtrader.data.trading_calendar import get_holidays
from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
from ghtrader.questdb.main_l5_validate import (
    MainL5ValidateGapRow,
    MainL5ValidateSummaryRow,
    clear_main_l5_validate_gap_rows,
    ensure_main_l5_tick_gaps_table,
    ensure_main_l5_validate_gaps_table,
    ensure_main_l5_validate_summary_table,
    insert_main_l5_validate_gap_rows,
    upsert_main_l5_tick_gaps_from_validate_summary,
    upsert_main_l5_validate_summary_rows,
)
from ghtrader.questdb.main_schedule import fetch_schedule
from ghtrader.questdb.queries import query_symbol_day_bounds
from ghtrader.tq.runtime import trading_day_from_ts_ns
from ghtrader.util.json_io import read_json, write_json_atomic
from ghtrader.util.l5_detection import l5_mask_df, l5_sql_condition

log = structlog.get_logger()


def _symbol_slug(symbol: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", str(symbol or "").strip())
    return s.strip("_").lower() or "symbol"


def _report_dir(runs_dir: Path) -> Path:
    return runs_dir / "control" / "reports" / "main_l5_validate"


def _latest_path(*, runs_dir: Path, exchange: str, variety: str, derived_symbol: str) -> Path:
    ex = str(exchange).upper().strip()
    v = str(variety).lower().strip()
    sym = _symbol_slug(derived_symbol)
    return _report_dir(runs_dir) / f"main_l5_validate_exchange={ex}_var={v}_symbol={sym}_latest.json"


def read_latest_validation_report(
    *, runs_dir: Path, exchange: str, variety: str, derived_symbol: str
) -> dict[str, Any] | None:
    latest = _latest_path(
        runs_dir=runs_dir, exchange=exchange, variety=variety, derived_symbol=derived_symbol
    )
    if latest.exists():
        obj = read_json(latest)
        if isinstance(obj, dict):
            obj = dict(obj)
            obj["_path"] = str(latest)
            return obj
    rep_dir = _report_dir(runs_dir)
    if not rep_dir.exists():
        return None
    ex = str(exchange).upper().strip()
    v = str(variety).lower().strip()
    sym = _symbol_slug(derived_symbol)
    pat = f"main_l5_validate_exchange={ex}_var={v}_symbol={sym}_*.json"
    paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)
    for p in paths:
        obj = read_json(p)
        if isinstance(obj, dict):
            obj = dict(obj)
            obj["_path"] = str(p)
            return obj
    return None


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
        start_raw = s.get("start")
        end_raw = s.get("end")
        start_sec = _parse_hms_to_seconds(start_raw)
        end_sec = _parse_hms_to_seconds(end_raw)
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
        start_sec_utc = int(start_utc.timestamp())
        end_sec_utc = int(end_utc.timestamp())
        intervals.append(
            {
                "session": sess,
                "start_sec": int(start_sec_utc),
                "end_sec": int(end_sec_utc),
                "start_ts": start_utc.isoformat(),
                "end_ts": end_utc.isoformat(),
            }
        )
    intervals.sort(key=lambda r: (r["start_sec"], r["end_sec"]))
    return intervals


def _sec_to_iso(sec: int) -> str:
    return datetime.fromtimestamp(int(sec), tz=timezone.utc).isoformat()


def _apply_exchange_events(
    *,
    day: date,
    intervals: list[dict[str, Any]],
    tz: timezone,
    prev_trading_day: dict[date, date] | None,
    events: list[ExchangeEvent],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not events:
        return intervals, []

    def _base_day(sess: str) -> date:
        if sess == "night":
            if prev_trading_day and day in prev_trading_day:
                return prev_trading_day[day]
            return day - timedelta(days=1)
        return day

    out = list(intervals)
    applied: list[dict[str, Any]] = []

    for ev in events:
        if ev.action != "skip":
            continue
        before = len(out)
        out = [it for it in out if str(it.get("session") or "") != ev.session]
        if len(out) != before:
            applied.append(
                {
                    "trading_day": day.isoformat(),
                    "exchange": ev.exchange,
                    "variety": ev.variety,
                    "session": ev.session,
                    "action": ev.action,
                    "reason": ev.reason,
                    "source_url": ev.source_url,
                }
            )

    for ev in events:
        if ev.action not in {"shift_start", "shift_end"}:
            continue
        time_value = ev.start_time if ev.action == "shift_start" else ev.end_time
        if not time_value:
            continue
        sec_val = _parse_hms_to_seconds(time_value)
        if sec_val is None:
            continue
        idxs = [i for i, it in enumerate(out) if str(it.get("session") or "") == ev.session]
        if not idxs:
            continue
        idx = idxs[0] if ev.action == "shift_start" else idxs[-1]
        it = dict(out[idx])
        base = _base_day(ev.session)
        base_dt = datetime(base.year, base.month, base.day, tzinfo=tz)
        new_dt = base_dt + timedelta(seconds=int(sec_val))
        new_utc = new_dt.astimezone(timezone.utc)
        new_sec = int(new_utc.timestamp())
        if ev.action == "shift_start":
            if new_sec >= int(it.get("end_sec") or 0):
                continue
            it["start_sec"] = int(new_sec)
            it["start_ts"] = new_utc.isoformat()
        else:
            if new_sec <= int(it.get("start_sec") or 0):
                continue
            it["end_sec"] = int(new_sec)
            it["end_ts"] = new_utc.isoformat()
        it["override_reason"] = ev.reason
        out[idx] = it
        applied.append(
            {
                "trading_day": day.isoformat(),
                "exchange": ev.exchange,
                "variety": ev.variety,
                "session": ev.session,
                "action": ev.action,
                "time": time_value,
                "reason": ev.reason,
                "source_url": ev.source_url,
            }
        )

    out.sort(key=lambda r: (r["start_sec"], r["end_sec"]))
    return out, applied




def _load_latest_manifest(*, data_dir: Path, derived_symbol: str) -> dict[str, Any] | None:
    paths = list_manifests(data_dir=data_dir)
    if not paths:
        return None
    paths_sorted = sorted(paths, key=lambda p: p.stat().st_mtime_ns, reverse=True)
    sym = str(derived_symbol).strip()
    for p in paths_sorted[:200]:
        try:
            manifest = read_manifest(p)
        except Exception:
            continue
        if str(manifest.source or "").strip() != "tq_main_l5":
            continue
        if sym and sym not in (manifest.symbols or []):
            continue
        obj = manifest.to_dict()
        obj["_path"] = str(p)
        return obj
    return None


def _fetch_day_second_stats(
    *,
    cfg: Any,
    symbol: str,
    start_day: date,
    end_day: date,
    l5_only: bool = True,
) -> dict[date, dict[str, int]]:
    l5_cond = l5_sql_condition() if l5_only else ""
    l5_where = f" AND {l5_cond} " if l5_cond else " "
    sql = (
        "WITH per_sec AS ("
        "  SELECT cast(trading_day as string) AS trading_day, "
        "         cast(datetime_ns/1000000000 as long) AS sec, "
        "         count() AS n "
        "  FROM ghtrader_ticks_main_l5_v2 "
        f"  WHERE symbol=%s AND ticks_kind='main_l5' AND dataset_version='v2'{l5_where}"
        "    AND cast(trading_day as string) >= %s AND cast(trading_day as string) <= %s "
        "  GROUP BY trading_day, sec"
        ") "
        "SELECT trading_day, "
        "       count() AS seconds_with_ticks, "
        "       sum(case when n=1 then 1 else 0 end) AS seconds_with_one, "
        "       sum(case when n>=2 then 1 else 0 end) AS seconds_with_two_plus "
        "FROM per_sec GROUP BY trading_day"
    )
    out: dict[date, dict[str, int]] = {}
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol).strip(), start_day.isoformat(), end_day.isoformat()])
            for row in cur.fetchall():
                try:
                    td = date.fromisoformat(str(row[0]))
                except Exception:
                    continue
                out[td] = {
                    "seconds_with_ticks": int(row[1] or 0),
                    "seconds_with_one": int(row[2] or 0),
                    "seconds_with_two_plus": int(row[3] or 0),
                }
    return out


def _fetch_per_second_counts(
    *,
    cfg: Any,
    symbol: str,
    trading_day: date,
    l5_only: bool = True,
) -> dict[int, int]:
    l5_cond = l5_sql_condition() if l5_only else ""
    l5_where = f" AND {l5_cond} " if l5_cond else " "
    sql = (
        "SELECT cast(datetime_ns/1000000000 as long) AS sec, count() AS n "
        "FROM ghtrader_ticks_main_l5_v2 "
        f"WHERE symbol=%s AND ticks_kind='main_l5' AND dataset_version='v2'{l5_where}AND trading_day=%s "
        "GROUP BY sec"
    )
    out: dict[int, int] = {}
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol).strip(), trading_day.isoformat()])
            for sec, n in cur.fetchall():
                if sec is None:
                    continue
                out[int(sec)] = int(n or 0)
    return out


def get_last_validated_day(
    *,
    cfg: Any,
    symbol: str,
    table: str = "ghtrader_main_l5_validate_summary_v2",
) -> date | None:
    """Get the last validated trading day for a symbol from QuestDB."""
    sql = f"SELECT max(cast(trading_day as string)) FROM {table} WHERE symbol=%s"
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol).strip()])
            row = cur.fetchone()
            if row and row[0]:
                try:
                    return date.fromisoformat(str(row[0]))
                except Exception:
                    pass
    return None


def validate_main_l5(
    *,
    exchange: str,
    variety: str,
    derived_symbol: str | None = None,
    data_dir: Path | None = None,
    runs_dir: Path | None = None,
    start_day: date | None = None,
    end_day: date | None = None,
    tqsdk_check: bool | None = None,
    tqsdk_check_max_days: int | None = None,
    tqsdk_check_max_segments: int | None = None,
    max_segments_per_day: int | None = None,
    gap_threshold_s: float | None = None,
    strict_ratio: float | None = None,
    incremental: bool = False,
) -> tuple[dict[str, Any], Path | None]:
    if data_dir is None:
        data_dir = get_data_dir()
    if runs_dir is None:
        runs_dir = get_runs_dir()

    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    ds = str(derived_symbol or "").strip() or f"KQ.m@{ex}.{var}"

    cfg = make_questdb_query_config_from_env()

    if incremental and start_day is None:
        last_val = get_last_validated_day(cfg=cfg, symbol=ds)
        if last_val:
            start_day = last_val + timedelta(days=1)
            log.info("main_l5_validate.incremental", last_validated=last_val.isoformat(), new_start=start_day.isoformat())

    schedule = fetch_schedule(cfg=cfg, exchange=ex, variety=var, start_day=start_day, end_day=end_day, connect_timeout_s=2)
    if schedule.empty:
        raise FileNotFoundError(f"No schedule rows found for {ex}.{var} (build main-schedule first).")

    schedule = schedule.dropna(subset=["trading_day", "main_contract"]).sort_values("trading_day").reset_index(drop=True)
    schedule_days = [d for d in schedule["trading_day"].tolist() if isinstance(d, date)]
    schedule_start = min(schedule_days) if schedule_days else None
    schedule_end = max(schedule_days) if schedule_days else None
    contract_by_day: dict[date, str] = {
        d: str(mc) for d, mc in schedule[["trading_day", "main_contract"]].itertuples(index=False, name=None)
        if isinstance(d, date)
    }

    aligned_start: date | None = None
    try:
        coverage = query_symbol_day_bounds(
            cfg=cfg,
            table="ghtrader_ticks_main_l5_v2",
            symbols=[ds],
            dataset_version="v2",
            ticks_kind="main_l5",
            l5_only=True,
        )
        first_ns = (coverage.get(ds) or {}).get("first_ns")
        if first_ns is not None:
            aligned_start = trading_day_from_ts_ns(int(first_ns), data_dir=Path(data_dir))
    except Exception:
        aligned_start = None

    if start_day is not None and (aligned_start is None or start_day > aligned_start):
        aligned_start = start_day

    if aligned_start is not None:
        schedule = schedule[schedule["trading_day"] >= aligned_start].reset_index(drop=True)
        schedule_days = [d for d in schedule["trading_day"].tolist() if isinstance(d, date)]
        contract_by_day = {
            d: str(mc)
            for d, mc in schedule[["trading_day", "main_contract"]].itertuples(index=False, name=None)
            if isinstance(d, date)
        }
        if schedule_days:
            schedule_start = min(schedule_days)
            schedule_end = max(schedule_days)
        else:
            raise ValueError("No schedule days available after alignment")
    schedule_hash = ""
    try:
        schedule_hash = str(schedule["schedule_hash"].dropna().astype(str).iloc[0])
    except Exception:
        schedule_hash = ""

    sessions_payload = read_trading_sessions_cache(data_dir=data_dir, exchange=ex, variety=var) or {}
    sessions = sessions_payload.get("sessions") if isinstance(sessions_payload, dict) else None
    if not isinstance(sessions, list) or not sessions:
        report = {
            "ok": False,
            "error": "trading_sessions_missing",
            "exchange": ex,
            "variety": var,
            "derived_symbol": ds,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        out_path = _write_report(runs_dir=Path(runs_dir), report=report, exchange=ex, variety=var, derived_symbol=ds)
        return report, out_path

    holiday_gap_days: set[date] = set()
    try:
        holidays = set(get_holidays(data_dir=Path(data_dir), refresh=False, allow_download=True))
    except Exception:
        holidays = set()
    if holidays and len(schedule_days) > 1:
        for i in range(1, len(schedule_days)):
            prev_day = schedule_days[i - 1]
            cur_day = schedule_days[i]
            check_day = prev_day + timedelta(days=1)
            while check_day < cur_day:
                if check_day in holidays:
                    holiday_gap_days.add(cur_day)
                    break
                check_day = check_day + timedelta(days=1)

    exchange_events = load_exchange_events(data_dir=Path(data_dir), exchange=ex, variety=var)
    event_override_days: set[date] = set()

    ensure_main_l5_validate_summary_table(cfg=cfg)
    ensure_main_l5_validate_gaps_table(cfg=cfg)
    ensure_main_l5_tick_gaps_table(cfg=cfg)

    if tqsdk_check is None:
        raw = str(os.environ.get("GHTRADER_L5_VALIDATE_TQ_CHECK", "1") or "1").strip().lower()
        tqsdk_check = raw not in {"0", "false", "no", "off"}
    if tqsdk_check_max_days is None:
        try:
            tqsdk_check_max_days = int(os.environ.get("GHTRADER_L5_VALIDATE_TQ_CHECK_MAX_DAYS", "2") or "2")
        except Exception:
            tqsdk_check_max_days = 2
    if tqsdk_check_max_segments is None:
        try:
            tqsdk_check_max_segments = int(os.environ.get("GHTRADER_L5_VALIDATE_TQ_CHECK_MAX_SEGMENTS", "8") or "8")
        except Exception:
            tqsdk_check_max_segments = 8
    if max_segments_per_day is None:
        try:
            max_segments_per_day = int(os.environ.get("GHTRADER_L5_VALIDATE_MAX_SEGMENTS_PER_DAY", "200") or "200")
        except Exception:
            max_segments_per_day = 200
    try:
        report_max_days = int(os.environ.get("GHTRADER_L5_VALIDATE_REPORT_MAX_DAYS", "200") or "200")
    except Exception:
        report_max_days = 200
    if gap_threshold_s is None:
        try:
            gap_threshold_s = float(os.environ.get("GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S", "5") or "5")
        except Exception:
            gap_threshold_s = 5.0
    if strict_ratio is None:
        try:
            strict_ratio = float(os.environ.get("GHTRADER_L5_VALIDATE_STRICT_RATIO", "0.8") or "0.8")
        except Exception:
            strict_ratio = 0.8
    tqsdk_check_max_days = max(0, int(tqsdk_check_max_days))
    tqsdk_check_max_segments = max(0, int(tqsdk_check_max_segments))
    max_segments_per_day = max(10, int(max_segments_per_day))
    report_max_days = max(1, int(report_max_days))
    gap_threshold_s = max(0.5, float(gap_threshold_s))
    strict_ratio = max(0.0, min(1.0, float(strict_ratio)))

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

    tz = None
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
    except Exception:
        tz = timezone.utc

    manifest = _load_latest_manifest(data_dir=Path(data_dir), derived_symbol=ds)
    row_counts = {}
    if isinstance(manifest, dict):
        rc = manifest.get("row_counts")
        if isinstance(rc, dict):
            row_counts = rc

    tqsdk_api = None
    tqsdk_error = ""
    if tqsdk_check:
        try:
            from tqsdk import TqApi  # type: ignore
            from ghtrader.config import get_tqsdk_auth

            tqsdk_api = TqApi(auth=get_tqsdk_auth(), disable_print=True)
        except Exception as e:
            tqsdk_api = None
            tqsdk_error = str(e)
            tqsdk_check = False

    log.info(
        "main_l5_validate.start",
        exchange=ex,
        var=var,
        derived_symbol=ds,
        start=(schedule_start.isoformat() if schedule_start else ""),
        end=(schedule_end.isoformat() if schedule_end else ""),
        days_total=int(len(schedule_days)),
        tqsdk_check=bool(tqsdk_check),
        gap_threshold_s=float(gap_threshold_s),
        strict_ratio=float(strict_ratio),
    )

    day_stats: dict[date, dict[str, int]] = {}
    if schedule_start and schedule_end:
        try:
            day_stats = _fetch_day_second_stats(cfg=cfg, symbol=ds, start_day=schedule_start, end_day=schedule_end, l5_only=True)
        except Exception:
            day_stats = {}

    days_all: list[dict[str, Any]] = []
    missing_days: list[str] = []
    missing_segments_total = 0
    missing_seconds_total = 0
    missing_half_seconds_total = 0
    boundary_missing_seconds_total = 0
    ticks_outside_sessions_total = 0
    max_gap_s = 0
    seconds_with_ticks_total = 0
    seconds_with_one_tick_total = 0
    seconds_with_two_plus_total = 0
    expected_seconds_total = 0
    expected_seconds_strict_total = 0
    total_segments_total = 0

    summary_rows: list[MainL5ValidateSummaryRow] = []
    gap_rows: list[MainL5ValidateGapRow] = []

    gap_bucket_defs = [
        ("2_5", 2, 5),
        ("6_15", 6, 15),
        ("16_30", 16, 30),
        ("gt_30", 31, None),
    ]
    gap_bucket_totals: dict[str, int] = {label: 0 for label, _, _ in gap_bucket_defs}
    gap_bucket_totals_by_session: dict[str, dict[str, int]] = {}
    gap_count_gt_30_total = 0
    exchange_events_used: list[dict[str, Any]] = []
    session_end_lags: list[int] = []

    tqsdk_checked_days = 0
    tqsdk_checked_segments = 0
    tqsdk_missing_segments = 0
    tqsdk_present_segments = 0
    tqsdk_check_errors = 0

    last_progress_ts = time.time()

    from bisect import bisect_left, bisect_right

    prev_trading_day: dict[date, date] = {}
    for i, d in enumerate(schedule_days):
        if i > 0:
            prev_trading_day[d] = schedule_days[i - 1]

    for idx, day in enumerate(schedule_days, start=1):
        contract = contract_by_day.get(day, "")
        intervals = _intervals_for_trading_day(day=day, sessions=sessions, tz=tz, prev_trading_day=prev_trading_day)
        if day in holiday_gap_days:
            intervals = [it for it in intervals if it.get("session") != "night"]
        events = events_for_day(exchange_events, day)
        intervals, applied_events = _apply_exchange_events(
            day=day,
            intervals=intervals,
            tz=tz,
            prev_trading_day=prev_trading_day,
            events=events,
        )
        if applied_events:
            event_override_days.add(day)
            exchange_events_used.extend(applied_events)
        if not intervals:
            continue

        stats = day_stats.get(day) or {"seconds_with_ticks": 0, "seconds_with_one": 0, "seconds_with_two_plus": 0}
        seconds_with_ticks_day = int(stats.get("seconds_with_ticks") or 0)
        seconds_with_one_day = int(stats.get("seconds_with_one") or 0)
        seconds_with_two_plus_day = int(stats.get("seconds_with_two_plus") or 0)
        two_plus_ratio_day = (
            float(seconds_with_two_plus_day) / float(seconds_with_ticks_day) if seconds_with_ticks_day > 0 else 0.0
        )
        day_cadence = "fixed_0p5s" if two_plus_ratio_day >= float(strict_ratio) else "event"

        sec_counts = _fetch_per_second_counts(cfg=cfg, symbol=ds, trading_day=day, l5_only=True) if seconds_with_ticks_day > 0 else {}
        secs_sorted = sorted(sec_counts.keys())
        session_end_sec = max((int(it["end_sec"]) for it in intervals), default=None)
        last_tick_sec = None
        if secs_sorted and session_end_sec is not None:
            for sec in reversed(secs_sorted):
                for it in intervals:
                    if int(it["start_sec"]) <= sec <= int(it["end_sec"]):
                        last_tick_sec = sec
                        break
                if last_tick_sec is not None:
                    break
        session_end_lag_s = None
        if session_end_sec is not None and last_tick_sec is not None:
            session_end_lag_s = int(max(0, int(session_end_sec) - int(last_tick_sec)))
            session_end_lags.append(session_end_lag_s)

        expected_seconds = 0
        expected_seconds_strict = 0
        boundary_seconds: set[int] = set()
        for it in intervals:
            start_sec = int(it["start_sec"])
            end_sec = int(it["end_sec"])
            if end_sec < start_sec:
                continue
            expected_seconds += int(end_sec - start_sec + 1)
            boundary_seconds.add(start_sec)
            boundary_seconds.add(end_sec)
            eff_start = start_sec + 1
            eff_end = end_sec - 1
            if eff_end >= eff_start:
                expected_seconds_strict += int(eff_end - eff_start + 1)

        observed_in_sessions = 0
        observed_outside_sessions = 0
        seconds_with_one = 0
        seconds_with_two_plus = 0
        interval_idx = 0
        for sec in secs_sorted:
            while interval_idx < len(intervals) and sec > int(intervals[interval_idx]["end_sec"]):
                interval_idx += 1
            if interval_idx < len(intervals):
                start_sec = int(intervals[interval_idx]["start_sec"])
                end_sec = int(intervals[interval_idx]["end_sec"])
                if start_sec <= sec <= end_sec:
                    observed_in_sessions += 1
                    if sec not in boundary_seconds:
                        n = sec_counts.get(sec, 0)
                        if n == 1:
                            seconds_with_one += 1
                        elif n >= 2:
                            seconds_with_two_plus += 1
                else:
                    observed_outside_sessions += 1
            else:
                observed_outside_sessions += 1

        missing_day = bool(observed_in_sessions == 0 and expected_seconds > 0)
        if missing_day:
            missing_days.append(day.isoformat())

        day_missing_segments: list[dict[str, Any]] = []
        day_missing_seconds = 0
        day_missing_half_seconds = 0
        day_boundary_missing = 0
        day_segments_total = 0
        max_gap_day = 0
        observed_segments_day = 0
        day_gap_buckets: dict[str, int] = {label: 0 for label, _, _ in gap_bucket_defs}
        day_gap_buckets_by_session: dict[str, dict[str, int]] = {}
        day_gap_count_gt_30 = 0

        def record_gap(*, sess: str, gap_start: int, gap_end: int) -> None:
            nonlocal day_segments_total, max_gap_day, day_gap_count_gt_30, gap_count_gt_30_total
            if gap_end < gap_start:
                return
            duration = int(gap_end - gap_start + 1)
            max_gap_day = max(max_gap_day, duration)
            if duration >= gap_threshold_s:
                day_segments_total += 1
                bucket_label = None
                for label, start_s, end_s in gap_bucket_defs:
                    if duration >= int(start_s) and (end_s is None or duration <= int(end_s)):
                        bucket_label = label
                        break
                if bucket_label:
                    day_gap_buckets[bucket_label] = int(day_gap_buckets.get(bucket_label, 0)) + 1
                    gap_bucket_totals[bucket_label] = int(gap_bucket_totals.get(bucket_label, 0)) + 1
                    if sess:
                        sess_key = str(sess)
                        if sess_key not in day_gap_buckets_by_session:
                            day_gap_buckets_by_session[sess_key] = {label: 0 for label, _, _ in gap_bucket_defs}
                        if sess_key not in gap_bucket_totals_by_session:
                            gap_bucket_totals_by_session[sess_key] = {label: 0 for label, _, _ in gap_bucket_defs}
                        day_gap_buckets_by_session[sess_key][bucket_label] += 1
                        gap_bucket_totals_by_session[sess_key][bucket_label] += 1
                if duration > 30:
                    day_gap_count_gt_30 += 1
                    gap_count_gt_30_total += 1
                if len(day_missing_segments) < max_segments_per_day:
                    day_missing_segments.append(
                        {
                            "session": sess,
                            "start_ts": _sec_to_iso(gap_start),
                            "end_ts": _sec_to_iso(gap_end),
                            "duration_s": duration,
                            "tqsdk_status": "unchecked",
                        }
                    )

        for it in intervals:
            sess = str(it.get("session") or "")
            start_sec = int(it["start_sec"])
            end_sec = int(it["end_sec"])
            if sec_counts.get(start_sec, 0) == 0:
                day_boundary_missing += 1
            if sec_counts.get(end_sec, 0) == 0:
                day_boundary_missing += 1
            eff_start = start_sec + 1
            eff_end = end_sec - 1
            if eff_end < eff_start:
                continue
            left = bisect_left(secs_sorted, eff_start)
            right = bisect_right(secs_sorted, eff_end)
            secs_eff = secs_sorted[left:right]
            day_missing_seconds += int((eff_end - eff_start + 1) - len(secs_eff))
            if day_cadence == "fixed_0p5s":
                secs_two_plus = sum(1 for s in secs_eff if sec_counts.get(s, 0) >= 2)
                day_missing_half_seconds += int((eff_end - eff_start + 1) - secs_two_plus)

            if not secs_eff:
                record_gap(sess=sess, gap_start=eff_start, gap_end=eff_end)
                continue

            observed_segments_session = 1
            if secs_eff[0] > eff_start:
                record_gap(sess=sess, gap_start=eff_start, gap_end=secs_eff[0] - 1)
            for a, b in zip(secs_eff, secs_eff[1:]):
                if b - a > 1:
                    gap_len = int(b - a - 1)
                    if gap_len >= gap_threshold_s:
                        observed_segments_session += 1
                    record_gap(sess=sess, gap_start=a + 1, gap_end=b - 1)
            if secs_eff[-1] < eff_end:
                record_gap(sess=sess, gap_start=secs_eff[-1] + 1, gap_end=eff_end)
            observed_segments_day += int(observed_segments_session)

        if tqsdk_api is not None and tqsdk_check and day_segments_total > 0 and tqsdk_checked_days < tqsdk_check_max_days:
            try:
                df = tqsdk_api.get_tick_data_series(
                    symbol=contract or ds, start_dt=day, end_dt=day + timedelta(days=1)
                )
                if df is None or df.empty:
                    for seg in day_missing_segments[:tqsdk_check_max_segments]:
                        seg["tqsdk_status"] = "provider_missing"
                    checked_n = int(min(len(day_missing_segments), tqsdk_check_max_segments))
                    tqsdk_missing_segments += checked_n
                    tqsdk_checked_segments += checked_n
                else:
                    try:
                        mask = l5_mask_df(df)
                    except Exception:
                        mask = None
                    if mask is not None:
                        df = df.loc[mask].copy()
                    try:
                        import pandas as pd

                        tick_ns_tq = (
                            pd.to_numeric(df.get("datetime"), errors="coerce")
                            .dropna()
                            .astype("int64")
                            .tolist()
                        )
                    except Exception:
                        tick_ns_tq = [int(x) for x in (df.get("datetime") if df is not None else []) if x is not None]
                    tick_ns_tq.sort()
                    from bisect import bisect_left

                    checked = 0
                    for seg in day_missing_segments:
                        if checked >= tqsdk_check_max_segments:
                            break
                        start_sec = int(datetime.fromisoformat(str(seg["start_ts"])).timestamp())
                        end_sec = int(datetime.fromisoformat(str(seg["end_ts"])).timestamp())
                        start_ns = int(start_sec * 1_000_000_000)
                        end_ns = int((end_sec + 1) * 1_000_000_000 - 1)
                        j = bisect_left(tick_ns_tq, start_ns)
                        has_data = bool(j < len(tick_ns_tq) and tick_ns_tq[j] <= end_ns)
                        seg["tqsdk_status"] = "provider_has_data" if has_data else "provider_missing"
                        checked += 1
                        if has_data:
                            tqsdk_present_segments += 1
                        else:
                            tqsdk_missing_segments += 1
                    tqsdk_checked_segments += int(checked)
                tqsdk_checked_days += 1
            except Exception as e:
                tqsdk_check_errors += 1
                log.warning(
                    "main_l5_validate.tqsdk_check_failed",
                    trading_day=day.isoformat(),
                    error=str(e),
                )

        manifest_rows = None
        manifest_inference = "unknown"
        if row_counts:
            manifest_rows = row_counts.get(day.isoformat())
            if manifest_rows == 0:
                manifest_inference = "provider_empty_day"
            elif manifest_rows is not None:
                manifest_inference = "unknown_partial"

        day_out = {
            "trading_day": day.isoformat(),
            "underlying_contract": contract,
            "cadence_mode": day_cadence,
            "two_plus_ratio": two_plus_ratio_day,
            "expected_seconds": int(expected_seconds),
            "expected_seconds_strict": int(expected_seconds_strict),
            "observed_seconds": int(observed_in_sessions),
            "seconds_with_one_tick": int(seconds_with_one),
            "seconds_with_two_plus": int(seconds_with_two_plus),
            "ticks_outside_sessions_seconds": int(observed_outside_sessions),
            "last_tick_ts": (_sec_to_iso(last_tick_sec) if last_tick_sec is not None else None),
            "session_end_ts": (_sec_to_iso(session_end_sec) if session_end_sec is not None else None),
            "session_end_lag_s": session_end_lag_s,
            "missing_seconds": int(day_missing_seconds),
            "missing_half_seconds": int(day_missing_half_seconds),
            "boundary_missing_seconds": int(day_boundary_missing),
            "missing_segments_total": int(day_segments_total),
            "missing_segments": day_missing_segments,
            "missing_segments_truncated": bool(day_segments_total > len(day_missing_segments)),
            "missing_seconds_ratio": (float(day_missing_seconds) / float(expected_seconds) if expected_seconds > 0 else 0.0),
            "gap_buckets": day_gap_buckets,
            "gap_buckets_by_session": day_gap_buckets_by_session,
            "gap_count_gt_30s": int(day_gap_count_gt_30),
            "manifest_rows": manifest_rows,
            "tqsdk_inference": manifest_inference,
        }
        day_out["_has_gap"] = bool(day_segments_total > 0)
        day_out["_has_half"] = bool(day_missing_half_seconds > 0 and day_cadence == "fixed_0p5s")
        day_out["_has_outside"] = bool(observed_outside_sessions > 0)
        day_out["_has_missing_day"] = bool(missing_day)
        days_all.append(day_out)

        missing_segments_total += int(day_segments_total)
        missing_seconds_total += int(day_missing_seconds)
        missing_half_seconds_total += int(day_missing_half_seconds)
        boundary_missing_seconds_total += int(day_boundary_missing)
        ticks_outside_sessions_total += int(observed_outside_sessions)
        seconds_with_ticks_total += int(seconds_with_ticks_day)
        seconds_with_one_tick_total += int(seconds_with_one_day)
        seconds_with_two_plus_total += int(seconds_with_two_plus_day)
        max_gap_s = max(max_gap_s, int(max_gap_day))
        expected_seconds_total += int(expected_seconds)
        expected_seconds_strict_total += int(expected_seconds_strict)
        total_segments_day = int(observed_segments_day + day_segments_total)
        total_segments_total += int(total_segments_day)
        summary_rows.append(
            MainL5ValidateSummaryRow(
                symbol=ds,
                trading_day=day.isoformat(),
                cadence_mode=day_cadence,
                expected_seconds=int(expected_seconds),
                expected_seconds_strict=int(expected_seconds_strict),
                seconds_with_ticks=int(seconds_with_ticks_day),
                seconds_with_two_plus=int(seconds_with_two_plus_day),
                two_plus_ratio=float(two_plus_ratio_day),
                observed_segments=int(observed_segments_day),
                total_segments=int(total_segments_day),
                missing_day=int(missing_day),
                missing_segments=int(day_segments_total),
                missing_seconds=int(day_missing_seconds),
                missing_seconds_ratio=(
                    float(day_missing_seconds) / float(expected_seconds) if expected_seconds > 0 else 0.0
                ),
                gap_bucket_2_5=int(day_gap_buckets.get("2_5") or 0),
                gap_bucket_6_15=int(day_gap_buckets.get("6_15") or 0),
                gap_bucket_16_30=int(day_gap_buckets.get("16_30") or 0),
                gap_bucket_gt_30=int(day_gap_buckets.get("gt_30") or 0),
                gap_count_gt_30=int(day_gap_count_gt_30),
                missing_half_seconds=int(day_missing_half_seconds if day_cadence == "fixed_0p5s" else 0),
                last_tick_ts=(
                    datetime.fromtimestamp(float(last_tick_sec), tz=timezone.utc) if last_tick_sec is not None else None
                ),
                session_end_lag_s=int(session_end_lag_s) if session_end_lag_s is not None else None,
                max_gap_s=int(max_gap_day),
                gap_threshold_s=float(gap_threshold_s),
                schedule_hash=str(schedule_hash),
            )
        )

        for seg in day_missing_segments:
            try:
                gap_rows.append(
                    MainL5ValidateGapRow(
                        symbol=ds,
                        trading_day=day.isoformat(),
                        session=str(seg.get("session") or ""),
                        start_ts=datetime.fromisoformat(str(seg.get("start_ts"))),
                        end_ts=datetime.fromisoformat(str(seg.get("end_ts"))),
                        duration_s=int(seg.get("duration_s") or 0),
                        tqsdk_status=str(seg.get("tqsdk_status") or ""),
                        schedule_hash=str(schedule_hash),
                    )
                )
            except Exception:
                continue

        if idx == 1 or idx == len(schedule_days) or idx % progress_every_n == 0 or (time.time() - last_progress_ts) >= progress_every_s:
            log.info(
                "main_l5_validate.progress",
                day_index=int(idx),
                days_total=int(len(schedule_days)),
                last_day=day.isoformat(),
                missing_segments=int(missing_segments_total),
                missing_half_seconds=int(missing_half_seconds_total),
            )
            last_progress_ts = time.time()

    try:
        upsert_main_l5_validate_summary_rows(cfg=cfg, rows=summary_rows)
    except Exception as e:
        log.warning("main_l5_validate.summary_persist_failed", error=str(e))
    try:
        clear_main_l5_validate_gap_rows(
            cfg=cfg,
            symbol=ds,
            start_day=schedule_start,
            end_day=schedule_end,
        )
    except Exception as e:
        log.warning("main_l5_validate.gaps_clear_failed", error=str(e))
    try:
        insert_main_l5_validate_gap_rows(cfg=cfg, rows=gap_rows)
    except Exception as e:
        log.warning("main_l5_validate.gaps_persist_failed", error=str(e))
    try:
        upsert_main_l5_tick_gaps_from_validate_summary(cfg=cfg, summary_rows=summary_rows)
    except Exception as e:
        log.warning("main_l5_validate.tick_gaps_persist_failed", error=str(e))

    if tqsdk_api is not None:
        try:
            tqsdk_api.close()
        except Exception:
            pass

    two_plus_ratio = 0.0
    if seconds_with_ticks_total > 0:
        two_plus_ratio = float(seconds_with_two_plus_total) / float(seconds_with_ticks_total)
    cadence_mode = "hybrid"

    days_with_issues = [
        d
        for d in days_all
        if d.get("_has_gap") or d.get("_has_half") or d.get("_has_outside") or d.get("_has_missing_day")
    ]
    for d in days_with_issues:
        for key in ("_has_gap", "_has_half", "_has_outside", "_has_missing_day"):
            d.pop(key, None)

    ok = bool(
        missing_segments_total == 0
        and ticks_outside_sessions_total == 0
        and len(missing_days) == 0
        and missing_half_seconds_total == 0
    )
    issues_total = int(len(days_with_issues))
    issues_truncated = bool(issues_total > report_max_days)
    if issues_truncated:
        days_with_issues = days_with_issues[:report_max_days]
    lag_p95 = None
    lag_max = None
    if session_end_lags:
        sorted_lags = sorted(session_end_lags)
        lag_max = int(sorted_lags[-1])
        idx = int(0.95 * (len(sorted_lags) - 1))
        lag_p95 = int(sorted_lags[max(0, idx)])
    report = {
        "ok": ok,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "exchange": ex,
        "variety": var,
        "derived_symbol": ds,
        "schedule_hash": schedule_hash,
        "schedule_start": (schedule_start.isoformat() if schedule_start else None),
        "schedule_end": (schedule_end.isoformat() if schedule_end else None),
        "checked_days": int(len(schedule_days)),
        "missing_days": int(len(missing_days)),
        "missing_days_sample": missing_days[:20],
        "missing_segments_total": int(missing_segments_total),
        "missing_seconds_total": int(missing_seconds_total),
        "missing_half_seconds_total": int(missing_half_seconds_total),
        "boundary_missing_seconds_total": int(boundary_missing_seconds_total),
        "ticks_outside_sessions_seconds_total": int(ticks_outside_sessions_total),
        "max_gap_s": int(max_gap_s),
        "days_with_issues": int(len(days_with_issues)),
        "issues_total": int(issues_total),
        "issues_truncated": bool(issues_truncated),
        "report_max_days": int(report_max_days),
        "gap_threshold_s": float(gap_threshold_s),
        "cadence_mode": cadence_mode,
        "seconds_with_ticks_total": int(seconds_with_ticks_total),
        "seconds_with_one_tick_total": int(seconds_with_one_tick_total),
        "seconds_with_two_plus_total": int(seconds_with_two_plus_total),
        "expected_seconds_total": int(expected_seconds_total),
        "expected_seconds_strict_total": int(expected_seconds_strict_total),
        "missing_seconds_ratio": (float(missing_seconds_total) / float(expected_seconds_total) if expected_seconds_total > 0 else 0.0),
        "gap_buckets_total": gap_bucket_totals,
        "gap_buckets_by_session_total": gap_bucket_totals_by_session,
        "gap_count_gt_30s": int(gap_count_gt_30_total),
        "total_segments": int(total_segments_total),
        "two_plus_ratio": float(two_plus_ratio),
        "strict_ratio": float(strict_ratio),
        "timeliness": {
            "p95_lag_s": lag_p95,
            "max_lag_s": lag_max,
            "days_with_lag": int(len(session_end_lags)),
        },
        "sessions": {
            "count": int(len(sessions)),
            "source": "cache",
            "raw_trading_time": sessions_payload.get("raw_trading_time") if isinstance(sessions_payload, dict) else None,
        },
        "holiday_night_skipped_days": int(len(holiday_gap_days)),
        "holiday_night_skipped_sample": [d.isoformat() for d in sorted(holiday_gap_days)[:20]],
        "exchange_events_used": exchange_events_used,
        "event_override_days": int(len(event_override_days)),
        "event_override_days_sample": [d.isoformat() for d in sorted(event_override_days)[:20]],
        "tqsdk_check": {
            "enabled": bool(tqsdk_check),
            "error": tqsdk_error or None,
            "max_days": int(tqsdk_check_max_days),
            "max_segments": int(tqsdk_check_max_segments),
            "checked_days": int(tqsdk_checked_days),
            "checked_segments": int(tqsdk_checked_segments),
            "provider_missing_segments": int(tqsdk_missing_segments),
            "provider_has_data_segments": int(tqsdk_present_segments),
            "errors": int(tqsdk_check_errors),
        },
        "manifest": {
            "path": (manifest.get("_path") if isinstance(manifest, dict) else None),
            "run_id": (manifest.get("run_id") if isinstance(manifest, dict) else None),
            "created_at": (manifest.get("created_at") if isinstance(manifest, dict) else None),
        },
        "issues": days_with_issues,
    }

    log.info(
        "main_l5_validate.done",
        exchange=ex,
        var=var,
        derived_symbol=ds,
        checked_days=int(len(schedule_days)),
        missing_days=int(len(missing_days)),
        missing_segments=int(missing_segments_total),
        missing_half_seconds=int(missing_half_seconds_total),
        max_gap_s=int(max_gap_s),
        cadence_mode=cadence_mode,
        two_plus_ratio=float(two_plus_ratio),
    )

    out_path = _write_report(runs_dir=Path(runs_dir), report=report, exchange=ex, variety=var, derived_symbol=ds)
    return report, out_path


def _write_report(*, runs_dir: Path, report: dict[str, Any], exchange: str, variety: str, derived_symbol: str) -> Path | None:
    try:
        out_dir = _report_dir(runs_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        sym = _symbol_slug(derived_symbol)
        ex = str(exchange).upper().strip()
        v = str(variety).lower().strip()
        out_path = out_dir / f"main_l5_validate_exchange={ex}_var={v}_symbol={sym}_{run_id}.json"
        write_json_atomic(out_path, report)
        latest = _latest_path(runs_dir=runs_dir, exchange=ex, variety=v, derived_symbol=derived_symbol)
        write_json_atomic(latest, report)
        return out_path
    except Exception:
        return None
