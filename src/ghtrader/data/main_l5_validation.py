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
from ghtrader.data.main_l5_validation_builders import build_day_validation_artifacts
from ghtrader.data.main_l5_validation_stats import compute_day_gap_stats
from ghtrader.data.main_l5_validation_tqsdk import verify_missing_segments_with_tqsdk
from ghtrader.data.trading_sessions import read_trading_sessions_cache
from ghtrader.data.trading_calendar import get_holidays
from ghtrader.questdb.client import make_questdb_query_config_from_env
from ghtrader.questdb.main_l5_validate import (
    MainL5ValidateGapRow,
    MainL5ValidateSummaryRow,
    assert_main_l5_validate_tables_ready,
    clear_main_l5_validate_gap_rows,
    ensure_main_l5_tick_gaps_table,
    ensure_main_l5_validate_gaps_table,
    ensure_main_l5_validate_summary_table,
    insert_main_l5_validate_gap_rows,
    upsert_main_l5_tick_gaps_from_validate_summary,
    upsert_main_l5_validate_summary_rows,
)
from ghtrader.questdb.main_l5_validation_queries import (
    fetch_day_second_stats as _fetch_day_second_stats,
    fetch_per_second_counts as _fetch_per_second_counts,
    get_last_validated_day,
)
from ghtrader.questdb.main_schedule import fetch_schedule
from ghtrader.questdb.queries import query_symbol_day_bounds
from ghtrader.tq.runtime import create_tq_data_api, trading_day_from_ts_ns
from ghtrader.util.json_io import read_json, write_json_atomic

log = structlog.get_logger()


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _env_float(name: str) -> float | None:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


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

    def _write_noop_report(
        *,
        reason: str,
        schedule_hash: str,
        schedule_start: date | None,
        schedule_end: date | None,
        last_validated_day: date | None,
    ) -> tuple[dict[str, Any], Path | None]:
        report = {
            "ok": True,
            "state": "noop",
            "reason": str(reason or "up_to_date"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "exchange": ex,
            "variety": var,
            "derived_symbol": ds,
            "schedule_hash": str(schedule_hash or ""),
            "schedule_start": (schedule_start.isoformat() if schedule_start else None),
            "schedule_end": (schedule_end.isoformat() if schedule_end else None),
            "last_validated_day": (last_validated_day.isoformat() if last_validated_day else None),
            "checked_days": 0,
            "missing_days": 0,
            "missing_days_sample": [],
            "missing_segments_total": 0,
            "missing_seconds_total": 0,
            "missing_half_seconds_total": 0,
            "boundary_missing_seconds_total": 0,
            "ticks_outside_sessions_seconds_total": 0,
            "max_gap_s": 0,
            "days_with_issues": 0,
            "issues_total": 0,
            "issues_truncated": False,
            "report_max_days": 0,
            "gap_threshold_s": float(gap_threshold_s or 0.0) if gap_threshold_s is not None else None,
            "gap_threshold_s_by_session": {},
            "cadence_mode": "hybrid",
            "seconds_with_ticks_total": 0,
            "seconds_with_one_tick_total": 0,
            "seconds_with_two_plus_total": 0,
            "expected_seconds_total": 0,
            "expected_seconds_strict_total": 0,
            "expected_seconds_strict_fixed_total": 0,
            "missing_seconds_ratio": 0.0,
            "gap_buckets_total": {"2_5": 0, "6_15": 0, "16_30": 0, "gt_30": 0},
            "gap_buckets_by_session_total": {},
            "gap_count_gt_30s": 0,
            "total_segments": 0,
            "two_plus_ratio": 0.0,
            "strict_ratio": float(strict_ratio or 0.0) if strict_ratio is not None else None,
            "missing_half_seconds_ratio": 0.0,
            "missing_half_seconds_info_ratio": None,
            "missing_half_seconds_block_ratio": None,
            "missing_half_seconds_state": "ok",
            "timeliness": {"p95_lag_s": None, "max_lag_s": None, "days_with_lag": 0},
            "sessions": {"count": 0, "source": "cache", "raw_trading_time": None},
            "holiday_night_skipped_days": 0,
            "holiday_night_skipped_sample": [],
            "exchange_events_used": [],
            "exchange_events_used_total": 0,
            "exchange_events_used_truncated": False,
            "event_override_days": 0,
            "event_override_days_sample": [],
            "tqsdk_check": {
                "enabled": bool(tqsdk_check) if tqsdk_check is not None else None,
                "error": None,
                "max_days": 0,
                "max_segments": 0,
                "checked_days": 0,
                "checked_segments": 0,
                "provider_missing_segments": 0,
                "provider_has_data_segments": 0,
                "errors": 0,
            },
            "manifest": {"path": None, "run_id": None, "created_at": None},
            "persist": {
                "summary_rows_upserted": 0,
                "gap_rows_deleted": 0,
                "gap_rows_inserted": 0,
                "tick_gap_rows_upserted": 0,
            },
            "issues": [],
        }
        out_path = _write_report(runs_dir=Path(runs_dir), report=report, exchange=ex, variety=var, derived_symbol=ds)
        return report, out_path

    schedule = fetch_schedule(cfg=cfg, exchange=ex, variety=var, start_day=start_day, end_day=end_day, connect_timeout_s=2)
    if schedule.empty:
        raise FileNotFoundError(f"No schedule rows found for {ex}.{var} (build main-schedule first).")

    schedule = schedule.dropna(subset=["trading_day", "main_contract"]).sort_values("trading_day").reset_index(drop=True)
    raw_hashes = schedule["schedule_hash"].tolist() if "schedule_hash" in schedule.columns else []
    schedule_hashes = sorted({str(h).strip() for h in raw_hashes if str(h).strip()})
    if len(schedule_hashes) > 1:
        raise RuntimeError(f"main_l5_validate mixed schedule_hash in schedule source: {schedule_hashes}")
    schedule_hash = str(schedule_hashes[0]) if schedule_hashes else ""

    if incremental and start_day is None:
        last_val = get_last_validated_day(cfg=cfg, symbol=ds, schedule_hash=(schedule_hash or None))
        if last_val:
            schedule = schedule[schedule["trading_day"] > last_val].reset_index(drop=True)
            log.info(
                "main_l5_validate.incremental",
                last_validated=last_val.isoformat(),
                schedule_hash=(schedule_hash or None),
                remaining_days=int(len(schedule)),
            )
            if schedule.empty:
                log.info(
                    "main_l5_validate.noop",
                    reason="up_to_date",
                    last_validated=last_val.isoformat(),
                    schedule_hash=(schedule_hash or None),
                )
                return _write_noop_report(
                    reason="up_to_date",
                    schedule_hash=schedule_hash,
                    schedule_start=None,
                    schedule_end=last_val,
                    last_validated_day=last_val,
                )

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
            if incremental:
                log.info(
                    "main_l5_validate.noop",
                    reason="up_to_date_after_alignment",
                    schedule_hash=(schedule_hash or None),
                )
                return _write_noop_report(
                    reason="up_to_date_after_alignment",
                    schedule_hash=schedule_hash,
                    schedule_start=aligned_start,
                    schedule_end=None,
                    last_validated_day=None,
                )
            raise ValueError("No schedule days available after alignment")

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

    ensure_main_l5_validate_summary_table(cfg=cfg, fail_fast=True)
    ensure_main_l5_validate_gaps_table(cfg=cfg, fail_fast=True)
    ensure_main_l5_tick_gaps_table(cfg=cfg, fail_fast=True)
    try:
        assert_main_l5_validate_tables_ready(cfg=cfg)
    except Exception as e:
        raise RuntimeError(f"main_l5_validate preflight failed: {e}") from e

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
    report_include_segments = _env_bool("GHTRADER_L5_VALIDATE_REPORT_INCLUDE_SEGMENTS", False)
    try:
        report_segments_sample = int(os.environ.get("GHTRADER_L5_VALIDATE_REPORT_SEGMENTS_SAMPLE", "5") or "5")
    except Exception:
        report_segments_sample = 5
    try:
        report_max_events = int(os.environ.get("GHTRADER_L5_VALIDATE_REPORT_MAX_EVENTS", "200") or "200")
    except Exception:
        report_max_events = 200
    gap_threshold_s_by_session: dict[str, float] = {}
    if gap_threshold_s is None:
        var_key = re.sub(r"[^A-Za-z0-9]+", "_", var).strip("_").upper()
        candidates = []
        if var_key:
            candidates.append(f"GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S_{var_key}")
        candidates.append("GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S")
        for key in candidates:
            val = _env_float(key)
            if val is not None:
                gap_threshold_s = float(val)
                break
        if gap_threshold_s is None:
            gap_threshold_s = 5.0

        for sess in ("day", "night"):
            sess_key = sess.upper()
            sess_candidates = []
            if var_key:
                sess_candidates.append(f"GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S_{var_key}_{sess_key}")
            sess_candidates.append(f"GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S_{sess_key}")
            for key in sess_candidates:
                sval = _env_float(key)
                if sval is not None:
                    gap_threshold_s_by_session[sess] = float(sval)
                    break
    if strict_ratio is None:
        try:
            strict_ratio = float(os.environ.get("GHTRADER_L5_VALIDATE_STRICT_RATIO", "0.8") or "0.8")
        except Exception:
            strict_ratio = 0.8
    half_info_ratio = _env_float("GHTRADER_L5_VALIDATE_MISSING_HALF_INFO_RATIO")
    half_block_ratio = _env_float("GHTRADER_L5_VALIDATE_MISSING_HALF_BLOCK_RATIO")
    if half_info_ratio is None:
        half_info_ratio = 0.02
    if half_block_ratio is None:
        half_block_ratio = 0.10
    tqsdk_check_max_days = max(0, int(tqsdk_check_max_days))
    tqsdk_check_max_segments = max(0, int(tqsdk_check_max_segments))
    max_segments_per_day = max(10, int(max_segments_per_day))
    report_max_days = max(1, int(report_max_days))
    report_segments_sample = max(0, int(report_segments_sample))
    report_max_events = max(1, int(report_max_events))
    gap_threshold_s = max(0.5, float(gap_threshold_s))
    for sess_key, sval in list(gap_threshold_s_by_session.items()):
        try:
            gap_threshold_s_by_session[sess_key] = max(0.5, float(sval))
        except Exception:
            gap_threshold_s_by_session.pop(sess_key, None)
    strict_ratio = max(0.0, min(1.0, float(strict_ratio)))
    half_info_ratio = max(0.0, min(1.0, float(half_info_ratio)))
    half_block_ratio = max(float(half_info_ratio), min(1.0, float(half_block_ratio)))

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
            tqsdk_api = create_tq_data_api(disable_print=True)
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
        gap_threshold_s_by_session=gap_threshold_s_by_session,
        strict_ratio=float(strict_ratio),
        missing_half_info_ratio=float(half_info_ratio),
        missing_half_block_ratio=float(half_block_ratio),
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
    expected_seconds_strict_fixed_total = 0
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

        sec_counts = (
            _fetch_per_second_counts(cfg=cfg, symbol=ds, trading_day=day, l5_only=True)
            if seconds_with_ticks_day > 0
            else {}
        )
        day_calc = compute_day_gap_stats(
            intervals=intervals,
            sec_counts=sec_counts,
            day_cadence=day_cadence,
            gap_threshold_s=float(gap_threshold_s),
            gap_threshold_s_by_session=gap_threshold_s_by_session,
            gap_bucket_defs=gap_bucket_defs,
            max_segments_per_day=max_segments_per_day,
            sec_to_iso=_sec_to_iso,
        )

        session_end_sec = day_calc.session_end_sec
        last_tick_sec = day_calc.last_tick_sec
        session_end_lag_s = day_calc.session_end_lag_s
        if session_end_lag_s is not None:
            session_end_lags.append(session_end_lag_s)

        expected_seconds = int(day_calc.expected_seconds)
        expected_seconds_strict = int(day_calc.expected_seconds_strict)
        observed_in_sessions = int(day_calc.observed_in_sessions)
        observed_outside_sessions = int(day_calc.observed_outside_sessions)
        seconds_with_one = int(day_calc.seconds_with_one)
        seconds_with_two_plus = int(day_calc.seconds_with_two_plus)
        missing_day = bool(day_calc.missing_day)
        if missing_day:
            missing_days.append(day.isoformat())

        day_missing_segments = list(day_calc.day_missing_segments)
        day_missing_seconds = int(day_calc.day_missing_seconds)
        day_missing_half_seconds = int(day_calc.day_missing_half_seconds)
        day_boundary_missing = int(day_calc.day_boundary_missing)
        day_segments_total = int(day_calc.day_segments_total)
        max_gap_day = int(day_calc.max_gap_day)
        observed_segments_day = int(day_calc.observed_segments_day)
        day_gap_buckets = dict(day_calc.day_gap_buckets)
        day_gap_buckets_by_session = dict(day_calc.day_gap_buckets_by_session)
        day_gap_count_gt_30 = int(day_calc.day_gap_count_gt_30)

        for label, delta in day_calc.gap_bucket_totals_delta.items():
            gap_bucket_totals[label] = int(gap_bucket_totals.get(label, 0)) + int(delta)
        for sess_key, buckets in day_calc.gap_bucket_totals_by_session_delta.items():
            if sess_key not in gap_bucket_totals_by_session:
                gap_bucket_totals_by_session[sess_key] = {label: 0 for label, _, _ in gap_bucket_defs}
            for label, delta in buckets.items():
                gap_bucket_totals_by_session[sess_key][label] = (
                    int(gap_bucket_totals_by_session[sess_key].get(label, 0)) + int(delta)
                )
        gap_count_gt_30_total += int(day_calc.gap_count_gt_30_total_delta)

        if tqsdk_api is not None and tqsdk_check and day_segments_total > 0 and tqsdk_checked_days < tqsdk_check_max_days:
            tqsdk_out = verify_missing_segments_with_tqsdk(
                tqsdk_api=tqsdk_api,
                symbol=(contract or ds),
                day=day,
                day_missing_segments=day_missing_segments,
                tqsdk_check_max_segments=tqsdk_check_max_segments,
            )
            tqsdk_checked_days += int(tqsdk_out.get("checked_days") or 0)
            tqsdk_checked_segments += int(tqsdk_out.get("checked_segments") or 0)
            tqsdk_missing_segments += int(tqsdk_out.get("provider_missing_segments") or 0)
            tqsdk_present_segments += int(tqsdk_out.get("provider_has_data_segments") or 0)
            err_count = int(tqsdk_out.get("errors") or 0)
            if err_count > 0:
                tqsdk_check_errors += int(err_count)
                log.warning(
                    "main_l5_validate.tqsdk_check_failed",
                    trading_day=day.isoformat(),
                    error=str(tqsdk_out.get("error") or "unknown"),
                )

        day_out, summary_row, day_gap_rows = build_day_validation_artifacts(
            symbol=ds,
            trading_day=day,
            underlying_contract=contract,
            cadence_mode=day_cadence,
            two_plus_ratio=two_plus_ratio_day,
            expected_seconds=expected_seconds,
            expected_seconds_strict=expected_seconds_strict,
            observed_seconds_in_sessions=observed_in_sessions,
            seconds_with_one_tick=seconds_with_one,
            seconds_with_two_plus=seconds_with_two_plus,
            summary_seconds_with_ticks=seconds_with_ticks_day,
            summary_seconds_with_two_plus=seconds_with_two_plus_day,
            ticks_outside_sessions_seconds=observed_outside_sessions,
            last_tick_sec=last_tick_sec,
            session_end_sec=session_end_sec,
            session_end_lag_s=session_end_lag_s,
            missing_seconds=day_missing_seconds,
            missing_half_seconds=day_missing_half_seconds,
            boundary_missing_seconds=day_boundary_missing,
            missing_segments_total=day_segments_total,
            missing_segments=day_missing_segments,
            gap_buckets=day_gap_buckets,
            gap_buckets_by_session=day_gap_buckets_by_session,
            gap_count_gt_30s=day_gap_count_gt_30,
            missing_day=missing_day,
            observed_segments=observed_segments_day,
            max_gap_s=max_gap_day,
            gap_threshold_s=gap_threshold_s,
            schedule_hash=schedule_hash,
            row_counts=row_counts,
            sec_to_iso=_sec_to_iso,
        )
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
        if day_cadence == "fixed_0p5s":
            expected_seconds_strict_fixed_total += int(expected_seconds_strict)
        total_segments_day = int(summary_row.total_segments)
        total_segments_total += int(total_segments_day)
        summary_rows.append(summary_row)
        gap_rows.extend(day_gap_rows)

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

    persisted_summary_rows = 0
    persisted_gap_rows_deleted = 0
    persisted_gap_rows_inserted = 0
    persisted_tick_gap_rows = 0
    try:
        try:
            persisted_summary_rows = int(upsert_main_l5_validate_summary_rows(cfg=cfg, rows=summary_rows))
        except Exception as e:
            raise RuntimeError(f"main_l5_validate summary persist failed: {e}") from e
        try:
            persisted_gap_rows_deleted = int(
                clear_main_l5_validate_gap_rows(
                    cfg=cfg,
                    symbol=ds,
                    start_day=schedule_start,
                    end_day=schedule_end,
                )
            )
        except Exception as e:
            raise RuntimeError(f"main_l5_validate gaps clear failed: {e}") from e
        try:
            persisted_gap_rows_inserted = int(insert_main_l5_validate_gap_rows(cfg=cfg, rows=gap_rows))
        except Exception as e:
            raise RuntimeError(f"main_l5_validate gaps persist failed: {e}") from e
        try:
            persisted_tick_gap_rows = int(upsert_main_l5_tick_gaps_from_validate_summary(cfg=cfg, summary_rows=summary_rows))
        except Exception as e:
            raise RuntimeError(f"main_l5_validate tick_gaps persist failed: {e}") from e
    finally:
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
    days_with_issues.sort(
        key=lambda d: (
            int(d.get("missing_segments_total") or 0),
            int(d.get("missing_half_seconds") or 0),
            int(d.get("ticks_outside_sessions_seconds") or 0),
            int(d.get("max_gap_s") or 0),
            str(d.get("trading_day") or ""),
        ),
        reverse=True,
    )
    for d in days_with_issues:
        for key in ("_has_gap", "_has_half", "_has_outside", "_has_missing_day"):
            d.pop(key, None)

    missing_half_seconds_ratio = (
        float(missing_half_seconds_total) / float(expected_seconds_strict_fixed_total)
        if expected_seconds_strict_fixed_total > 0
        else 0.0
    )
    if missing_half_seconds_total > 0 and expected_seconds_strict_fixed_total <= 0:
        missing_half_seconds_state = "error"
    elif missing_half_seconds_ratio >= float(half_block_ratio) and missing_half_seconds_total > 0:
        missing_half_seconds_state = "error"
    elif missing_half_seconds_ratio >= float(half_info_ratio) and missing_half_seconds_total > 0:
        missing_half_seconds_state = "info"
    else:
        missing_half_seconds_state = "ok"

    ok = bool(
        missing_segments_total == 0
        and ticks_outside_sessions_total == 0
        and len(missing_days) == 0
        and missing_half_seconds_state != "error"
    )
    state = "error" if not ok else ("warn" if missing_half_seconds_state == "info" else "ok")
    issues_total = int(len(days_with_issues))
    issues_truncated = bool(issues_total > report_max_days)
    if issues_truncated:
        days_with_issues = days_with_issues[:report_max_days]
    if not report_include_segments:
        for d in days_with_issues:
            segs = list(d.get("missing_segments") or [])
            if segs and report_segments_sample > 0:
                d["missing_segments_sample"] = segs[:report_segments_sample]
                d["missing_segments_sampled"] = int(min(report_segments_sample, len(segs)))
            else:
                d["missing_segments_sample"] = []
                d["missing_segments_sampled"] = 0
            d["missing_segments"] = []
    exchange_events_total = int(len(exchange_events_used))
    exchange_events_truncated = bool(exchange_events_total > report_max_events)
    exchange_events_sample = list(exchange_events_used[:report_max_events])
    lag_p95 = None
    lag_max = None
    if session_end_lags:
        sorted_lags = sorted(session_end_lags)
        lag_max = int(sorted_lags[-1])
        idx = int(0.95 * (len(sorted_lags) - 1))
        lag_p95 = int(sorted_lags[max(0, idx)])
    report = {
        "ok": ok,
        "state": state,
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
        "gap_threshold_s_by_session": {k: float(v) for k, v in gap_threshold_s_by_session.items()},
        "cadence_mode": cadence_mode,
        "seconds_with_ticks_total": int(seconds_with_ticks_total),
        "seconds_with_one_tick_total": int(seconds_with_one_tick_total),
        "seconds_with_two_plus_total": int(seconds_with_two_plus_total),
        "expected_seconds_total": int(expected_seconds_total),
        "expected_seconds_strict_total": int(expected_seconds_strict_total),
        "expected_seconds_strict_fixed_total": int(expected_seconds_strict_fixed_total),
        "missing_seconds_ratio": (
            float(missing_seconds_total) / float(expected_seconds_strict_total)
            if expected_seconds_strict_total > 0
            else 0.0
        ),
        "gap_buckets_total": gap_bucket_totals,
        "gap_buckets_by_session_total": gap_bucket_totals_by_session,
        "gap_count_gt_30s": int(gap_count_gt_30_total),
        "total_segments": int(total_segments_total),
        "two_plus_ratio": float(two_plus_ratio),
        "strict_ratio": float(strict_ratio),
        "missing_half_seconds_ratio": float(missing_half_seconds_ratio),
        "missing_half_seconds_info_ratio": float(half_info_ratio),
        "missing_half_seconds_block_ratio": float(half_block_ratio),
        "missing_half_seconds_state": str(missing_half_seconds_state),
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
        "exchange_events_used": exchange_events_sample,
        "exchange_events_used_total": exchange_events_total,
        "exchange_events_used_truncated": bool(exchange_events_truncated),
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
        "persist": {
            "summary_rows_upserted": int(persisted_summary_rows),
            "gap_rows_deleted": int(persisted_gap_rows_deleted),
            "gap_rows_inserted": int(persisted_gap_rows_inserted),
            "tick_gap_rows_upserted": int(persisted_tick_gap_rows),
        },
        "report_compaction": {
            "include_segments": bool(report_include_segments),
            "segments_sample": int(report_segments_sample),
            "max_days": int(report_max_days),
            "max_events": int(report_max_events),
        },
        "issues": days_with_issues,
    }

    log.info(
        "main_l5_validate.persist_done",
        summary_rows_upserted=int(persisted_summary_rows),
        gap_rows_deleted=int(persisted_gap_rows_deleted),
        gap_rows_inserted=int(persisted_gap_rows_inserted),
        tick_gap_rows_upserted=int(persisted_tick_gap_rows),
    )

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
