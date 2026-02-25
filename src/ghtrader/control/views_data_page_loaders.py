from __future__ import annotations

from pathlib import Path
from typing import Any


def load_reports_snapshot(*, runs_dir: Path) -> list[str]:
    out: list[str] = []
    try:
        reports_dir = runs_dir / "audit"
        if reports_dir.exists():
            out = sorted([p.name for p in reports_dir.glob("*.json")], reverse=True)[:50]
    except Exception:
        out = []
    return out


def load_questdb_status_snapshot() -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False}
    try:
        from ghtrader.config import (
            get_questdb_host,
            get_questdb_ilp_port,
            get_questdb_pg_port,
        )
        from ghtrader.questdb.client import questdb_reachable_pg

        host = get_questdb_host()
        pg_port = int(get_questdb_pg_port())
        ilp_port = int(get_questdb_ilp_port())
        out.update({"host": host, "pg_port": pg_port, "ilp_port": ilp_port})

        q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
        out["ok"] = bool(q.get("ok"))
        if not out["ok"] and q.get("error"):
            out["error"] = str(q.get("error"))
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
    return out


def load_coverage_snapshot(
    *,
    cfg: Any | None,
    coverage_var: str,
    coverage_symbol: str,
) -> dict[str, Any]:
    out = {"main_schedule_coverage": {}, "main_l5_coverage": {}, "coverage_error": ""}
    if cfg is None:
        out["coverage_error"] = "questdb config unavailable"
        return out
    try:
        from ghtrader.questdb.main_schedule import fetch_main_schedule_state
        from ghtrader.questdb.queries import query_symbol_day_bounds

        sched = fetch_main_schedule_state(
            cfg=cfg,
            exchange="SHFE",
            variety=coverage_var,
        )
        hashes = sched.get("schedule_hashes") or set()
        sched["schedule_hashes"] = sorted([str(h) for h in hashes if str(h).strip()])
        cov = query_symbol_day_bounds(
            cfg=cfg,
            table="ghtrader_ticks_main_l5_v2",
            symbols=[coverage_symbol],
            dataset_version="v2",
            ticks_kind="main_l5",
            l5_only=True,
        )
        out["main_schedule_coverage"] = sched
        out["main_l5_coverage"] = dict(cov.get(coverage_symbol) or {})
    except Exception as e:
        out["coverage_error"] = str(e)
    return out


def load_validation_snapshot(
    *,
    cfg: Any | None,
    runs_dir: Path,
    coverage_var: str,
    coverage_symbol: str,
) -> dict[str, Any]:
    out = {"main_l5_validation": {}, "validation_error": ""}
    if cfg is None:
        out["validation_error"] = "questdb config unavailable"
        return out
    try:
        from ghtrader.questdb.main_l5_validate import (
            fetch_latest_main_l5_validate_summary,
            fetch_main_l5_validate_overview,
            fetch_main_l5_validate_top_gap_days,
            fetch_main_l5_validate_top_lag_days,
        )
        from ghtrader.questdb.main_schedule import fetch_main_schedule_state
        from ghtrader.data.main_l5_validation import read_latest_validation_report

        schedule_hash_filter = None
        try:
            sched = fetch_main_schedule_state(cfg=cfg, exchange="SHFE", variety=coverage_var)
            hashes_raw = sched.get("schedule_hashes") or set()
            hashes = sorted([str(h) for h in hashes_raw if str(h).strip()])
            if len(hashes) == 1:
                schedule_hash_filter = hashes[0]
        except Exception:
            schedule_hash_filter = None

        overview = fetch_main_l5_validate_overview(
            cfg=cfg,
            symbol=coverage_symbol,
            schedule_hash=schedule_hash_filter,
        )
        latest_rows = fetch_latest_main_l5_validate_summary(
            cfg=cfg,
            symbol=coverage_symbol,
            schedule_hash=schedule_hash_filter,
            limit=1,
        )
        latest = latest_rows[0] if latest_rows else {}
        top_gap_days = fetch_main_l5_validate_top_gap_days(
            cfg=cfg,
            symbol=coverage_symbol,
            schedule_hash=schedule_hash_filter,
            limit=8,
        )
        top_lag_days = fetch_main_l5_validate_top_lag_days(
            cfg=cfg,
            symbol=coverage_symbol,
            schedule_hash=schedule_hash_filter,
            limit=8,
        )
        if overview.get("days_total"):
            base_status = (
                "ok"
                if (
                    overview.get("missing_days", 0) == 0
                    and overview.get("missing_segments", 0) == 0
                    and overview.get("missing_half_seconds", 0) == 0
                )
                else "warn"
            )
            out["main_l5_validation"] = {
                "status": base_status,
                "status_label": base_status,
                "state": base_status,
                "last_day": overview.get("last_day"),
                "checked_days": overview.get("days_total"),
                "missing_days": overview.get("missing_days"),
                "missing_segments_total": overview.get("missing_segments"),
                "missing_seconds_total": overview.get("missing_seconds"),
                "missing_half_seconds_total": overview.get("missing_half_seconds"),
                "expected_seconds_strict_total": overview.get("expected_seconds_strict_total"),
                "total_segments": overview.get("total_segments"),
                "max_gap_s": overview.get("max_gap_s"),
                "gap_threshold_s": overview.get("gap_threshold_s"),
                "missing_seconds_ratio": overview.get("missing_seconds_ratio"),
                "gap_buckets_total": {
                    "2_5": overview.get("gap_bucket_2_5"),
                    "6_15": overview.get("gap_bucket_6_15"),
                    "16_30": overview.get("gap_bucket_16_30"),
                    "gt_30": overview.get("gap_bucket_gt_30"),
                },
                "gap_count_gt_30s": overview.get("gap_count_gt_30"),
                "p95_lag_s": overview.get("p95_lag_s"),
                "max_lag_s": overview.get("max_lag_s"),
                "cadence_mode": latest.get("cadence_mode"),
                "two_plus_ratio": latest.get("two_plus_ratio"),
                "last_run": latest.get("updated_at"),
                "schedule_hash": (schedule_hash_filter or latest.get("schedule_hash")),
                "top_gap_days": top_gap_days,
                "top_lag_days": top_lag_days,
            }
        rep = read_latest_validation_report(
            runs_dir=runs_dir,
            exchange="SHFE",
            variety=coverage_var,
            derived_symbol=coverage_symbol,
        )
        rep_usable = isinstance(rep, dict)
        if rep_usable and schedule_hash_filter:
            rep_sh = str((rep or {}).get("schedule_hash") or "").strip()
            if rep_sh and rep_sh != str(schedule_hash_filter):
                rep_usable = False

        if not out["main_l5_validation"] and rep_usable:
            rep_state = str((rep or {}).get("state") or "").strip().lower()
            if rep_state == "noop":
                rep_status = "ok"
                rep_status_label = "noop"
            elif rep_state in {"ok", "warn", "error"}:
                rep_status = rep_state
                rep_status_label = rep_state
            else:
                rep_status = ("ok" if bool((rep or {}).get("ok")) else "warn")
                rep_status_label = rep_status
            out["main_l5_validation"] = {
                "status": rep_status,
                "status_label": rep_status_label,
                "state": (rep_state or rep_status),
                "last_run": (rep or {}).get("created_at"),
                "max_gap_s": (rep or {}).get("max_gap_s"),
                "gap_threshold_s": (rep or {}).get("gap_threshold_s"),
                "missing_seconds_ratio": (rep or {}).get("missing_seconds_ratio"),
                "schedule_hash": (rep or {}).get("schedule_hash"),
            }
        if rep_usable and (rep or {}).get("_path"):
            out["main_l5_validation"]["report_name"] = Path(str(rep.get("_path"))).name
        if rep_usable and isinstance(rep, dict):
            rep_state = str(rep.get("state") or "").strip().lower()
            if rep_state == "noop":
                out["main_l5_validation"]["status"] = "ok"
                out["main_l5_validation"]["status_label"] = "noop"
                out["main_l5_validation"]["state"] = "noop"
            elif rep_state in {"ok", "warn", "error"}:
                out["main_l5_validation"]["status"] = rep_state
                out["main_l5_validation"]["status_label"] = rep_state
                out["main_l5_validation"]["state"] = rep_state
            if rep.get("created_at"):
                out["main_l5_validation"]["last_run"] = rep.get("created_at")
            merge_if_missing = (
                "checked_days",
                "missing_days",
                "missing_segments_total",
                "missing_seconds_total",
                "missing_half_seconds_total",
                "expected_seconds_strict_total",
                "total_segments",
                "max_gap_s",
                "gap_threshold_s",
                "cadence_mode",
                "two_plus_ratio",
                "schedule_hash",
            )
            for key in merge_if_missing:
                if key in rep and key not in out["main_l5_validation"]:
                    out["main_l5_validation"][key] = rep.get(key)
            merge_from_report = (
                "reason",
                "reason_code",
                "action_hint",
                "strict_ratio",
                "state",
                "overall_state",
                "engineering_state",
                "source_state",
                "policy_state",
                "missing_seconds_ratio",
                "missing_half_seconds_state",
                "missing_half_seconds_ratio",
                "missing_half_seconds_info_ratio",
                "missing_half_seconds_block_ratio",
                "source_missing_days_count",
                "source_missing_days_tolerance",
                "source_missing_days_blocking",
                "source_blocking",
                "policy_sources",
                "policy_profile",
                "ticks_outside_sessions_seconds_total",
                "expected_seconds_strict_fixed_total",
                "gap_threshold_s_by_session",
                "seconds_with_one_tick_total",
                "gap_buckets_total",
                "gap_buckets_by_session_total",
                "gap_count_gt_30s",
                "tqsdk_check",
            )
            for key in merge_from_report:
                if key in rep:
                    out["main_l5_validation"][key] = rep.get(key)
    except Exception as e:
        out["validation_error"] = str(e)
    return out
