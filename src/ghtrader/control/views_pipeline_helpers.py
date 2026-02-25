from __future__ import annotations

from pathlib import Path
from typing import Any

from ghtrader.config_service import get_config_resolver
from ghtrader.control.views_helpers import safe_int as _safe_int


def l5_calendar_context(*, data_dir: Path, variety: str) -> dict[str, str]:
    l5_start_date = ""
    l5_start_error = ""
    l5_start_env_key = ""
    l5_start_env_expected_key = ""
    l5_start_note = ""
    latest_trading_day = ""
    latest_trading_error = ""
    try:
        from ghtrader.config import get_l5_start_date_with_source, l5_start_env_key as _l5_start_env_key

        l5_start_env_expected_key = _l5_start_env_key(variety=variety)
        d, used_key = get_l5_start_date_with_source(variety=variety)
        l5_start_date = d.isoformat()
        l5_start_env_key = str(used_key or l5_start_env_expected_key)
        if l5_start_env_expected_key and l5_start_env_key and l5_start_env_key != l5_start_env_expected_key:
            l5_start_note = f"{l5_start_env_expected_key} not set; using {l5_start_env_key}"
    except Exception as e:
        l5_start_error = str(e)
    try:
        from ghtrader.data.trading_calendar import latest_trading_day as _latest_trading_day

        latest_trading_day = _latest_trading_day(data_dir=data_dir, refresh=False, allow_download=True).isoformat()
    except Exception as e:
        latest_trading_error = str(e)
    return {
        "l5_start_date": l5_start_date,
        "l5_start_error": l5_start_error,
        "l5_start_env_key": l5_start_env_key,
        "l5_start_env_expected_key": l5_start_env_expected_key,
        "l5_start_note": l5_start_note,
        "latest_trading_day": latest_trading_day,
        "latest_trading_error": latest_trading_error,
    }


def pipeline_guardrails_context() -> dict[str, Any]:
    resolver = get_config_resolver()
    enforce_health_global = resolver.get_bool("GHTRADER_PIPELINE_ENFORCE_HEALTH", True)
    enforce_health_schedule = resolver.get_bool("GHTRADER_MAIN_SCHEDULE_ENFORCE_HEALTH", enforce_health_global)
    enforce_health_main_l5 = resolver.get_bool("GHTRADER_MAIN_L5_ENFORCE_HEALTH", enforce_health_global)
    lock_wait_timeout_s = resolver.get_float("GHTRADER_LOCK_WAIT_TIMEOUT_S", 120.0, min_value=0.0)
    lock_poll_interval_s = resolver.get_float("GHTRADER_LOCK_POLL_INTERVAL_S", 1.0, min_value=0.1)
    lock_force_cancel = resolver.get_bool("GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT", True)
    lock_preempt_grace_s = resolver.get_float("GHTRADER_LOCK_PREEMPT_GRACE_S", 8.0, min_value=1.0)
    total_workers_raw, _src_total = resolver.get_raw_with_source("GHTRADER_MAIN_L5_TOTAL_WORKERS", None)
    segment_workers_raw, _src_seg = resolver.get_raw_with_source("GHTRADER_MAIN_L5_SEGMENT_WORKERS", None)
    total_workers_raw = str(total_workers_raw or "").strip() if total_workers_raw is not None else ""
    segment_workers_raw = str(segment_workers_raw or "").strip() if segment_workers_raw is not None else ""
    total_workers = _safe_int(total_workers_raw, 0, min_value=0) if total_workers_raw else 0
    segment_workers = _safe_int(segment_workers_raw, 0, min_value=0) if segment_workers_raw else 0

    health_gate_strict = bool(enforce_health_schedule and enforce_health_main_l5)
    worker_mode = "bounded" if (total_workers > 0 or segment_workers > 0) else "auto"
    return {
        "enforce_health_global": bool(enforce_health_global),
        "enforce_health_schedule": bool(enforce_health_schedule),
        "enforce_health_main_l5": bool(enforce_health_main_l5),
        "health_gate_state": ("ok" if health_gate_strict else "warn"),
        "health_gate_label": ("strict" if health_gate_strict else "partial"),
        "lock_wait_timeout_s": float(lock_wait_timeout_s),
        "lock_poll_interval_s": float(lock_poll_interval_s),
        "lock_force_cancel_on_timeout": bool(lock_force_cancel),
        "lock_preempt_grace_s": float(lock_preempt_grace_s),
        "lock_recovery_state": ("ok" if lock_force_cancel else "warn"),
        "main_l5_total_workers": int(total_workers),
        "main_l5_segment_workers": int(segment_workers),
        "main_l5_worker_mode": worker_mode,
    }


def validation_profile_suggestion(variety: str) -> dict[str, Any]:
    v = str(variety or "").strip().lower()
    profiles: dict[str, dict[str, Any]] = {
        "cu": {
            "label": "CU sparse-source profile",
            "note": "CU is naturally sparse; use a looser gap threshold for manual validate runs.",
            "gap_threshold_s": 8.0,
            "strict_ratio": 0.65,
            "max_segments_per_day": 300,
        },
        "ag": {
            "label": "AG balanced profile",
            "note": "AG keeps stricter thresholds after night-session attribution fixes.",
            "gap_threshold_s": 5.0,
            "strict_ratio": 0.8,
            "max_segments_per_day": 200,
        },
        "au": {
            "label": "AU conservative profile",
            "note": "AU may hit isolated provider-anomaly days; review missing-day list before blocking.",
            "gap_threshold_s": 5.0,
            "strict_ratio": 0.75,
            "max_segments_per_day": 200,
        },
    }
    return dict(profiles.get(v) or {})


def pipeline_health_context(
    *,
    main_schedule_coverage: dict[str, Any],
    main_l5_coverage: dict[str, Any],
    main_l5_validation: dict[str, Any],
    coverage_error: str,
    validation_error: str,
) -> dict[str, dict[str, Any]]:
    schedule_days = _safe_int(main_schedule_coverage.get("n_days"), 0, min_value=0)
    hashes_raw = main_schedule_coverage.get("schedule_hashes") or []
    hashes: list[str] = [str(h) for h in hashes_raw if str(h).strip()]
    hash_count = len(hashes)

    if coverage_error:
        schedule_state = "error"
        schedule_text = "coverage query failed"
    elif schedule_days <= 0:
        schedule_state = "error"
        schedule_text = "missing"
    elif hash_count == 1:
        schedule_state = "ok"
        schedule_text = f"{schedule_days}d · hash ok"
    else:
        schedule_state = "warn"
        schedule_text = f"{schedule_days}d · {hash_count} hashes"

    main_days = _safe_int(main_l5_coverage.get("n_days"), 0, min_value=0)
    if coverage_error:
        main_l5_state = "error"
        main_l5_text = "coverage query failed"
    elif schedule_days <= 0 and main_days <= 0:
        main_l5_state = "warn"
        main_l5_text = "wait for schedule"
    elif main_days <= 0:
        main_l5_state = "error"
        main_l5_text = "empty"
    elif schedule_days > 0:
        miss = max(0, int(schedule_days - main_days))
        if miss > 0:
            main_l5_state = "warn"
            main_l5_text = f"{main_days}/{schedule_days}d"
        else:
            main_l5_state = "ok"
            main_l5_text = f"{main_days}/{schedule_days}d"
    else:
        main_l5_state = "ok"
        main_l5_text = f"{main_days}d"

    val_status = str(main_l5_validation.get("status") or "").strip().lower()
    val_state = str(main_l5_validation.get("state") or "").strip().lower()
    val_overall = str(main_l5_validation.get("overall_state") or "").strip().lower()
    val_status_label = str(main_l5_validation.get("status_label") or "").strip().lower()
    status_token = val_overall or val_status or val_state or val_status_label
    checked_days = _safe_int(main_l5_validation.get("checked_days"), 0, min_value=0)
    missing_days = _safe_int(main_l5_validation.get("missing_days"), 0, min_value=0)
    missing_segments = _safe_int(main_l5_validation.get("missing_segments_total"), 0, min_value=0)
    if validation_error:
        validation_state = "error"
        validation_text = "summary unavailable"
    elif status_token == "error":
        validation_state = "error"
        validation_text = (
            f"{missing_days}/{checked_days}d miss · {missing_segments} seg"
            if checked_days > 0
            else "error"
        )
    elif status_token == "noop":
        validation_state = "ok"
        validation_text = "up-to-date (noop)"
    elif checked_days <= 0:
        validation_state = "warn"
        validation_text = "not run"
    elif status_token == "ok":
        validation_state = "ok"
        validation_text = f"{checked_days}d clean"
    elif status_token == "warn":
        validation_state = "warn"
        validation_text = (
            f"{missing_days}/{checked_days}d miss · {missing_segments} seg"
            if checked_days > 0
            else "warn"
        )
    else:
        validation_state = "warn"
        validation_text = (
            f"{missing_days}/{checked_days}d miss · {missing_segments} seg"
            if checked_days > 0
            else "warn"
        )

    return {
        "schedule": {"state": schedule_state, "text": schedule_text},
        "main_l5": {"state": main_l5_state, "text": main_l5_text},
        "validation": {"state": validation_state, "text": validation_text},
    }
