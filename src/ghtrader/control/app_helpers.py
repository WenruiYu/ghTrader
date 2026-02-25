from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ghtrader.config_service import get_config_resolver
from ghtrader.control.variety_context import (
    allowed_varieties as _allowed_varieties,
    default_variety as _default_variety,
    symbol_matches_variety as _symbol_matches_variety,
)
from ghtrader.util.time import now_iso as _now_iso


def normalize_variety_for_api(raw: str | None, *, allow_legacy_default: bool = False) -> str:
    v = str(raw or "").strip().lower()
    if not v:
        if allow_legacy_default:
            return _default_variety()
        raise HTTPException(status_code=400, detail="var is required")
    if v not in _allowed_varieties():
        allowed = ",".join(_allowed_varieties())
        raise HTTPException(status_code=400, detail=f"invalid var '{v}', allowed: {allowed}")
    return v


def job_matches_variety(job: Any, variety: str) -> bool:
    v = str(variety or "").strip().lower()
    if not v or v not in set(_allowed_varieties()):
        return True

    meta = getattr(job, "metadata", None)
    if isinstance(meta, dict):
        mv = str(meta.get("variety") or "").strip().lower()
        if mv in set(_allowed_varieties()):
            return mv == v
        ms = str(meta.get("symbol") or "").strip()
        if ms:
            return _symbol_matches_variety(ms, v)
        mss = meta.get("symbols")
        if isinstance(mss, list) and mss:
            return any(_symbol_matches_variety(str(x or ""), v) for x in mss)

    cmd = [str(x or "").lower() for x in (getattr(job, "command", None) or [])]
    for i, tok in enumerate(cmd[:-1]):
        if tok == "--var" and cmd[i + 1] == v:
            return True
    if any(_symbol_matches_variety(tok, v) for tok in cmd):
        return True

    title = str(getattr(job, "title", "") or "").lower()
    if _symbol_matches_variety(title, v):
        return True
    return bool(re.search(rf"(^|\s){re.escape(v)}(\s|$)", title))


def scan_model_files(artifacts_dir: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not artifacts_dir.exists():
        return out
    try:
        pat = re.compile(r"^model_h(?P<h>\d+)\.[a-zA-Z0-9]+$")
        allowed_ext = {".json", ".pt", ".pkl"}
        for f in artifacts_dir.rglob("model_h*.*"):
            try:
                if not f.is_file():
                    continue
                if f.suffix.lower() not in allowed_ext:
                    continue
                m = pat.match(f.name)
                if not m:
                    continue
                rel = f.relative_to(artifacts_dir).parts
                if len(rel) < 3:
                    continue
                namespace = None
                if rel[0] in {"production", "candidates", "temp"} and len(rel) >= 4:
                    namespace = rel[0]
                    symbol = rel[1]
                    model_type = rel[2]
                else:
                    symbol = rel[0]
                    model_type = rel[1]
                horizon = int(m.group("h"))
                st = f.stat()
                out.append(
                    {
                        "symbol": str(symbol),
                        "model_type": str(model_type),
                        "horizon": int(horizon),
                        "namespace": str(namespace) if namespace else "",
                        "path": str(f),
                        "size_bytes": int(st.st_size),
                        "mtime": float(st.st_mtime),
                    }
                )
            except Exception:
                continue
    except Exception:
        return []
    return out


def dashboard_guardrails_context() -> dict[str, Any]:
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
    total_workers = resolver.get_int("GHTRADER_MAIN_L5_TOTAL_WORKERS", 0, min_value=0) if total_workers_raw else 0
    segment_workers = resolver.get_int("GHTRADER_MAIN_L5_SEGMENT_WORKERS", 0, min_value=0) if segment_workers_raw else 0
    health_gate_strict = bool(enforce_health_schedule and enforce_health_main_l5)

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
        "main_l5_worker_mode": ("bounded" if (total_workers > 0 or segment_workers > 0) else "auto"),
    }


def ui_state_payload(
    *,
    state: str,
    text: str,
    error: str = "",
    stale: bool = False,
    updated_at: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "state": str(state or "unknown"),
        "text": str(text or ""),
        "error": str(error or ""),
        "stale": bool(stale),
        "updated_at": str(updated_at or _now_iso()),
    }
    if extra:
        payload.update(extra)
    return payload
