from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from ghtrader.config import env_bool, env_float, env_int
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.supervisor_helpers import (
    argv_opt,
    is_gateway_job,
    is_strategy_job,
    scan_gateway_desired,
    scan_strategy_desired,
)
from ghtrader.util.time import now_iso as _now_iso


def _new_telemetry_bucket() -> dict[str, Any]:
    return {
        "ticks_total": 0,
        "tick_failures_total": 0,
        "started_total": 0,
        "stopped_total": 0,
        "last_tick_at": "",
        "last_tick_error": "",
        "restart_reasons_total": {},
        "stop_reasons_total": {},
    }


_SUPERVISOR_TELEMETRY_LOCK = threading.Lock()
_SUPERVISOR_TELEMETRY: dict[str, dict[str, Any]] = {
    "scheduler": _new_telemetry_bucket(),
    "strategy": _new_telemetry_bucket(),
    "gateway": _new_telemetry_bucket(),
}


def _telemetry_bucket(name: str) -> dict[str, Any]:
    key = str(name or "").strip().lower()
    with _SUPERVISOR_TELEMETRY_LOCK:
        bucket = _SUPERVISOR_TELEMETRY.get(key)
        if bucket is None:
            bucket = _new_telemetry_bucket()
            _SUPERVISOR_TELEMETRY[key] = bucket
        return bucket


def _telemetry_inc_reason(bucket: dict[str, Any], field: str, reason: str) -> None:
    reason_key = str(reason or "unknown").strip() or "unknown"
    reason_map = bucket.get(field)
    if not isinstance(reason_map, dict):
        reason_map = {}
        bucket[field] = reason_map
    reason_map[reason_key] = int(reason_map.get(reason_key) or 0) + 1


def _telemetry_record_tick(name: str, *, started: int = 0, stopped: int = 0) -> None:
    bucket = _telemetry_bucket(name)
    with _SUPERVISOR_TELEMETRY_LOCK:
        bucket["ticks_total"] = int(bucket.get("ticks_total") or 0) + 1
        bucket["started_total"] = int(bucket.get("started_total") or 0) + int(max(0, int(started)))
        bucket["stopped_total"] = int(bucket.get("stopped_total") or 0) + int(max(0, int(stopped)))
        bucket["last_tick_at"] = _now_iso()
        bucket["last_tick_error"] = ""


def _telemetry_record_tick_failure(name: str, *, error: str) -> None:
    bucket = _telemetry_bucket(name)
    with _SUPERVISOR_TELEMETRY_LOCK:
        bucket["ticks_total"] = int(bucket.get("ticks_total") or 0) + 1
        bucket["tick_failures_total"] = int(bucket.get("tick_failures_total") or 0) + 1
        bucket["last_tick_at"] = _now_iso()
        bucket["last_tick_error"] = str(error or "")


def _telemetry_record_restart_reason(name: str, *, reason: str) -> None:
    bucket = _telemetry_bucket(name)
    with _SUPERVISOR_TELEMETRY_LOCK:
        _telemetry_inc_reason(bucket, "restart_reasons_total", reason)


def _telemetry_record_stop_reason(name: str, *, reason: str) -> None:
    bucket = _telemetry_bucket(name)
    with _SUPERVISOR_TELEMETRY_LOCK:
        _telemetry_inc_reason(bucket, "stop_reasons_total", reason)


def supervisor_telemetry_snapshot() -> dict[str, Any]:
    with _SUPERVISOR_TELEMETRY_LOCK:
        snap: dict[str, Any] = {}
        for key, bucket in _SUPERVISOR_TELEMETRY.items():
            snap[str(key)] = {
                "ticks_total": int(bucket.get("ticks_total") or 0),
                "tick_failures_total": int(bucket.get("tick_failures_total") or 0),
                "started_total": int(bucket.get("started_total") or 0),
                "stopped_total": int(bucket.get("stopped_total") or 0),
                "last_tick_at": str(bucket.get("last_tick_at") or ""),
                "last_tick_error": str(bucket.get("last_tick_error") or ""),
                "restart_reasons_total": dict(bucket.get("restart_reasons_total") or {}),
                "stop_reasons_total": dict(bucket.get("stop_reasons_total") or {}),
            }
    return {"generated_at": _now_iso(), "supervisors": snap}


def reset_supervisor_telemetry_for_tests() -> None:
    with _SUPERVISOR_TELEMETRY_LOCK:
        _SUPERVISOR_TELEMETRY.clear()
        _SUPERVISOR_TELEMETRY["scheduler"] = _new_telemetry_bucket()
        _SUPERVISOR_TELEMETRY["strategy"] = _new_telemetry_bucket()
        _SUPERVISOR_TELEMETRY["gateway"] = _new_telemetry_bucket()


def scheduler_enabled() -> bool:
    if env_bool("GHTRADER_DISABLE_TQSDK_SCHEDULER", False):
        return False
    # Avoid background threads during pytest unless explicitly enabled.
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and not env_bool(
        "GHTRADER_ENABLE_TQSDK_SCHEDULER_IN_TESTS", False
    ):
        return False
    return True


def gateway_supervisor_enabled() -> bool:
    """
    Whether the dashboard should supervise AccountGateway processes.

    Disabled by default during pytest to avoid background process management in unit tests.
    """
    if env_bool("GHTRADER_DISABLE_GATEWAY_SUPERVISOR", False):
        return False
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and not env_bool(
        "GHTRADER_ENABLE_GATEWAY_SUPERVISOR_IN_TESTS", False
    ):
        return False
    return True


def strategy_supervisor_enabled() -> bool:
    """
    Whether the dashboard should supervise StrategyRunner processes.

    Disabled by default during pytest to avoid background process management in unit tests.
    """
    if env_bool("GHTRADER_DISABLE_STRATEGY_SUPERVISOR", False):
        return False
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and not env_bool(
        "GHTRADER_ENABLE_STRATEGY_SUPERVISOR_IN_TESTS", False
    ):
        return False
    return True


def tqsdk_scheduler_tick(
    *,
    store: Any,
    jm: Any,
    max_parallel: int,
    is_tqsdk_heavy_job: Callable[[list[str]], bool],
) -> int:
    """
    Run one scheduling tick: start up to `max_parallel` TqSdk-heavy queued jobs.
    """
    active = [j for j in store.list_active_jobs() if is_tqsdk_heavy_job(j.command)]
    slots = int(max_parallel) - int(len(active))
    if slots <= 0:
        return 0
    queued = [j for j in store.list_unstarted_queued_jobs(limit=5000) if is_tqsdk_heavy_job(j.command)]
    started = 0
    for j in queued[:slots]:
        out = jm.start_queued_job(j.id)
        if out is not None and out.pid is not None:
            started += 1
    _telemetry_record_tick("scheduler", started=int(started), stopped=0)
    return started


def strategy_supervisor_tick(*, store: Any, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one StrategySupervisor tick.
    """
    desired_by_profile = scan_strategy_desired(runs_dir=runs_dir)

    active_jobs = store.list_active_jobs()
    active_strategy: dict[str, Any] = {}
    for j in active_jobs:
        if not is_strategy_job(j.command):
            continue
        prof = argv_opt(j.command, "--account") or ""
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(prof)
        except Exception:
            prof = str(prof).strip().lower() or "default"
        active_strategy[prof] = j

    stopped = 0
    started = 0

    # Stop strategies whose desired mode is explicitly idle.
    for prof, job in active_strategy.items():
        cfg = desired_by_profile.get(prof) or {}
        dm = str(cfg.get("mode") or "idle").strip().lower()
        if dm in {"", "idle"}:
            try:
                if bool(jm.cancel_job(job.id)):
                    stopped += 1
                    _telemetry_record_stop_reason("strategy", reason="desired_idle")
            except Exception:
                pass

    # Start at most one strategy per tick.
    for prof, cfg in desired_by_profile.items():
        dm = str(cfg.get("mode") or "idle").strip().lower()
        if dm in {"", "idle"}:
            continue
        if prof in active_strategy:
            continue

        symbols: list[str] = []
        raw_syms = cfg.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]
        if not symbols:
            continue

        model_name = str(cfg.get("model_name") or "xgboost").strip() or "xgboost"
        horizon = str(cfg.get("horizon") or "50").strip()
        threshold_up = str(cfg.get("threshold_up") or "0.6").strip()
        threshold_down = str(cfg.get("threshold_down") or "0.6").strip()
        position_size = str(cfg.get("position_size") or "1").strip()
        artifacts_dir = str(cfg.get("artifacts_dir") or "artifacts").strip()
        poll_interval_sec = str(cfg.get("poll_interval_sec") or "0.5").strip()

        argv = python_module_argv(
            "ghtrader.cli",
            "strategy",
            "run",
            "--account",
            prof,
            "--model",
            model_name,
            "--horizon",
            horizon,
            "--threshold-up",
            threshold_up,
            "--threshold-down",
            threshold_down,
            "--position-size",
            position_size,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            str(runs_dir),
            "--poll-interval-sec",
            poll_interval_sec,
        )
        for s in symbols:
            argv += ["--symbols", s]
        title = f"strategy {prof} {model_name} h={horizon}"
        try:
            restart_reason = "desired_run_no_active_job"
            jm.start_job(
                JobSpec(
                    title=title,
                    argv=argv,
                    cwd=Path.cwd(),
                    metadata={
                        "supervisor": "strategy",
                        "restart_reason": restart_reason,
                        "supervisor_tick_at": _now_iso(),
                    },
                )
            )
            started += 1
            _telemetry_record_restart_reason("strategy", reason=restart_reason)
        except Exception:
            pass
        break

    _telemetry_record_tick("strategy", started=int(started), stopped=int(stopped))
    return {"started": int(started), "stopped": int(stopped)}


def gateway_supervisor_tick(*, store: Any, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one GatewaySupervisor tick.
    """
    desired_by_profile = scan_gateway_desired(runs_dir=runs_dir)

    active_jobs = store.list_active_jobs()
    active_gateway: dict[str, Any] = {}
    for j in active_jobs:
        if not is_gateway_job(j.command):
            continue
        prof = argv_opt(j.command, "--account") or ""
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(prof)
        except Exception:
            prof = str(prof).strip().lower() or "default"
        active_gateway[prof] = j

    stopped = 0
    started = 0

    # Stop gateways whose desired mode is explicitly idle.
    for prof, job in active_gateway.items():
        dm = str(desired_by_profile.get(prof) or "").strip().lower()
        if dm == "idle":
            try:
                if bool(jm.cancel_job(job.id)):
                    stopped += 1
                    _telemetry_record_stop_reason("gateway", reason="desired_idle")
            except Exception:
                pass

    # Start at most one gateway per tick to avoid a thundering herd.
    for prof, dm in desired_by_profile.items():
        dm2 = str(dm or "").strip().lower()
        if dm2 in {"", "idle"}:
            continue
        if prof in active_gateway:
            continue
        argv = python_module_argv(
            "ghtrader.cli",
            "gateway",
            "run",
            "--account",
            prof,
            "--runs-dir",
            str(runs_dir),
        )
        title = f"gateway {prof}"
        try:
            restart_reason = "desired_non_idle_no_active_job"
            jm.start_job(
                JobSpec(
                    title=title,
                    argv=argv,
                    cwd=Path.cwd(),
                    metadata={
                        "supervisor": "gateway",
                        "restart_reason": restart_reason,
                        "supervisor_tick_at": _now_iso(),
                    },
                )
            )
            started += 1
            _telemetry_record_restart_reason("gateway", reason=restart_reason)
        except Exception:
            pass
        break

    _telemetry_record_tick("gateway", started=int(started), stopped=int(stopped))
    return {"started": int(started), "stopped": int(stopped)}


def start_tqsdk_scheduler(
    *,
    app: Any,
    log: Any,
    tick: Callable[[Any, Any, int], Any],
    max_parallel_default: int = 4,
) -> None:
    if getattr(app.state, "_tqsdk_scheduler_started", False):
        return
    app.state._tqsdk_scheduler_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                max_parallel = int(env_int("GHTRADER_MAX_PARALLEL_TQSDK_JOBS", int(max_parallel_default)))
                max_parallel = max(1, max_parallel)
                tick(store, jm, max_parallel)
            except Exception as e:
                log.warning("tqsdk_scheduler.tick_failed", error=str(e))
                _telemetry_record_tick_failure("scheduler", error=str(e))
            time.sleep(1.0)

    t = threading.Thread(target=_loop, name="tqsdk-job-scheduler", daemon=True)
    t.start()


def start_strategy_supervisor(
    *,
    app: Any,
    log: Any,
    tick: Callable[[Any, Any, Path], Any],
    get_runs_dir: Callable[[], Path],
) -> None:
    if getattr(app.state, "_strategy_supervisor_started", False):
        return
    app.state._strategy_supervisor_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                runs_dir = get_runs_dir()
                tick(store, jm, runs_dir)
            except Exception as e:
                log.warning("strategy_supervisor.tick_failed", error=str(e))
                _telemetry_record_tick_failure("strategy", error=str(e))
            time.sleep(float(env_float("GHTRADER_STRATEGY_SUPERVISOR_POLL_SECONDS", 2.0)))

    t = threading.Thread(target=_loop, name="strategy-supervisor", daemon=True)
    t.start()


def start_gateway_supervisor(
    *,
    app: Any,
    log: Any,
    tick: Callable[[Any, Any, Path], Any],
    get_runs_dir: Callable[[], Path],
) -> None:
    if getattr(app.state, "_gateway_supervisor_started", False):
        return
    app.state._gateway_supervisor_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                runs_dir = get_runs_dir()
                tick(store, jm, runs_dir)
            except Exception as e:
                log.warning("gateway_supervisor.tick_failed", error=str(e))
                _telemetry_record_tick_failure("gateway", error=str(e))
            time.sleep(float(env_float("GHTRADER_GATEWAY_SUPERVISOR_POLL_SECONDS", 2.0)))

    t = threading.Thread(target=_loop, name="gateway-supervisor", daemon=True)
    t.start()


__all__ = [
    "scheduler_enabled",
    "gateway_supervisor_enabled",
    "strategy_supervisor_enabled",
    "tqsdk_scheduler_tick",
    "strategy_supervisor_tick",
    "gateway_supervisor_tick",
    "start_tqsdk_scheduler",
    "start_strategy_supervisor",
    "start_gateway_supervisor",
    "supervisor_telemetry_snapshot",
    "reset_supervisor_telemetry_for_tests",
]

