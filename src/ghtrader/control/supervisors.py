from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.supervisor_helpers import (
    argv_opt,
    is_gateway_job,
    is_strategy_job,
    scan_gateway_desired,
    scan_strategy_desired,
)


def scheduler_enabled() -> bool:
    if str(os.environ.get("GHTRADER_DISABLE_TQSDK_SCHEDULER", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    # Avoid background threads during pytest unless explicitly enabled.
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_TQSDK_SCHEDULER_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def gateway_supervisor_enabled() -> bool:
    """
    Whether the dashboard should supervise AccountGateway processes.

    Disabled by default during pytest to avoid background process management in unit tests.
    """
    if str(os.environ.get("GHTRADER_DISABLE_GATEWAY_SUPERVISOR", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_GATEWAY_SUPERVISOR_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def strategy_supervisor_enabled() -> bool:
    """
    Whether the dashboard should supervise StrategyRunner processes.

    Disabled by default during pytest to avoid background process management in unit tests.
    """
    if str(os.environ.get("GHTRADER_DISABLE_STRATEGY_SUPERVISOR", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_STRATEGY_SUPERVISOR_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
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
            jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            started += 1
        except Exception:
            pass
        break

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
            jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            started += 1
        except Exception:
            pass
        break

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
                max_parallel = int(os.environ.get("GHTRADER_MAX_PARALLEL_TQSDK_JOBS", str(max_parallel_default)))
                max_parallel = max(1, max_parallel)
                tick(store, jm, max_parallel)
            except Exception as e:
                log.warning("tqsdk_scheduler.tick_failed", error=str(e))
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
            time.sleep(float(os.environ.get("GHTRADER_STRATEGY_SUPERVISOR_POLL_SECONDS", "2.0")))

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
            time.sleep(float(os.environ.get("GHTRADER_GATEWAY_SUPERVISOR_POLL_SECONDS", "2.0")))

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
]

