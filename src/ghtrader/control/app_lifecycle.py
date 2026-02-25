from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, Request

from ghtrader.control.bootstrap import (
    gateway_supervisor_enabled as _gateway_supervisor_enabled,
    jobs_db_path as _jobs_db_path,
    logs_dir as _logs_dir,
    scheduler_enabled as _scheduler_enabled,
    start_gateway_supervisor as _start_gateway_supervisor,
    start_strategy_supervisor as _start_strategy_supervisor,
    start_tqsdk_scheduler as _start_tqsdk_scheduler,
    strategy_supervisor_enabled as _strategy_supervisor_enabled,
    supervisor_telemetry_snapshot as _supervisor_telemetry_snapshot,
)
from ghtrader.control.db import JobStore
from ghtrader.control.jobs import JobManager
from ghtrader.util.observability import get_store

log = structlog.get_logger()


def _build_runtime_components(*, runs_dir: Path) -> tuple[JobStore, JobManager]:
    store = JobStore(_jobs_db_path(runs_dir))
    jm = JobManager(store=store, logs_dir=_logs_dir(runs_dir))
    jm.reconcile()
    return store, jm


def _apply_persisted_scheduler_settings(*, runs_dir: Path) -> None:
    # Apply persisted dashboard settings before starting background schedulers.
    try:
        from ghtrader.control.settings import apply_tqsdk_scheduler_settings_from_disk

        apply_tqsdk_scheduler_settings_from_disk(runs_dir=runs_dir)
    except Exception:
        # Preserve current fail-open behavior for non-critical dashboard settings.
        log.warning("control.settings.load_failed")


def _install_http_observability_middleware(app: FastAPI) -> None:
    obs = get_store("control.api")

    @app.middleware("http")
    async def _observe_http_requests(request: Request, call_next: Any) -> Any:
        started = time.perf_counter()
        ok = False
        try:
            response = await call_next(request)
            status = int(getattr(response, "status_code", 500))
            ok = status < 500
            return response
        finally:
            route = request.scope.get("route")
            route_path = str(getattr(route, "path", "") or request.url.path or "/")
            obs.observe(
                metric=f"{request.method} {route_path}",
                latency_s=(time.perf_counter() - started),
                ok=bool(ok),
            )


def _attach_runtime_state(app: FastAPI, *, store: JobStore, jm: JobManager) -> None:
    app.state.job_store = store
    app.state.job_manager = jm
    app.state.supervisor_telemetry_getter = _supervisor_telemetry_snapshot


def _start_background_workers(app: FastAPI) -> None:
    if _scheduler_enabled():
        _start_tqsdk_scheduler(app, log=log)
    if _gateway_supervisor_enabled():
        _start_gateway_supervisor(app, log=log)
    if _strategy_supervisor_enabled():
        _start_strategy_supervisor(app, log=log)
