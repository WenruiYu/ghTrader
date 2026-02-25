from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI

from ghtrader.config import get_runs_dir
from ghtrader.control.db import JobStore
from ghtrader.control.job_command import extract_cli_subcommands
from ghtrader.control.jobs import JobManager
from ghtrader.control.supervisors import (
    gateway_supervisor_enabled as _gateway_supervisor_enabled_impl,
    gateway_supervisor_tick as _gateway_supervisor_tick_impl,
    reset_supervisor_telemetry_for_tests as _reset_supervisor_telemetry_for_tests_impl,
    scheduler_enabled as _scheduler_enabled_impl,
    start_gateway_supervisor as _start_gateway_supervisor_impl,
    start_strategy_supervisor as _start_strategy_supervisor_impl,
    start_tqsdk_scheduler as _start_tqsdk_scheduler_impl,
    strategy_supervisor_enabled as _strategy_supervisor_enabled_impl,
    strategy_supervisor_tick as _strategy_supervisor_tick_impl,
    supervisor_telemetry_snapshot as _supervisor_telemetry_snapshot_impl,
    tqsdk_scheduler_tick as _tqsdk_scheduler_tick_impl,
)

_TQSDK_HEAVY_SUBCOMMANDS = {"account"}
_TQSDK_HEAVY_DATA_SUBCOMMANDS = {"repair", "health", "main-l5-validate"}


def job_subcommand2(argv: list[str]) -> tuple[str | None, str | None]:
    return extract_cli_subcommands(argv)


def is_tqsdk_heavy_job(argv: list[str]) -> bool:
    sub1, sub2 = job_subcommand2(argv)
    s1 = sub1 or ""
    s2 = sub2 or ""
    if s1 in _TQSDK_HEAVY_SUBCOMMANDS:
        return True
    # Nested CLI groups (e.g., `ghtrader data health`) may use TqSdk and must be throttled.
    if s1 == "data" and s2 in _TQSDK_HEAVY_DATA_SUBCOMMANDS:
        return True
    return False


def scheduler_enabled() -> bool:
    return _scheduler_enabled_impl()


def tqsdk_scheduler_tick(*, store: JobStore, jm: JobManager, max_parallel: int) -> int:
    """
    Run one scheduling tick: start up to `max_parallel` TqSdk-heavy queued jobs.

    Exposed for unit tests.
    """
    return _tqsdk_scheduler_tick_impl(
        store=store,
        jm=jm,
        max_parallel=max_parallel,
        is_tqsdk_heavy_job=is_tqsdk_heavy_job,
    )


def start_tqsdk_scheduler(app: FastAPI, *, log: Any, max_parallel_default: int = 4) -> None:
    _start_tqsdk_scheduler_impl(
        app=app,
        log=log,
        tick=lambda store, jm, max_parallel: tqsdk_scheduler_tick(
            store=store,
            jm=jm,
            max_parallel=max_parallel,
        ),
        max_parallel_default=max_parallel_default,
    )


def gateway_supervisor_enabled() -> bool:
    return _gateway_supervisor_enabled_impl()


def strategy_supervisor_enabled() -> bool:
    return _strategy_supervisor_enabled_impl()


def strategy_supervisor_tick(*, store: JobStore, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one StrategySupervisor tick.

    Exposed for unit tests (pass fake `jm` to avoid launching subprocesses).
    """
    return _strategy_supervisor_tick_impl(store=store, jm=jm, runs_dir=runs_dir)


def gateway_supervisor_tick(*, store: JobStore, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one GatewaySupervisor tick.

    Exposed for unit tests (pass fake `jm` to avoid launching subprocesses).
    """
    return _gateway_supervisor_tick_impl(store=store, jm=jm, runs_dir=runs_dir)


def start_strategy_supervisor(app: FastAPI, *, log: Any) -> None:
    _start_strategy_supervisor_impl(
        app=app,
        log=log,
        tick=lambda store, jm, runs_dir: strategy_supervisor_tick(
            store=store,
            jm=jm,
            runs_dir=runs_dir,
        ),
        get_runs_dir=get_runs_dir,
    )


def start_gateway_supervisor(app: FastAPI, *, log: Any) -> None:
    _start_gateway_supervisor_impl(
        app=app,
        log=log,
        tick=lambda store, jm, runs_dir: gateway_supervisor_tick(
            store=store,
            jm=jm,
            runs_dir=runs_dir,
        ),
        get_runs_dir=get_runs_dir,
    )


def supervisor_telemetry_snapshot() -> dict[str, Any]:
    return _supervisor_telemetry_snapshot_impl()


def reset_supervisor_telemetry_for_tests() -> None:
    _reset_supervisor_telemetry_for_tests_impl()


def control_root(runs_dir: Path) -> Path:
    return runs_dir / "control"


def jobs_db_path(runs_dir: Path) -> Path:
    return control_root(runs_dir) / "jobs.db"


def logs_dir(runs_dir: Path) -> Path:
    return control_root(runs_dir) / "logs"
