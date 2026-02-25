from __future__ import annotations

import importlib
from pathlib import Path

from ghtrader.control.db import JobRecord
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.tq.gateway import GatewayDesired, write_gateway_desired
from ghtrader.trading.strategy_control import StrategyDesired, write_strategy_desired


class _FakeStore:
    def __init__(self, active: list[JobRecord]) -> None:
        self._active = list(active)

    def list_active_jobs(self):  # type: ignore[no-untyped-def]
        return list(self._active)


class _FakeJM:
    def __init__(self) -> None:
        self.started: list[JobSpec] = []
        self.cancelled: list[str] = []

    def start_job(self, spec: JobSpec) -> None:
        self.started.append(spec)

    def cancel_job(self, job_id: str) -> bool:
        self.cancelled.append(job_id)
        return True


def test_strategy_supervisor_telemetry_records_restart_reason(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    write_strategy_desired(
        runs_dir=runs_dir,
        profile="alt",
        desired=StrategyDesired(mode="run", symbols=["SHFE.cu2602"], model_name="xgboost", horizon=50),
    )
    bootstrap_mod = importlib.import_module("ghtrader.control.bootstrap")
    bootstrap_mod.reset_supervisor_telemetry_for_tests()

    out = bootstrap_mod.strategy_supervisor_tick(store=_FakeStore(active=[]), jm=_FakeJM(), runs_dir=runs_dir)
    assert int(out.get("started") or 0) == 1

    snap = bootstrap_mod.supervisor_telemetry_snapshot()
    strategy = ((snap.get("supervisors") or {}).get("strategy") or {})
    assert int(strategy.get("started_total") or 0) >= 1
    reasons = strategy.get("restart_reasons_total") if isinstance(strategy.get("restart_reasons_total"), dict) else {}
    assert int(reasons.get("desired_run_no_active_job") or 0) >= 1


def test_gateway_supervisor_telemetry_records_stop_reason(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    write_gateway_desired(runs_dir=runs_dir, profile="alt", desired=GatewayDesired(mode="idle"))

    job = JobRecord(
        id="job1",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        status="running",
        title="gateway alt",
        command=python_module_argv("ghtrader.cli", "gateway", "run", "--account", "alt", "--runs-dir", str(runs_dir)),
        cwd=str(tmp_path),
        source="dashboard",
        pid=123,
    )
    bootstrap_mod = importlib.import_module("ghtrader.control.bootstrap")
    bootstrap_mod.reset_supervisor_telemetry_for_tests()

    out = bootstrap_mod.gateway_supervisor_tick(store=_FakeStore(active=[job]), jm=_FakeJM(), runs_dir=runs_dir)
    assert int(out.get("stopped") or 0) == 1

    snap = bootstrap_mod.supervisor_telemetry_snapshot()
    gateway = ((snap.get("supervisors") or {}).get("gateway") or {})
    assert int(gateway.get("stopped_total") or 0) >= 1
    reasons = gateway.get("stop_reasons_total") if isinstance(gateway.get("stop_reasons_total"), dict) else {}
    assert int(reasons.get("desired_idle") or 0) >= 1
