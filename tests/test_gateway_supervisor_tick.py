from __future__ import annotations

import importlib
from pathlib import Path

from ghtrader.control.db import JobRecord
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.tq.gateway import GatewayDesired, write_gateway_desired


class _FakeStore:
    def __init__(self, active: list[JobRecord]) -> None:
        self._active = list(active)

    def list_active_jobs(self):  # matches JobStore API used by supervisor tick
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


def test_gateway_supervisor_tick_starts_one_job(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    write_gateway_desired(runs_dir=runs_dir, profile="alt", desired=GatewayDesired(mode="paper", symbols=["SHFE.cu2602"]))

    app_mod = importlib.import_module("ghtrader.control.app")
    tick = getattr(app_mod, "_gateway_supervisor_tick")

    store = _FakeStore(active=[])
    jm = _FakeJM()
    out = tick(store=store, jm=jm, runs_dir=runs_dir)

    assert int(out["started"]) == 1
    assert len(jm.started) == 1
    argv = jm.started[0].argv
    assert "ghtrader.cli" in argv
    assert "gateway" in argv
    assert "run" in argv
    assert "--account" in argv


def test_gateway_supervisor_tick_cancels_when_desired_idle(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    write_gateway_desired(runs_dir=runs_dir, profile="alt", desired=GatewayDesired(mode="idle"))

    argv = python_module_argv("ghtrader.cli", "gateway", "run", "--account", "alt", "--runs-dir", str(runs_dir))
    job = JobRecord(
        id="job1",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        status="running",
        title="gateway alt",
        command=argv,
        cwd=str(tmp_path),
        source="dashboard",
        pid=123,
    )

    app_mod = importlib.import_module("ghtrader.control.app")
    tick = getattr(app_mod, "_gateway_supervisor_tick")

    store = _FakeStore(active=[job])
    jm = _FakeJM()
    out = tick(store=store, jm=jm, runs_dir=runs_dir)
    assert int(out["stopped"]) == 1
    assert jm.cancelled == ["job1"]

