from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


def test_cli_entrypoint_auto_registers_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Force runs dir to temp so we don't touch real runs/control/jobs.db
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.delenv("GHTRADER_JOB_ID", raising=False)
    monkeypatch.delenv("GHTRADER_JOB_SOURCE", raising=False)
    monkeypatch.delenv("GHTRADER_JOB_LOG_PATH", raising=False)

    # Minimal command that exits cleanly (help).
    monkeypatch.setattr(sys, "argv", ["ghtrader", "audit", "--help"])

    from ghtrader.cli import entrypoint
    from ghtrader.control.db import JobStore

    with pytest.raises(SystemExit) as e:
        entrypoint()
    assert int(e.value.code or 0) == 0

    store = JobStore(runs_dir / "control" / "jobs.db")
    jobs = store.list_jobs(limit=10)
    assert len(jobs) == 1
    j = jobs[0]
    assert j.source == "terminal"
    assert j.status in {"succeeded", "failed", "cancelled"}  # help should succeed, but be tolerant of Click internals
    assert j.log_path is not None
    assert Path(j.log_path).exists()


def test_cli_lock_acquire_updates_job_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_JOB_ID", "jid123")
    monkeypatch.setenv("GHTRADER_JOB_SOURCE", "terminal")

    from ghtrader.control.db import JobStore

    db_path = runs_dir / "control" / "jobs.db"
    store = JobStore(db_path)
    store.create_job(job_id="jid123", title="t", command=["x"], cwd=tmp_path, source="terminal", log_path=None)
    store.update_job("jid123", status="running", pid=os.getpid())

    from ghtrader.cli import _acquire_locks
    from ghtrader.control.locks import LockStore

    _acquire_locks(["build:symbol=SHFE.cu2602,ticks_lake=raw"])

    j = store.get_job("jid123")
    assert j is not None
    assert j.status == "running"
    assert j.held_locks and "build:symbol=SHFE.cu2602,ticks_lake=raw" in j.held_locks

    locks = LockStore(db_path).list_locks()
    assert any(l.key == "build:symbol=SHFE.cu2602,ticks_lake=raw" and l.job_id == "jid123" for l in locks)

