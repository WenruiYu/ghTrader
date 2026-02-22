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
    monkeypatch.setattr(sys, "argv", ["ghtrader", "--help"])

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
    assert Path(j.log_path).parent.exists()


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

    _acquire_locks(["build:symbol=KQ.m@SHFE.cu,ticks_kind=main_l5"])

    j = store.get_job("jid123")
    assert j is not None
    assert j.status == "running"
    assert j.held_locks and "build:symbol=KQ.m@SHFE.cu,ticks_kind=main_l5" in j.held_locks

    locks = LockStore(db_path).list_locks()
    assert any(l.key == "build:symbol=KQ.m@SHFE.cu,ticks_kind=main_l5" and l.job_id == "jid123" for l in locks)


def test_cli_lock_acquire_timeout_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_LOCK_WAIT_TIMEOUT_S", "0.2")
    monkeypatch.setenv("GHTRADER_LOCK_POLL_INTERVAL_S", "0.05")
    monkeypatch.setenv("GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT", "0")

    from ghtrader.control.db import JobStore
    from ghtrader.control.locks import LockStore

    db_path = runs_dir / "control" / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)
    owner_id = "owner_job"
    waiter_id = "waiter_job"
    store.create_job(job_id=owner_id, title="owner", command=["x"], cwd=tmp_path, source="terminal", log_path=None)
    store.create_job(job_id=waiter_id, title="waiter", command=["y"], cwd=tmp_path, source="terminal", log_path=None)
    store.update_job(owner_id, status="running", pid=os.getpid())
    store.update_job(waiter_id, status="running", pid=os.getpid())
    ok, _ = locks.acquire(lock_keys=["main_l5:symbol=KQ.m@SHFE.au"], job_id=owner_id, pid=os.getpid(), wait=False)
    assert ok is True

    monkeypatch.setenv("GHTRADER_JOB_ID", waiter_id)
    monkeypatch.setenv("GHTRADER_JOB_SOURCE", "terminal")
    from ghtrader.cli import _acquire_locks

    with pytest.raises(RuntimeError, match="Failed to acquire locks"):
        _acquire_locks(["main_l5:symbol=KQ.m@SHFE.au"])


def test_cli_lock_acquire_preempts_conflict_on_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_LOCK_WAIT_TIMEOUT_S", "0.2")
    monkeypatch.setenv("GHTRADER_LOCK_POLL_INTERVAL_S", "0.05")
    monkeypatch.setenv("GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT", "1")
    monkeypatch.setenv("GHTRADER_LOCK_PREEMPT_GRACE_S", "0.1")

    from ghtrader.control.db import JobStore
    from ghtrader.control.locks import LockStore

    db_path = runs_dir / "control" / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)
    owner_id = "owner_job"
    waiter_id = "waiter_job"
    store.create_job(job_id=owner_id, title="owner", command=["x"], cwd=tmp_path, source="terminal", log_path=None)
    store.create_job(job_id=waiter_id, title="waiter", command=["y"], cwd=tmp_path, source="terminal", log_path=None)
    store.update_job(owner_id, status="running", pid=os.getpid())
    store.update_job(waiter_id, status="running", pid=os.getpid())
    ok, _ = locks.acquire(lock_keys=["main_schedule:var=au"], job_id=owner_id, pid=os.getpid(), wait=False)
    assert ok is True

    monkeypatch.setenv("GHTRADER_JOB_ID", waiter_id)
    monkeypatch.setenv("GHTRADER_JOB_SOURCE", "terminal")
    import ghtrader.cli as cli

    monkeypatch.setattr(cli, "_terminate_pid_for_lock", lambda *_args, **_kwargs: True)
    cli._acquire_locks(["main_schedule:var=au"])

    waiter = store.get_job(waiter_id)
    owner = store.get_job(owner_id)
    assert waiter is not None and waiter.held_locks and "main_schedule:var=au" in waiter.held_locks
    assert owner is not None and owner.status in {"cancelled", "failed"}


def test_cli_lock_acquire_can_disable_preempt_on_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_LOCK_WAIT_TIMEOUT_S", "0.2")
    monkeypatch.setenv("GHTRADER_LOCK_POLL_INTERVAL_S", "0.05")
    monkeypatch.setenv("GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT", "1")

    from ghtrader.control.db import JobStore
    from ghtrader.control.locks import LockStore

    db_path = runs_dir / "control" / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)
    owner_id = "owner_job"
    waiter_id = "waiter_job"
    store.create_job(job_id=owner_id, title="owner", command=["x"], cwd=tmp_path, source="terminal", log_path=None)
    store.create_job(job_id=waiter_id, title="waiter", command=["y"], cwd=tmp_path, source="terminal", log_path=None)
    store.update_job(owner_id, status="running", pid=os.getpid())
    store.update_job(waiter_id, status="running", pid=os.getpid())
    ok, _ = locks.acquire(lock_keys=["main_l5:symbol=KQ.m@SHFE.cu"], job_id=owner_id, pid=os.getpid(), wait=False)
    assert ok is True

    monkeypatch.setenv("GHTRADER_JOB_ID", waiter_id)
    monkeypatch.setenv("GHTRADER_JOB_SOURCE", "terminal")
    import ghtrader.cli as cli

    def _unexpected_preempt(*_args, **_kwargs):
        raise AssertionError("lock preempt should be disabled")

    monkeypatch.setattr(cli, "_terminate_pid_for_lock", _unexpected_preempt)
    with pytest.raises(RuntimeError, match="Failed to acquire locks"):
        cli._acquire_locks(
            ["main_l5:symbol=KQ.m@SHFE.cu"],
            wait_timeout_s=0.2,
            preempt_on_timeout=False,
        )

    owner = store.get_job(owner_id)
    assert owner is not None and owner.status == "running"

