from __future__ import annotations

import time
from pathlib import Path

import pytest

from ghtrader.control.db import JobStore
from ghtrader.control.jobs import JobManager, JobSpec


def _wait_for_status(store: JobStore, job_id: str, *, timeout_s: float = 10.0) -> str:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        job = store.get_job(job_id)
        assert job is not None
        if job.status not in {"queued", "running"}:
            return job.status
        time.sleep(0.05)
    raise AssertionError("timeout waiting for job to finish")


def test_job_manager_runs_and_captures_log(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.db")
    jm = JobManager(store=store, logs_dir=tmp_path / "logs")

    spec = JobSpec(
        title="echo",
        argv=["/usr/bin/env", "python", "-c", "print('hello-control')"],
        cwd=tmp_path,
    )
    rec = jm.start_job(spec)
    status = _wait_for_status(store, rec.id, timeout_s=10.0)
    assert status == "succeeded"

    log_text = jm.read_log_tail(rec.id)
    assert "hello-control" in log_text


def test_job_manager_cancel(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.db")
    jm = JobManager(store=store, logs_dir=tmp_path / "logs")

    spec = JobSpec(
        title="sleep",
        argv=["/usr/bin/env", "python", "-c", "import time; time.sleep(60)"],
        cwd=tmp_path,
    )
    rec = jm.start_job(spec)
    assert rec.pid is not None

    ok = jm.cancel_job(rec.id)
    assert ok is True

    job = store.get_job(rec.id)
    assert job is not None
    assert job.status == "cancelled"


def test_reconcile_marks_stale_queued_job_failed(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.db")
    jm = JobManager(store=store, logs_dir=tmp_path / "logs")

    rec = store.create_job(
        job_id="stalejob001",
        title="stale queued",
        command=["/usr/bin/env", "python", "-c", "print('noop')"],
        cwd=tmp_path,
        source="dashboard",
        log_path=tmp_path / "logs" / "job-stalejob001.log",
    )
    # Simulate a stale queued record that has a dead pid after process crash/restart.
    store.update_job(rec.id, status="queued", pid=999999)

    jm.reconcile()
    after = store.get_job(rec.id)
    assert after is not None
    assert after.status == "failed"
    assert "process not alive" in str(after.error or "")

