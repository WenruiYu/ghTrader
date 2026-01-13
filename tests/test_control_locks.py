from __future__ import annotations

import os
from pathlib import Path

import pytest

from ghtrader.control.db import JobStore
from ghtrader.control.locks import LockStore


def test_lock_acquire_release_and_conflicts(tmp_path: Path):
    db_path = tmp_path / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)

    pid = os.getpid()
    j1 = "job1"
    j2 = "job2"
    store.create_job(job_id=j1, title="j1", command=["x"], cwd=tmp_path, source="terminal", log_path=None)
    store.create_job(job_id=j2, title="j2", command=["y"], cwd=tmp_path, source="terminal", log_path=None)
    store.update_job(j1, status="running", pid=pid)
    store.update_job(j2, status="running", pid=pid)

    ok, conflicts = locks.acquire(lock_keys=["k1"], job_id=j1, pid=pid, wait=False)
    assert ok is True
    assert conflicts == []

    ok2, conflicts2 = locks.acquire(lock_keys=["k1"], job_id=j2, pid=pid, wait=False)
    assert ok2 is False
    assert conflicts2 and conflicts2[0]["key"] == "k1"

    released = locks.release_all(job_id=j1)
    assert released == 1

    ok3, conflicts3 = locks.acquire(lock_keys=["k1"], job_id=j2, pid=pid, wait=False)
    assert ok3 is True
    assert conflicts3 == []


def test_stale_lock_reaped_when_owner_not_running(tmp_path: Path):
    db_path = tmp_path / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)

    pid = os.getpid()
    owner = "owner"
    waiter = "waiter"
    store.create_job(job_id=owner, title="owner", command=["x"], cwd=tmp_path, source="terminal", log_path=None)
    store.create_job(job_id=waiter, title="waiter", command=["y"], cwd=tmp_path, source="terminal", log_path=None)
    store.update_job(owner, status="running", pid=pid)
    store.update_job(waiter, status="running", pid=pid)

    ok, _ = locks.acquire(lock_keys=["k2"], job_id=owner, pid=pid, wait=False)
    assert ok is True

    # Mark owner finished but leave lock row (simulates a crashy release).
    store.update_job(owner, status="succeeded", finished_at="t")

    ok2, conflicts2 = locks.acquire(lock_keys=["k2"], job_id=waiter, pid=pid, wait=False)
    assert ok2 is True
    assert conflicts2 == []

