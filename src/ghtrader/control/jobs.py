from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import uuid
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.control.db import JobRecord, JobStore

log = structlog.get_logger()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _tail_text(path: Path, max_bytes: int = 64_000) -> str:
    if not path.exists():
        return ""
    size = path.stat().st_size
    start = max(0, size - max_bytes)
    with open(path, "rb") as f:
        f.seek(start)
        data = f.read()
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return data.decode(errors="replace")


def _normalize_argv(argv: list[str]) -> list[str]:
    """
    Best-effort normalization to ensure dashboard jobs run with a valid Python.

    Some environments (including CI containers) do not provide a `python` shim.
    Prefer `sys.executable` when the argv requests `python`/`python3` but it is
    not resolvable via PATH.
    """
    if not argv:
        return argv

    # Direct invocation: ["python", ...]
    if argv[0] in {"python", "python3"} and shutil.which(argv[0]) is None:
        return [sys.executable, *argv[1:]]

    # /usr/bin/env python ...
    if argv[0].endswith("/env") and len(argv) >= 2 and argv[1] in {"python", "python3"} and shutil.which(argv[1]) is None:
        return [argv[0], sys.executable, *argv[2:]]

    return argv


@dataclass(frozen=True)
class JobSpec:
    title: str
    argv: list[str]
    cwd: Path


class JobManager:
    """
    Subprocess-based job runner with SQLite persistence and file logs.

    Notes:
    - Jobs are started as a new process group so we can cancel via SIGTERM.
    - We track completion in a background thread and persist status/exit codes.
    """

    def __init__(self, *, store: JobStore, logs_dir: Path) -> None:
        self.store = store
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def reconcile(self) -> None:
        """
        Best-effort reconciliation on server startup.

        If a job is marked running but the PID is no longer alive, mark it failed.
        """
        for job in self.store.list_running_jobs():
            if job.pid is None:
                self.store.update_job(job.id, status="failed", finished_at=_now_iso(), error="missing pid")
                continue
            if not _is_pid_alive(int(job.pid)):
                self.store.update_job(
                    job.id,
                    status="failed",
                    finished_at=_now_iso(),
                    error="process not alive (dashboard restart or crash)",
                )

    def start_job(self, spec: JobSpec) -> JobRecord:
        job_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"job-{job_id}.log"
        argv = _normalize_argv(list(spec.argv))
        rec = self.store.create_job(
            job_id=job_id,
            title=spec.title,
            command=argv,
            cwd=spec.cwd,
            source="dashboard",
            log_path=log_path,
        )
        started_at = _now_iso()

        # Start subprocess
        env = os.environ.copy()
        env["GHTRADER_JOB_ID"] = job_id
        env["GHTRADER_JOB_SOURCE"] = "dashboard"
        env["GHTRADER_JOB_LOG_PATH"] = str(log_path)
        with open(log_path, "ab", buffering=0) as f:
            proc = subprocess.Popen(
                argv,
                cwd=str(spec.cwd),
                stdout=f,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=os.setsid,  # new process group
            )

        self.store.update_job(
            job_id,
            status="running",
            pid=int(proc.pid),
            log_path=log_path,
            started_at=started_at,
        )

        def _waiter() -> None:
            try:
                exit_code = proc.wait()
                # The child process is responsible for writing final status in the shared job DB.
                # This waiter is best-effort backup and must not override an already-final status
                # (e.g. cancelled or succeeded written by the child).
                current = self.store.get_job(job_id)
                if current is None or current.status not in {"running", "queued"}:
                    return
                status = "succeeded" if exit_code == 0 else "failed"
                self.store.update_job(job_id, status=status, exit_code=int(exit_code), finished_at=_now_iso())
            except Exception as e:
                self.store.update_job(job_id, status="failed", finished_at=_now_iso(), error=str(e))

        t = threading.Thread(target=_waiter, name=f"job-wait-{job_id}", daemon=True)
        t.start()

        out = self.store.get_job(job_id)
        assert out is not None
        return out

    def cancel_job(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if job is None or job.pid is None:
            return False

        pid = int(job.pid)
        try:
            os.killpg(pid, signal.SIGTERM)
            self.store.update_job(job_id, status="cancelled", finished_at=_now_iso())
            return True
        except Exception as e:
            self.store.update_job(job_id, error=f"cancel failed: {e}")
            return False

    def read_log_tail(self, job_id: str, max_bytes: int = 64_000) -> str:
        job = self.store.get_job(job_id)
        if job is None or not job.log_path:
            return ""
        return _tail_text(Path(job.log_path), max_bytes=max_bytes)


def python_module_argv(module: str, *args: str) -> list[str]:
    """
    Build a subprocess argv that runs under the current Python interpreter.
    """
    return [sys.executable, "-m", module, *args]

