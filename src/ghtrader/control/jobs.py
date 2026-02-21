from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
import uuid
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.control.db import JobRecord, JobStore
from ghtrader.control.job_metadata import infer_job_metadata, merge_job_metadata

log = structlog.get_logger()


from ghtrader.util.time import now_iso as _now_iso


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
    metadata: dict[str, Any] | None = None


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
        self._spawn_lock = threading.Lock()
        try:
            cancel_grace_s = float(os.environ.get("GHTRADER_JOB_CANCEL_GRACE_S", "8") or "8")
        except Exception:
            cancel_grace_s = 8.0
        self._cancel_grace_s = max(1.0, float(cancel_grace_s))

    def reconcile(self) -> None:
        """
        Best-effort reconciliation on server startup.

        If a job is marked running but the PID is no longer alive, mark it failed.
        """
        # Running jobs without a PID are always invalid.
        for job in self.store.list_running_jobs():
            if job.pid is None:
                self.store.update_job(job.id, status="failed", finished_at=_now_iso(), error="missing pid")
                continue
        # Reconcile all active jobs (running + queued jobs that already have a PID).
        for job in self.store.list_active_jobs():
            if job.pid is None:
                continue
            if _is_pid_alive(int(job.pid)):
                continue
            self.store.try_mark_failed(
                job_id=job.id,
                finished_at=_now_iso(),
                error="process not alive (dashboard restart or crash)",
            )

    def _terminate_process_group(self, pid: int, *, grace_s: float) -> tuple[bool, bool]:
        """
        Terminate a process group with SIGTERM and optional SIGKILL escalation.

        Returns:
            (terminated, force_killed)
        """
        force_killed = False
        try:
            os.killpg(int(pid), signal.SIGTERM)
        except Exception:
            try:
                os.kill(int(pid), signal.SIGTERM)
            except Exception:
                return False, force_killed

        deadline = time.time() + max(0.1, float(grace_s))
        while time.time() < deadline:
            if not _is_pid_alive(int(pid)):
                return True, force_killed
            time.sleep(0.1)

        if _is_pid_alive(int(pid)):
            force_killed = True
            try:
                os.killpg(int(pid), signal.SIGKILL)
            except Exception:
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except Exception:
                    pass

        hard_deadline = time.time() + 2.0
        while time.time() < hard_deadline:
            if not _is_pid_alive(int(pid)):
                return True, force_killed
            time.sleep(0.05)
        return (not _is_pid_alive(int(pid))), force_killed

    def start_job(self, spec: JobSpec) -> JobRecord:
        job_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"job-{job_id}.log"
        argv = _normalize_argv(list(spec.argv))
        metadata = merge_job_metadata(
            infer_job_metadata(argv=argv, title=spec.title),
            (dict(spec.metadata) if isinstance(spec.metadata, dict) else None),
        )
        rec = self.store.create_job(
            job_id=job_id,
            title=spec.title,
            command=argv,
            cwd=spec.cwd,
            source="dashboard",
            log_path=log_path,
            metadata=metadata,
        )
        started_at = _now_iso()
        env = os.environ.copy()
        env["GHTRADER_JOB_ID"] = job_id
        env["GHTRADER_JOB_SOURCE"] = "dashboard"
        env["GHTRADER_JOB_LOG_PATH"] = str(log_path)
        env.setdefault("GHTRADER_JOB_VERBOSE", "0")
        env.setdefault("GHTRADER_LOG_LEVEL", "info")
        env["PYTHONUNBUFFERED"] = "1"

        with self._spawn_lock:
            with open(log_path, "ab", buffering=0) as f:
                proc = subprocess.Popen(
                    argv,
                    cwd=str(spec.cwd),
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    env=env,
                    preexec_fn=os.setsid,  # new process group
                )
            claimed = self.store.try_mark_started(job_id=job_id, pid=int(proc.pid), started_at=started_at, log_path=log_path)

        if not claimed:
            try:
                os.killpg(int(proc.pid), signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            out = self.store.get_job(job_id)
            assert out is not None
            return out

        def _waiter() -> None:
            try:
                exit_code = proc.wait()
                status = "succeeded" if exit_code == 0 else "failed"
                self.store.try_mark_finished(job_id=job_id, status=status, exit_code=int(exit_code), finished_at=_now_iso())
            except Exception as e:
                self.store.try_mark_failed(job_id=job_id, error=str(e), finished_at=_now_iso())

        t = threading.Thread(target=_waiter, name=f"job-wait-{job_id}", daemon=True)
        t.start()

        out = self.store.get_job(job_id)
        assert out is not None
        return out

    def enqueue_job(self, spec: JobSpec) -> JobRecord:
        """
        Create a queued job record without spawning the subprocess.

        Intended for bulk operations (downloads / probes) where we want to avoid
        starting hundreds of processes at once.
        """
        job_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"job-{job_id}.log"
        argv = _normalize_argv(list(spec.argv))
        metadata = merge_job_metadata(
            infer_job_metadata(argv=argv, title=spec.title),
            (dict(spec.metadata) if isinstance(spec.metadata, dict) else None),
        )
        self.store.create_job(
            job_id=job_id,
            title=spec.title,
            command=argv,
            cwd=spec.cwd,
            source="dashboard",
            log_path=log_path,
            metadata=metadata,
        )
        out = self.store.get_job(job_id)
        assert out is not None
        return out

    def start_queued_job(self, job_id: str) -> JobRecord | None:
        """
        Spawn a previously-enqueued job (status=queued, pid IS NULL).

        Uses an atomic DB claim to avoid double-start when multiple dashboards run.
        """
        job = self.store.get_job(job_id)
        if job is None:
            return None
        if job.pid is not None:
            return job
        if job.status != "queued":
            return job

        argv = _normalize_argv(list(job.command))
        cwd = Path(str(job.cwd))
        log_path = Path(job.log_path) if job.log_path else (self.logs_dir / f"job-{job_id}.log")
        started_at = _now_iso()

        env = os.environ.copy()
        env["GHTRADER_JOB_ID"] = job_id
        env["GHTRADER_JOB_SOURCE"] = str(job.source or "dashboard")
        env["GHTRADER_JOB_LOG_PATH"] = str(log_path)
        env.setdefault("GHTRADER_JOB_VERBOSE", "0")
        env.setdefault("GHTRADER_LOG_LEVEL", "info")
        env["PYTHONUNBUFFERED"] = "1"

        with open(log_path, "ab", buffering=0) as f:
            with self._spawn_lock:
                proc = subprocess.Popen(
                    argv,
                    cwd=str(cwd),
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    env=env,
                    preexec_fn=os.setsid,
                )

                claimed = False
                try:
                    claimed = self.store.try_mark_started(job_id=job_id, pid=int(proc.pid), started_at=started_at, log_path=log_path)
                except Exception:
                    claimed = False

        if not claimed:
            # Another scheduler beat us to it. Terminate our duplicate quickly.
            try:
                os.killpg(int(proc.pid), signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            return self.store.get_job(job_id) or job

        def _waiter() -> None:
            try:
                exit_code = proc.wait()
                status = "succeeded" if exit_code == 0 else "failed"
                self.store.try_mark_finished(job_id=job_id, status=status, exit_code=int(exit_code), finished_at=_now_iso())
            except Exception as e:
                self.store.try_mark_failed(job_id=job_id, error=str(e), finished_at=_now_iso())

        t = threading.Thread(target=_waiter, name=f"job-wait-{job_id}", daemon=True)
        t.start()

        return self.store.get_job(job_id) or job

    def cancel_job(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if job is None:
            return False

        # Allow cancelling an unstarted queued job (pid is NULL).
        if job.pid is None:
            if str(job.status or "").strip().lower() == "queued":
                try:
                    return bool(self.store.try_mark_cancelled(job_id=job_id, finished_at=_now_iso()))
                except Exception as e:
                    self.store.update_job(job_id, error=f"cancel failed: {e}")
                    return False
            return False

        pid = int(job.pid)
        try:
            terminated, forced = self._terminate_process_group(pid, grace_s=self._cancel_grace_s)
            if not terminated:
                self.store.update_job(job_id, error=f"cancel failed: pid={pid} still alive")
                return False
            err = None
            if forced:
                err = f"force-killed after timeout {self._cancel_grace_s:.1f}s"
            finished_at = _now_iso()
            claimed = self.store.try_mark_cancelled(job_id=job_id, finished_at=finished_at, error=err)
            if not claimed:
                # Waiter thread may win the race and write `failed` right after process exit.
                # Since this path has positively terminated the process for cancellation,
                # keep final state consistent as `cancelled`.
                latest = self.store.get_job(job_id)
                if latest is not None and str(latest.status or "").strip().lower() in {"queued", "running", "failed"}:
                    self.store.update_job(job_id, status="cancelled", finished_at=finished_at, error=err)
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

