"""
ghTrader CLI thin entrypoint.

Command implementations are registered from `ghtrader.cli_commands.*`.
"""

from __future__ import annotations

import logging
import os
import re
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import structlog

from ghtrader.config import get_runs_dir, load_config

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?")


def _strip_ansi(line: str) -> str:
    return _ANSI_RE.sub("", line)


def _has_timestamp(line: str) -> bool:
    return bool(_TS_RE.match(_strip_ansi(line).lstrip()))


def _now_ts() -> str:
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    return ts.replace("+00:00", "Z")


class TimestampedWriter:
    def __init__(self, stream: Any, *, strip_ansi: bool = False) -> None:
        self._stream = stream
        self._strip_ansi = strip_ansi
        self._buffer = ""
        self._closed = False

    def write(self, data: Any) -> int:
        if self._closed:
            return 0
        if data is None:
            return 0
        if isinstance(data, bytes):
            text = data.decode("utf-8", errors="replace")
        else:
            text = str(data)
        if not text:
            return 0
        self._buffer += text
        out: list[str] = []
        while "\n" in self._buffer:
            line, rest = self._buffer.split("\n", 1)
            self._buffer = rest
            out.append(self._format_line(line, newline=True))
        if out:
            self._stream.write("".join(out))
            self._stream.flush()
        return len(text)

    def flush(self) -> None:
        if self._closed:
            return
        if self._buffer:
            self._stream.write(self._format_line(self._buffer, newline=False))
            self._buffer = ""
        try:
            self._stream.flush()
        except Exception:
            pass

    def close(self) -> None:
        self.flush()
        self._closed = True

    def isatty(self) -> bool:
        return False

    def _format_line(self, line: str, *, newline: bool) -> str:
        out_line = _strip_ansi(line) if self._strip_ansi else line
        if not _has_timestamp(out_line):
            out_line = f"{_now_ts()} {out_line}" if out_line else f"{_now_ts()}"
        if newline:
            out_line += "\n"
        return out_line

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)


def _wrap_dashboard_stdio() -> None:
    if getattr(sys.stdout, "_ghtrader_timestamped", False):
        return
    stdout_wrapped = TimestampedWriter(sys.stdout, strip_ansi=True)
    stderr_wrapped = TimestampedWriter(sys.stderr, strip_ansi=True)
    setattr(stdout_wrapped, "_ghtrader_timestamped", True)
    setattr(stderr_wrapped, "_ghtrader_timestamped", True)
    sys.stdout = stdout_wrapped  # type: ignore[assignment]
    sys.stderr = stderr_wrapped  # type: ignore[assignment]


def _setup_logging(verbose: bool) -> None:
    env_level = os.environ.get("GHTRADER_LOG_LEVEL", "").strip().lower()
    env_verbose = os.environ.get("GHTRADER_JOB_VERBOSE", "").strip().lower() in {"1", "true", "yes", "on"}
    is_dashboard = os.environ.get("GHTRADER_JOB_SOURCE", "").strip() == "dashboard"
    force_debug = bool(is_dashboard or env_verbose or env_level in {"debug", "trace"} or env_level == "")
    if force_debug:
        level = logging.DEBUG
    elif env_level in {"warning", "warn"}:
        level = logging.WARNING
    elif env_level == "error":
        level = logging.ERROR
    elif env_level == "critical":
        level = logging.CRITICAL
    else:
        level = logging.DEBUG if verbose else logging.INFO
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=(not is_dashboard)),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_path = os.environ.get("GHTRADER_JOB_LOG_PATH", "").strip()
    if log_path and (os.environ.get("GHTRADER_JOB_SOURCE", "").strip() or "terminal") != "dashboard":
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path))
    logging.basicConfig(format="%(message)s", level=level, handlers=handlers)


_heartbeat_thread: threading.Thread | None = None
_heartbeat_stop = threading.Event()


def _start_job_heartbeat() -> None:
    """Emit periodic heartbeat logs so dashboard jobs show ongoing progress."""
    global _heartbeat_thread
    if _heartbeat_thread and _heartbeat_thread.is_alive():
        return
    if os.environ.get("GHTRADER_JOB_HEARTBEAT_DISABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    try:
        interval = float(os.environ.get("GHTRADER_JOB_HEARTBEAT_S", "15") or "15")
    except Exception:
        interval = 15.0
    if interval <= 0:
        return
    interval = max(2.0, float(interval))
    _heartbeat_stop.clear()
    log = structlog.get_logger()
    job_id = os.environ.get("GHTRADER_JOB_ID", "").strip()
    cmd = " ".join(sys.argv[1:]).strip() or "ghtrader"
    started_at = time.time()

    log.info("job.heartbeat_start", job_id=job_id, cmd=cmd, interval_s=interval)

    def _run() -> None:
        while not _heartbeat_stop.wait(interval):
            elapsed = int(time.time() - started_at)
            log.info("job.heartbeat", job_id=job_id, cmd=cmd, elapsed_s=elapsed)

    _heartbeat_thread = threading.Thread(target=_run, name=f"job-heartbeat-{job_id or 'local'}", daemon=True)
    _heartbeat_thread.start()


def _stop_job_heartbeat() -> None:
    if _heartbeat_stop.is_set():
        return
    _heartbeat_stop.set()
    if os.environ.get("GHTRADER_JOB_SOURCE", "").strip() == "dashboard":
        log = structlog.get_logger()
        job_id = os.environ.get("GHTRADER_JOB_ID", "").strip()
        cmd = " ".join(sys.argv[1:]).strip() or "ghtrader"
        log.info("job.heartbeat_stop", job_id=job_id, cmd=cmd)


def _control_root(runs_dir: Path) -> Path:
    return runs_dir / "control"


def _jobs_db_path(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "jobs.db"


def _logs_dir(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "logs"


def _current_job_id() -> str | None:
    return os.environ.get("GHTRADER_JOB_ID") or None


def _acquire_locks(lock_keys: list[str]) -> None:
    """Acquire strict cross-session locks for the current CLI run."""
    job_id = _current_job_id()
    if not job_id:
        return

    runs_dir = get_runs_dir()
    from ghtrader.control.db import JobStore
    from ghtrader.control.locks import LockStore

    store = JobStore(_jobs_db_path(runs_dir))
    locks = LockStore(_jobs_db_path(runs_dir))

    store.update_job(job_id, status="queued", waiting_locks=lock_keys, held_locks=[])
    ok, conflicts = locks.acquire(lock_keys=lock_keys, job_id=job_id, pid=os.getpid(), wait=True)
    if not ok:
        raise RuntimeError(f"Failed to acquire locks: {conflicts}")
    store.update_job(job_id, status="running", waiting_locks=[], held_locks=lock_keys)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """ghTrader: AI-centric SHFE tick system (CU/AU/AG)."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    _setup_logging(verbose)
    load_config()
    _start_job_heartbeat()


# Command-heavy groups live in `ghtrader.cli_commands.*` and are registered here.
from ghtrader.cli_commands.data import register as _register_data
from ghtrader.cli_commands.db import register as _register_db
from ghtrader.cli_commands.features import register as _register_features
from ghtrader.cli_commands.research import register as _register_research
from ghtrader.cli_commands.runtime import register as _register_runtime

_register_data(main)
_register_db(main)
_register_features(main)
_register_research(main)
_register_runtime(main)


def entrypoint() -> None:
    """
    CLI entrypoint with session auto-registration + strict locks.

    This is used by both:
    - terminal users running `ghtrader ...`
    - the dashboard spawning subprocess jobs (via env GHTRADER_JOB_ID)
    """
    job_id = os.environ.get("GHTRADER_JOB_ID", "").strip() or uuid.uuid4().hex[:12]
    os.environ["GHTRADER_JOB_ID"] = job_id
    source = os.environ.get("GHTRADER_JOB_SOURCE", "").strip() or "terminal"
    os.environ["GHTRADER_JOB_SOURCE"] = source
    if source == "dashboard":
        _wrap_dashboard_stdio()

    load_config()

    runs_dir = get_runs_dir()
    db_path = _jobs_db_path(runs_dir)
    logs_dir = _logs_dir(runs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    default_log_path = logs_dir / f"job-{job_id}.log"
    log_path_str = os.environ.get("GHTRADER_JOB_LOG_PATH", "").strip()
    log_path = Path(log_path_str) if log_path_str else default_log_path
    if source != "dashboard":
        os.environ["GHTRADER_JOB_LOG_PATH"] = str(log_path)

    from ghtrader.control.db import JobStore

    store = JobStore(db_path)

    rec = store.get_job(job_id)
    if rec is None:
        title = " ".join(sys.argv[1:]).strip() or "ghtrader"
        store.create_job(
            job_id=job_id,
            title=title,
            command=list(sys.argv),
            cwd=Path.cwd(),
            source=source,
            log_path=log_path,
        )
    else:
        if not rec.log_path:
            store.update_job(job_id, log_path=log_path)
        if getattr(rec, "source", "") != source:
            store.update_job(job_id, source=source)

    store.update_job(job_id, status="running", pid=os.getpid(), started_at=datetime.now().isoformat(), error="")

    cancelled = {"flag": False}

    def _handle_term(signum: int, _frame: object) -> None:
        _ = signum
        cancelled["flag"] = True
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, _handle_term)
    signal.signal(signal.SIGINT, _handle_term)

    exit_code = 0
    error: str | None = None
    try:
        main(standalone_mode=False)
    except SystemExit as e:
        try:
            exit_code = int(e.code or 0)
        except Exception:
            exit_code = 1
    except KeyboardInterrupt:
        cancelled["flag"] = True
        exit_code = 130
    except Exception as e:
        exit_code = 1
        error = str(e)
        try:
            log = structlog.get_logger()
            cmd = " ".join(sys.argv[1:]).strip() or "ghtrader"
            log.exception("job.failed", job_id=job_id, cmd=cmd, error=error)
        except Exception:
            pass
    finally:
        _stop_job_heartbeat()
        try:
            from ghtrader.control.locks import LockStore

            LockStore(db_path).release_all(job_id=job_id)
        except Exception:
            pass

        status = "cancelled" if cancelled["flag"] else ("succeeded" if exit_code == 0 else "failed")
        store.update_job(
            job_id,
            status=status,
            exit_code=int(exit_code),
            finished_at=datetime.now().isoformat(),
            error=error,
            waiting_locks=[],
            held_locks=[],
        )

    raise SystemExit(exit_code)


if __name__ == "__main__":
    entrypoint()
