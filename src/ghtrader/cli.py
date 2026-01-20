"""
ghTrader CLI: unified entrypoint for all operations.

Subcommands:
- download: fetch historical L5 ticks via TqSdk Pro (tq_dl)
- record: run live tick recorder
- build: generate features and labels from QuestDB (canonical)
- train: train models (baseline or deep)
- backtest: run TqSdk backtest harness
- paper: run paper-trading loop with online calibrator
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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import click
import structlog

from ghtrader.config import get_runs_dir, load_config
from ghtrader.util.json_io import read_json as _read_json_shared, write_json_atomic as _write_json_atomic_shared

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
    force_debug = bool(is_dashboard or env_verbose or env_level in {"debug", "trace"})
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
    # Dashboard jobs already redirect stdout/stderr to the job log file.
    # Avoid adding an additional FileHandler (it would duplicate lines).
    if log_path and (os.environ.get("GHTRADER_JOB_SOURCE", "").strip() or "terminal") != "dashboard":
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path))
    logging.basicConfig(format="%(message)s", level=level, handlers=handlers)


_heartbeat_thread: threading.Thread | None = None
_heartbeat_stop = threading.Event()


def _start_job_heartbeat() -> None:
    """
    Emit periodic heartbeat logs so dashboard jobs show ongoing progress.
    """
    global _heartbeat_thread
    if _heartbeat_thread and _heartbeat_thread.is_alive():
        return
    if os.environ.get("GHTRADER_JOB_SOURCE", "").strip() != "dashboard":
        return
    try:
        interval = float(os.environ.get("GHTRADER_JOB_HEARTBEAT_S", "15") or "15")
    except Exception:
        interval = 15.0
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
    """
    Acquire strict cross-session locks for the current CLI run (if job context is present).
    """
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
    
    # Load configuration from .env file
    load_config()
    _start_job_heartbeat()


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbol", "-s", required=True, help="Symbol to download (e.g., SHFE.cu2502)")
@click.option("--start", "-S", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Start date (YYYY-MM-DD)")
@click.option("--end", "-E", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="End date (YYYY-MM-DD)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--chunk-days", default=5, type=int, show_default=True, help="Days per download chunk")
@click.option("--force/--no-force", default=False, show_default=True, help="Force re-download for the date range")
@click.pass_context
def download(ctx: click.Context, symbol: str, start: datetime, end: datetime,
             data_dir: str, chunk_days: int, force: bool) -> None:
    """Download historical L5 ticks for a symbol into QuestDB (canonical)."""
    _ = ctx, symbol, start, end, data_dir, chunk_days, force
    raise click.ClickException("download deferred (Phase-1/2)")


# ---------------------------------------------------------------------------
# download-contract-range
# ---------------------------------------------------------------------------

@main.command("download-contract-range")
@click.option("--exchange", required=True, type=click.Choice(["SHFE"]), help="Exchange (currently SHFE only)")
@click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
@click.option("--start-contract", required=True, type=str, help="Start contract YYMM (e.g., 1601)")
@click.option("--end-contract", required=True, type=str, help="End contract YYMM (e.g., 2701) or 'auto'")
@click.option("--start-date", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Backfill start date (YYYY-MM-DD)")
@click.option("--end-date", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Backfill end date (YYYY-MM-DD). Default: today.")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--chunk-days", default=5, type=int, help="Days per download chunk")
@click.pass_context
def download_contract_range(
    ctx: click.Context,
    exchange: str,
    variety: str,
    start_contract: str,
    end_contract: str,
    start_date: datetime | None,
    end_date: datetime | None,
    data_dir: str,
    chunk_days: int,
) -> None:
    """Exhaustively backfill L5 ticks for a YYMM contract range (no akshare)."""
    _ = (
        ctx,
        exchange,
        variety,
        start_contract,
        end_contract,
        start_date,
        end_date,
        data_dir,
        chunk_days,
    )
    raise click.ClickException("download-contract-range deferred (Phase-1/2)")


# ---------------------------------------------------------------------------
# update (remote-aware daily forward fill)
# ---------------------------------------------------------------------------


@main.command("update")
@click.option("--exchange", required=True, type=click.Choice(["SHFE"]), help="Exchange (currently SHFE only)")
@click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
@click.option("--symbols", "-s", multiple=True, help="Optional symbol filter(s) (e.g., SHFE.cu2602)")
@click.option(
    "--recent-expired-days",
    default=10,
    type=int,
    show_default=True,
    help="Include contracts expired within last N trading days",
)
@click.option(
    "--refresh-catalog/--no-refresh-catalog",
    default=True,
    show_default=True,
    help="Refresh TqSdk contract catalog (network)",
)
@click.option(
    "--catalog-ttl-seconds",
    default=3600,
    type=int,
    show_default=True,
    help="Catalog cache TTL in seconds (used when refresh is false or gated)",
)
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs directory root")
@click.option("--chunk-days", default=5, type=int, show_default=True, help="Days per download chunk")
@click.pass_context
def update(
    ctx: click.Context,
    exchange: str,
    variety: str,
    symbols: tuple[str, ...],
    recent_expired_days: int,
    refresh_catalog: bool,
    catalog_ttl_seconds: int,
    data_dir: str,
    runs_dir: str,
    chunk_days: int,
) -> None:
    """Update is deferred in the minimal main_l5 pipeline."""
    _ = ctx, exchange, variety, symbols, recent_expired_days, refresh_catalog, catalog_ttl_seconds, data_dir, runs_dir, chunk_days
    raise click.ClickException("update deferred (Phase-1/2)")


# ---------------------------------------------------------------------------
# data command group (unified data management)
# ---------------------------------------------------------------------------


@main.group("data")
@click.pass_context
def data_group(ctx: click.Context) -> None:
    """Unified data management commands (QuestDB-first)."""
    pass


@data_group.command("status")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", default=None, help="Variety code (e.g., cu, au, ag). If not specified, shows all.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def data_status(ctx: click.Context, exchange: str, variety: str | None, as_json: bool) -> None:
    """Show data status overview from QuestDB index."""
    _ = ctx, exchange, variety, as_json
    raise click.ClickException("data status deferred (Phase-1/2)")


@data_group.command("rebuild-index")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", required=True, help="Variety code (e.g., cu, au, ag)")
@click.option("--parallel/--no-parallel", default=True, show_default=True, help="Use parallel bootstrap")
@click.option("--workers", default=None, type=int, help="Number of parallel workers (default: auto)")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def data_rebuild_index(
    ctx: click.Context,
    exchange: str,
    variety: str,
    parallel: bool,
    workers: int | None,
    as_json: bool,
) -> None:
    """Rebuild QuestDB index tables from tick data."""
    _ = ctx, exchange, variety, parallel, workers, as_json
    raise click.ClickException("data rebuild-index deferred (Phase-1/2)")


# ---------------------------------------------------------------------------
# contracts-snapshot-build (Data Hub Contracts cached snapshot)
# ---------------------------------------------------------------------------


@main.command("contracts-snapshot-build")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu, au, ag)")
@click.option("--refresh-catalog", default=0, type=int, show_default=True, help="Refresh TqSdk contract catalog (network) (0/1)")
@click.option(
    "--questdb-full",
    default=0,
    type=int,
    show_default=True,
    help="Bootstrap/refresh QuestDB symbol-day index by scanning ticks table (slow) (0/1)",
)
@click.option("--data-dir", default="data", show_default=True, help="Data directory root")
@click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
@click.pass_context
def contracts_snapshot_build(
    ctx: click.Context,
    exchange: str,
    variety: str,
    refresh_catalog: int,
    questdb_full: int,
    data_dir: str,
    runs_dir: str,
) -> None:
    """
    Build a cached snapshot for the Data Hub Contracts table.

    Output:
      runs/control/cache/contracts_snapshot/contracts_exchange=<EX>_var=<var>.json
    """
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    _ = ctx, refresh_catalog, questdb_full, data_dir
    ex = str(exchange).upper().strip() or "SHFE"
    v = str(variety).lower().strip() or "cu"
    rd = Path(runs_dir)
    out_dir = rd / "control" / "cache" / "contracts_snapshot"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"contracts_exchange={ex}_var={v}.json"
    snap = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "snapshot_cached_at": datetime.now(timezone.utc).isoformat(),
        "contracts": [],
        "completeness": {"ok": False, "mode": "deferred"},
    }
    out_path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    click.echo(json.dumps({"ok": True, "path": str(out_path)}))
    return

# ---------------------------------------------------------------------------
# data (QuestDB-first completeness tooling) - helper functions
# ---------------------------------------------------------------------------


def _verify_completeness_payload(
    *,
    exchange: str,
    variety: str,
    symbols: list[str],
    start_override: date | None,
    end_override: date | None,
    refresh_catalog: bool,
    allow_download: bool = True,
    data_dir: Path,
    runs_dir: Path,
) -> dict[str, Any]:
    _ = exchange, variety, symbols, start_override, end_override, refresh_catalog, allow_download, data_dir, runs_dir
    return {"ok": False, "error": "completeness deferred (Phase-1/2)"}


@data_group.command("diagnose")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
@click.option("--symbol", "-s", "symbols", multiple=True, help="Optional symbol(s) (default: all in catalog)")
@click.option(
    "--thoroughness",
    default="comprehensive",
    show_default=True,
    type=click.Choice(["quick", "standard", "comprehensive"]),
    help="Validation thoroughness",
)
@click.option(
    "--check-workers",
    default=0,
    type=int,
    show_default=True,
    help="Workers for per-partition checks (field-quality/gaps). 0=auto",
)
@click.option(
    "--force-full-scan/--no-force-full-scan",
    default=False,
    show_default=True,
    help="Revalidate all partitions (ignore incremental cache)",
)
@click.option("--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional expected start override (YYYY-MM-DD)")
@click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional expected end override (YYYY-MM-DD)")
@click.option("--refresh-catalog", default=0, type=int, show_default=True, help="Refresh catalog cache (network) (0/1)")
@click.option("--data-dir", default="data", show_default=True, help="Data directory root")
@click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
@click.option("--json", "as_json", is_flag=True, help="Print full JSON report")
@click.pass_context
def data_diagnose(
    ctx: click.Context,
    exchange: str,
    variety: str,
    symbols: tuple[str, ...],
    thoroughness: str,
    check_workers: int,
    force_full_scan: bool,
    start: datetime | None,
    end: datetime | None,
    refresh_catalog: int,
    data_dir: str,
    runs_dir: str,
    as_json: bool,
) -> None:
    _ = (
        ctx,
        exchange,
        variety,
        symbols,
        thoroughness,
        check_workers,
        force_full_scan,
        start,
        end,
        refresh_catalog,
        data_dir,
        runs_dir,
        as_json,
    )
    raise click.ClickException("data diagnose deferred (Phase-1/2)")


@data_group.command("repair")
@click.option("--report", "report_path", required=True, help="Diagnose report JSON path")
@click.option("--auto-only/--no-auto-only", default=True, show_default=True, help="Execute only auto-fixable actions")
@click.option("--dry-run/--no-dry-run", default=False, show_default=True, help="Plan only (no changes)")
@click.option("--refresh-catalog", default=0, type=int, show_default=True, help="Refresh catalog cache (network) before repairs (0/1)")
@click.option("--chunk-days", default=5, type=int, show_default=True, help="Days per download chunk")
@click.option(
    "--download-workers",
    default=0,
    type=int,
    show_default=True,
    help="Workers for repair downloads (fill-missing-days). 0=auto",
)
@click.option("--data-dir", default="data", show_default=True, help="Data directory root")
@click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
@click.pass_context
def data_repair(
    ctx: click.Context,
    report_path: str,
    auto_only: bool,
    dry_run: bool,
    refresh_catalog: int,
    chunk_days: int,
    download_workers: int,
    data_dir: str,
    runs_dir: str,
) -> None:
    _ = (
        ctx,
        report_path,
        auto_only,
        dry_run,
        refresh_catalog,
        chunk_days,
        download_workers,
        data_dir,
        runs_dir,
    )
    raise click.ClickException("data repair deferred (Phase-1/2)")


@data_group.command("health")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
@click.option(
    "--thoroughness",
    default="comprehensive",
    show_default=True,
    type=click.Choice(["quick", "standard", "comprehensive"]),
    help="Validation thoroughness",
)
@click.option(
    "--check-workers",
    default=0,
    type=int,
    show_default=True,
    help="Workers for per-partition checks (field-quality/gaps). 0=auto",
)
@click.option(
    "--force-full-scan/--no-force-full-scan",
    default=False,
    show_default=True,
    help="Revalidate all partitions (ignore incremental cache)",
)
@click.option("--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional expected start override (YYYY-MM-DD)")
@click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional expected end override (YYYY-MM-DD)")
@click.option("--auto-repair/--no-auto-repair", default=False, show_default=True, help="If enabled, run auto-repair after diagnose")
@click.option("--dry-run/--no-dry-run", default=False, show_default=True, help="If auto-repair, plan only (no changes)")
@click.option("--refresh-catalog", default=0, type=int, show_default=True, help="Refresh catalog cache (network) (0/1)")
@click.option("--chunk-days", default=5, type=int, show_default=True, help="Days per download chunk")
@click.option(
    "--download-workers",
    default=0,
    type=int,
    show_default=True,
    help="Workers for repair downloads (fill-missing-days). 0=auto",
)
@click.option("--data-dir", default="data", show_default=True, help="Data directory root")
@click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
@click.pass_context
def data_health(
    ctx: click.Context,
    exchange: str,
    variety: str,
    thoroughness: str,
    check_workers: int,
    force_full_scan: bool,
    start: datetime | None,
    end: datetime | None,
    auto_repair: bool,
    dry_run: bool,
    refresh_catalog: int,
    chunk_days: int,
    download_workers: int,
    data_dir: str,
    runs_dir: str,
) -> None:
    """
    Convenience workflow: diagnose → (optional) auto-repair → summarize manual-review items.
    """
    _ = (
        ctx,
        exchange,
        variety,
        thoroughness,
        check_workers,
        force_full_scan,
        start,
        end,
        auto_repair,
        dry_run,
        refresh_catalog,
        chunk_days,
        download_workers,
        data_dir,
        runs_dir,
    )
    raise click.ClickException("data health deferred (Phase-1/2)")
    import json

    _ = ctx
    ex = str(exchange).upper().strip() or "SHFE"
    v = str(variety).lower().strip() or "cu"
    dd = Path(data_dir)
    rd = Path(runs_dir)
    refresh_cat = bool(int(refresh_catalog or 0))
    s0 = start.date() if start else None
    s1 = end.date() if end else None

    # Create progress tracker if we have a job ID
    job_id = _current_job_id()
    progress = None
    if job_id:
        from ghtrader.control.progress import JobProgress

        progress = JobProgress(job_id=job_id, runs_dir=rd)
        total_phases = 2 if bool(auto_repair) else 1
        progress.start(total_phases=total_phases, message=f"Starting health check for {ex}.{v}...")

    from ghtrader.data.diagnose import run_diagnose, write_diagnose_report
    from ghtrader.data.repair import execute_repair_plan, generate_repair_plan

    try:
        rep = run_diagnose(
            exchange=ex,
            var=v,
            symbols=None,
            thoroughness=str(thoroughness),
            check_workers=int(check_workers),
            force_full_scan=bool(force_full_scan),
            start=s0,
            end=s1,
            refresh_catalog=bool(refresh_cat),
            allow_download_calendar=True,
            data_dir=dd,
            runs_dir=rd,
            progress=progress,
        )
    except Exception as e:
        if progress:
            progress.set_error(str(e))
        raise

    diag_path = write_diagnose_report(report=rep, runs_dir=rd)

    out: dict[str, Any] = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "diagnose_report_path": str(diag_path),
        "run_id": str(rep.run_id),
        "findings": {
            "auto_fixable": int(len(rep.auto_fixable or [])),
            "manual_review": int(len(rep.manual_review or [])),
            "unfixable": int(len(rep.unfixable or [])),
        },
        "manual_review_codes": sorted({str(f.code) for f in (rep.manual_review or []) if str(f.code)}),
    }

    if bool(auto_repair):
        if rep.unfixable:
            out["ok"] = False
            out["error"] = "unfixable_findings"
            out["hint"] = "Resolve unfixable findings before auto-repair"
        else:
            # Update progress for repair phase
            if progress:
                progress.update(
                    phase="repair",
                    phase_idx=1,
                    total_phases=2,
                    step="generating_plan",
                    step_idx=0,
                    total_steps=1,
                    message="Generating repair plan...",
                )

            plan = generate_repair_plan(report=rep, include_refresh_catalog=bool(refresh_cat), chunk_days=int(chunk_days))
            res = execute_repair_plan(
                plan=plan,
                dry_run=bool(dry_run),
                auto_only=True,
                data_dir=dd,
                runs_dir=rd,
                progress=progress,
                download_workers=int(download_workers),
            )
            out["repair_run_id"] = str(plan.run_id)
            out["repair_ok"] = bool(res.ok)
            out["repair_actions"] = res.actions
            if not bool(res.ok):
                out["ok"] = False
            else:
                # If we bootstrapped the index, re-run a quick diagnose and run a second repair pass
                # so missing-days backfills can be computed after the index becomes available.
                did_bootstrap = any(a.kind == "bootstrap_index" for a in (plan.actions or []))
                if did_bootstrap and not bool(dry_run):
                    if progress:
                        progress.update(
                            phase="repair",
                            step="post_bootstrap_diagnose",
                            message="Re-running diagnose after index bootstrap...",
                        )

                    rep2 = run_diagnose(
                        exchange=ex,
                        var=v,
                        symbols=None,
                        thoroughness="quick",
                        start=s0,
                        end=s1,
                        refresh_catalog=False,
                        allow_download_calendar=True,
                        data_dir=dd,
                        runs_dir=rd,
                        progress=None,  # Don't report detailed progress for quick re-diagnose
                    )
                    diag_path2 = write_diagnose_report(report=rep2, runs_dir=rd)
                    out["diagnose_report_path_after_bootstrap"] = str(diag_path2)
                    if not rep2.unfixable:
                        plan2 = generate_repair_plan(report=rep2, include_refresh_catalog=False, chunk_days=int(chunk_days))
                        if any(a.kind != "bootstrap_index" for a in (plan2.actions or [])):
                            res2 = execute_repair_plan(
                                plan=plan2,
                                dry_run=False,
                                auto_only=True,
                                data_dir=dd,
                                runs_dir=rd,
                                progress=progress,
                                download_workers=int(download_workers),
                            )
                            out["repair_run_id_2"] = str(plan2.run_id)
                            out["repair_ok_2"] = bool(res2.ok)
                            out["repair_actions_2"] = res2.actions
                            if not bool(res2.ok):
                                out["ok"] = False

    # Mark progress complete
    if progress:
        if out.get("ok", False):
            progress.finish(message=f"Health check complete for {ex}.{v}")
        else:
            progress.set_error(out.get("error") or "Health check failed")

    click.echo(json.dumps(out, ensure_ascii=False, indent=2, default=str, sort_keys=True))
    if not bool(out.get("ok", False)):
        raise SystemExit(1)


@data_group.command("l5-start")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
@click.option("--symbol", "-s", "symbols", multiple=True, help="Optional symbol(s) (default: all in catalog)")
@click.option("--refresh-catalog", default=0, type=int, show_default=True, help="Refresh catalog cache (network) (0/1)")
@click.option("--data-dir", default="data", show_default=True, help="Data directory root")
@click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
@click.option("--json", "as_json", is_flag=True, help="Print full JSON payload")
@click.pass_context
def data_l5_start(
    ctx: click.Context,
    exchange: str,
    variety: str,
    symbols: tuple[str, ...],
    refresh_catalog: int,
    data_dir: str,
    runs_dir: str,
    as_json: bool,
) -> None:
    """
    Compute first L5 trading day per symbol (QuestDB index-backed) and persist a report under runs/.
    """
    _ = ctx, exchange, variety, symbols, refresh_catalog, data_dir, runs_dir, as_json
    raise click.ClickException("data l5-start deferred (Phase-1/2)")
# ---------------------------------------------------------------------------
# account (broker account profiles; env-only)
# ---------------------------------------------------------------------------


@main.group("account")
@click.pass_context
def account_group(ctx: click.Context) -> None:
    """Broker account profiles (env-based): list and verify (read-only)."""
    _ = ctx


@account_group.command("list")
@click.option("--json", "as_json", is_flag=True, help="Print JSON results to stdout")
def account_list(as_json: bool) -> None:
    import json

    from ghtrader.tq.runtime import is_trade_account_configured, list_account_profiles_from_env

    profiles = list_account_profiles_from_env()
    out = [{"profile": p, "configured": bool(is_trade_account_configured(profile=p))} for p in profiles]
    if as_json:
        click.echo(json.dumps({"ok": True, "profiles": out}, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        for r in out:
            click.echo(f"{r['profile']}\tconfigured={r['configured']}")


@account_group.command("verify")
@click.option("--account", "account_profile", default="default", show_default=True, type=str, help="Account profile to verify")
@click.option("--timeout-sec", default=20.0, show_default=True, type=float, help="Timeout for connect + snapshot")
@click.option("--json", "as_json", is_flag=True, help="Print JSON results to stdout")
@click.pass_context
def account_verify(ctx: click.Context, account_profile: str, timeout_sec: float, as_json: bool) -> None:
    """
    Verify a broker account profile (read-only): connect and capture a single snapshot.

    Writes a non-secret cache file under runs/control/cache/accounts/.
    """
    import json
    import signal
    import threading
    import time
    from datetime import timezone

    from ghtrader.config import get_runs_dir
    from ghtrader.tq.runtime import (
        canonical_account_profile,
        create_tq_account,
        create_tq_api,
        is_trade_account_configured,
        load_trade_account_config_from_env,
        now_utc_iso,
        snapshot_account_state,
    )

    _ = ctx
    prof = canonical_account_profile(account_profile)
    # Serialize by account profile.
    _acquire_locks([f"trade:account={prof}"])

    runs_dir = get_runs_dir()
    cache_dir = runs_dir / "control" / "cache" / "accounts"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"account={prof}.json"

    payload: dict[str, Any] = {
        "profile": prof,
        "ok": False,
        "configured": bool(is_trade_account_configured(profile=prof)),
        "verified_at": now_utc_iso(),
        "timeout_sec": float(timeout_sec or 0.0),
        "error": "",
    }

    try:
        cfg = load_trade_account_config_from_env(profile=prof)
        # Mask: never write full ids.
        payload["broker_id"] = str(cfg.broker_id)
        aid = str(cfg.account_id)
        payload["account_id_masked"] = (aid[:2] + "***" + aid[-2:]) if len(aid) >= 6 else "***"

        # Some broker endpoints can hang (eg. market closed / login denied). Enforce a hard timeout so
        # the dashboard doesn't wedge on verify jobs and locks.
        timeout_s = float(timeout_sec or 0.0)
        api = None

        class _Timeout:
            def __init__(self, seconds: float):
                self.seconds = float(seconds or 0.0)
                self._old = None

            def __enter__(self):
                if self.seconds <= 0:
                    return self

                def _handler(_signum, _frame):  # type: ignore[no-untyped-def]
                    raise TimeoutError(f"timeout after {self.seconds:.1f}s")

                self._old = signal.signal(signal.SIGALRM, _handler)
                signal.setitimer(signal.ITIMER_REAL, self.seconds)
                return self

            def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
                if self.seconds > 0:
                    try:
                        signal.setitimer(signal.ITIMER_REAL, 0.0)
                    except Exception:
                        pass
                    try:
                        if self._old is not None:
                            signal.signal(signal.SIGALRM, self._old)
                    except Exception:
                        pass
                return False

        def _safe_close(a) -> None:  # type: ignore[no-untyped-def]
            try:
                a.close()
            except Exception:
                return

        account = create_tq_account(mode="live", monitor_only=True, account_profile=prof)  # type: ignore[arg-type]
        with _Timeout(timeout_s):
            api = create_tq_api(account=account)
            # Wait for at least one update so account data is populated (best-effort).
            try:
                ok_update = bool(api.wait_update(deadline=time.time() + min(5.0, max(0.0, timeout_s))))
                if not ok_update:
                    raise TimeoutError("timeout waiting for first update")
            except Exception:
                # If wait_update fails, still attempt a snapshot (it may include useful error fields).
                pass

            # We don't know symbols yet; snapshot_account_state supports empty symbols.
            snap = snapshot_account_state(api=api, symbols=[], account=account, account_meta={"account_profile": prof})
            payload["snapshot"] = snap
            # Consider verified "ok" if we could produce a snapshot object.
            payload["ok"] = True

        if api is not None:
            t = threading.Thread(target=_safe_close, args=(api,), daemon=True)
            t.start()
            t.join(timeout=2.0)
    except Exception as e:
        payload["ok"] = False
        payload["error"] = str(e)

    # Atomic-ish write.
    tmp = cache_path.with_suffix(f".tmp-{int(time.time()*1000)}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(cache_path)

    if as_json:
        click.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str, sort_keys=True))


# ---------------------------------------------------------------------------
# gateway (AccountGateway; OMS/EMS per account profile)
# ---------------------------------------------------------------------------


@main.group("gateway")
@click.pass_context
def gateway_group(ctx: click.Context) -> None:
    """AccountGateway (OMS/EMS): per-account-profile gateway process."""
    _ = ctx


@gateway_group.command("run")
@click.option("--account", "account_profile", default="default", show_default=True, type=str, help="Account profile to run")
@click.option("--runs-dir", default="runs", show_default=True, type=str, help="Runs directory root")
@click.option("--snapshot-interval-sec", default=10.0, show_default=True, type=float, help="Snapshot interval (seconds)")
@click.option("--poll-interval-sec", default=0.5, show_default=True, type=float, help="wait_update polling interval (seconds)")
@click.pass_context
def gateway_run(
    ctx: click.Context,
    account_profile: str,
    runs_dir: str,
    snapshot_interval_sec: float,
    poll_interval_sec: float,
) -> None:
    """
    Run the AccountGateway loop for a broker account profile.

    This process owns TqApi connectivity and persists transparent artifacts under:
      runs/gateway/account=<PROFILE>/
    """
    from ghtrader.tq.gateway import run_gateway
    from ghtrader.tq.runtime import canonical_account_profile

    _ = ctx
    prof = canonical_account_profile(account_profile)
    _acquire_locks([f"trade:account={prof}"])
    run_gateway(
        account_profile=prof,
        runs_dir=Path(runs_dir),
        snapshot_interval_sec=float(snapshot_interval_sec),
        poll_interval_sec=float(poll_interval_sec),
    )


# ---------------------------------------------------------------------------
# strategy (AI StrategyRunner; consumes gateway market snapshots)
# ---------------------------------------------------------------------------


@main.group("strategy")
@click.pass_context
def strategy_group(ctx: click.Context) -> None:
    """AI StrategyRunner: reads gateway state, writes targets."""
    _ = ctx


@strategy_group.command("run")
@click.option("--account", "account_profile", default="default", show_default=True, type=str, help="Account profile to bind targets to")
@click.option(
    "--symbols",
    "-s",
    required=True,
    multiple=True,
    help="Execution symbols (can specify multiple). Must match gateway subscribed symbols.",
)
@click.option(
    "--model",
    "model_name",
    default="xgboost",
    show_default=True,
    type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
    help="Model type",
)
@click.option("--horizon", default=50, show_default=True, type=int, help="Label horizon")
@click.option("--threshold-up", default=0.6, show_default=True, type=float, help="Long threshold")
@click.option("--threshold-down", default=0.6, show_default=True, type=float, help="Short threshold")
@click.option("--position-size", default=1, show_default=True, type=int, help="Target position size")
@click.option("--artifacts-dir", default="artifacts", show_default=True, type=str, help="Artifacts directory")
@click.option("--runs-dir", default="runs", show_default=True, type=str, help="Runs directory")
@click.option("--poll-interval-sec", default=0.5, show_default=True, type=float, help="Poll interval (seconds)")
@click.pass_context
def strategy_run(
    ctx: click.Context,
    account_profile: str,
    symbols: tuple[str, ...],
    model_name: str,
    horizon: int,
    threshold_up: float,
    threshold_down: float,
    position_size: int,
    artifacts_dir: str,
    runs_dir: str,
    poll_interval_sec: float,
) -> None:
    from ghtrader.trading.strategy_runner import StrategyConfig, run_strategy_runner
    from ghtrader.tq.runtime import canonical_account_profile

    _ = ctx
    prof = canonical_account_profile(account_profile)
    cfg = StrategyConfig(
        account_profile=prof,
        symbols=[str(s).strip() for s in symbols if str(s).strip()],
        model_name=model_name,  # type: ignore[arg-type]
        horizon=int(horizon),
        threshold_up=float(threshold_up),
        threshold_down=float(threshold_down),
        position_size=int(position_size),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
        poll_interval_sec=float(poll_interval_sec),
    )
    run_strategy_runner(cfg)


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbols", "-s", required=True, multiple=True,
              help="Symbols to record (can specify multiple)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.pass_context
def record(ctx: click.Context, symbols: tuple[str, ...], data_dir: str) -> None:
    """Run live tick recorder (QuestDB canonical)."""
    _ = ctx, symbols, data_dir
    raise click.ClickException("record deferred (Phase-1/2)")


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbol", "-s", required=True, help="Symbol to build features for")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--horizons", default="10,50,200", help="Comma-separated label horizons (ticks)")
@click.option("--threshold-k", default=1, type=int, help="Label threshold in price ticks")
@click.option(
    "--ticks-kind",
    "--ticks-lake",
    default="raw",
    type=click.Choice(["raw", "main_l5"]),
    show_default=True,
    help="Which ticks kind to read from (raw ticks vs derived main-with-depth ticks).",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    show_default=True,
    help="Overwrite existing features/labels outputs for this symbol (full rebuild). Default is incremental/resume.",
)
@click.pass_context
def build(ctx: click.Context, symbol: str, data_dir: str, horizons: str,
          threshold_k: int, ticks_kind: str, overwrite: bool) -> None:
    """Build features and labels (QuestDB-first)."""
    from ghtrader.datasets.features import FactorEngine
    from ghtrader.datasets.labels import build_labels_for_symbol

    log = structlog.get_logger()
    if str(symbol).startswith("KQ.m@") and str(ticks_kind) != "main_l5":
        raise click.ClickException(
            "Continuous symbols (KQ.m@...) are L1-only in raw ticks. "
            "Build on the derived L5 dataset instead: run `ghtrader main-l5 --var <var>` "
            "and then `ghtrader build --ticks-kind main_l5 ...`."
        )
    _acquire_locks([f"build:symbol={symbol},ticks_kind={ticks_kind}"])
    horizon_list = [int(h.strip()) for h in horizons.split(",")]
    log.info(
        "build.start",
        symbol=symbol,
        horizons=horizon_list,
        threshold_k=threshold_k,
        ticks_kind=ticks_kind,
        overwrite=overwrite,
    )

    # Build labels
    build_labels_for_symbol(
        symbol=symbol,
        data_dir=Path(data_dir),
        horizons=horizon_list,
        threshold_k=threshold_k,
        ticks_kind=ticks_kind,  # type: ignore[arg-type]
        overwrite=overwrite,
    )

    # Build features
    engine = FactorEngine()
    engine.build_features_for_symbol(
        symbol=symbol,
        data_dir=Path(data_dir),
        ticks_kind=ticks_kind,  # type: ignore[arg-type]
        overwrite=overwrite,
    )

    log.info("build.done", symbol=symbol)


# ---------------------------------------------------------------------------
# train
# ---------------------------------------------------------------------------

@main.command()
@click.option("--model", "-m", required=True,
              type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
              help="Model type to train")
@click.option("--symbol", "-s", required=True, help="Symbol to train on")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts output directory")
@click.option("--horizon", default=50, type=int, help="Label horizon to train on")
@click.option("--gpus", default=1, type=int, help="Number of GPUs for deep models")
@click.option("--epochs", default=50, type=int, help="Epochs for deep models")
@click.option("--batch-size", default=256, type=int, help="Batch size for deep models")
@click.option("--seq-len", default=100, type=int, help="Sequence length (ticks) for sequence models")
@click.option("--lr", default=1e-3, type=float, help="Learning rate for deep models")
@click.option(
    "--ddp/--no-ddp",
    default=True,
    show_default=True,
    help="Use DDP when launched via torchrun (WORLD_SIZE>1). Disable to force single-process behavior.",
)
@click.pass_context
def train(ctx: click.Context, model: str, symbol: str, data_dir: str,
          artifacts_dir: str, horizon: int, gpus: int,
          epochs: int, batch_size: int, seq_len: int, lr: float, ddp: bool) -> None:
    """Train a model (baseline or deep)."""
    from ghtrader.research.models import train_model

    log = structlog.get_logger()
    _acquire_locks([f"train:symbol={symbol},model={model},h={horizon}"])
    log.info(
        "train.start",
        model=model,
        symbol=symbol,
        horizon=horizon,
        gpus=gpus,
        epochs=epochs,
        batch_size=batch_size,
        seq_len=seq_len,
        lr=lr,
        ddp=ddp,
    )

    deep_models = {"deeplob", "transformer", "tcn", "tlob", "ssm"}
    model_kwargs = {"seq_len": seq_len} if model in deep_models else {}

    train_model(
        model_type=model,
        symbol=symbol,
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        horizon=horizon,
        gpus=gpus,
        epochs=epochs,
        batch_size=batch_size,
        lr=lr,
        ddp=ddp,
        **model_kwargs,
    )
    log.info("train.done", model=model, symbol=symbol)


# ---------------------------------------------------------------------------
# backtest
# ---------------------------------------------------------------------------

@main.command()
@click.option("--model", "-m", required=True, help="Model name/path to backtest")
@click.option("--symbol", "-s", required=True, help="Symbol to backtest on")
@click.option("--start", "-S", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Backtest start date")
@click.option("--end", "-E", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Backtest end date")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
@click.option("--runs-dir", default="runs", help="Runs output directory")
@click.pass_context
def backtest(ctx: click.Context, model: str, symbol: str, start: datetime,
             end: datetime, data_dir: str, artifacts_dir: str, runs_dir: str) -> None:
    """Run TqSdk backtest harness with trained model."""
    from ghtrader.tq.eval import run_backtest

    log = structlog.get_logger()
    log.info("backtest.start", model=model, symbol=symbol, start=start.date(), end=end.date())
    run_backtest(
        model_name=model,
        symbol=symbol,
        start_date=start.date(),
        end_date=end.date(),
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
    )
    log.info("backtest.done", model=model, symbol=symbol)


# ---------------------------------------------------------------------------
# paper
# ---------------------------------------------------------------------------

@main.command()
@click.option("--model", "-m", required=True, help="Model name/path for inference")
@click.option("--symbols", "-s", required=True, multiple=True,
              help="Symbols to trade (can specify multiple)")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
@click.pass_context
def paper(ctx: click.Context, model: str, symbols: tuple[str, ...],
          artifacts_dir: str) -> None:
    """Run paper-trading loop with online calibrator (no real orders)."""
    from ghtrader.tq.paper import run_paper_trading

    log = structlog.get_logger()
    log.info("paper.start", model=model, symbols=symbols)
    run_paper_trading(
        model_name=model,
        symbols=list(symbols),
        artifacts_dir=Path(artifacts_dir),
    )


# ---------------------------------------------------------------------------
# benchmark
# ---------------------------------------------------------------------------

@main.command()
@click.option("--model", "-m", required=True,
              type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
              help="Model type to benchmark")
@click.option("--symbol", "-s", required=True, help="Symbol to benchmark on")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
@click.option("--runs-dir", default="runs", help="Runs directory")
@click.option("--horizon", default=50, type=int, help="Label horizon")
@click.pass_context
def benchmark(ctx: click.Context, model: str, symbol: str, data_dir: str,
              artifacts_dir: str, runs_dir: str, horizon: int) -> None:
    """Run a benchmark for a model on a symbol."""
    from ghtrader.research.benchmark import run_benchmark

    log = structlog.get_logger()
    log.info("benchmark.cli", model=model, symbol=symbol)
    report = run_benchmark(
        model_type=model,
        symbol=symbol,
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
        horizon=horizon,
    )
    log.info("benchmark.result", accuracy=f"{report.offline.accuracy:.3f}")


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbol", "-s", required=True, help="Symbol to compare on")
@click.option("--models", "-m", default="logistic,xgboost,deeplob",
              help="Comma-separated list of models to compare")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
@click.option("--runs-dir", default="runs", help="Runs directory")
@click.option("--horizon", default=50, type=int, help="Label horizon")
@click.pass_context
def compare(ctx: click.Context, symbol: str, models: str, data_dir: str,
            artifacts_dir: str, runs_dir: str, horizon: int) -> None:
    """Compare multiple models on the same dataset."""
    from ghtrader.research.benchmark import compare_models

    log = structlog.get_logger()
    model_list = [m.strip() for m in models.split(",")]
    log.info("compare.cli", symbol=symbol, models=model_list)
    
    df = compare_models(
        model_types=model_list,
        symbol=symbol,
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
        horizon=horizon,
    )
    
    # Print results
    click.echo("\nModel Comparison Results:")
    click.echo(df.to_string(index=False))


# ---------------------------------------------------------------------------
# daily-train (scheduled pipeline)
# ---------------------------------------------------------------------------

@main.command("daily-train")
@click.option("--symbols", "-s", required=True, multiple=True,
              help="Symbols to train (can specify multiple)")
@click.option("--model", "-m", default="deeplob",
              type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
              help="Model type to train")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
@click.option("--runs-dir", default="runs", help="Runs directory")
@click.option("--horizon", default=50, type=int, help="Label horizon")
@click.option("--lookback-days", default=30, type=int, help="Days of history to use")
@click.pass_context
def daily_train(ctx: click.Context, symbols: tuple[str, ...], model: str,
                data_dir: str, artifacts_dir: str, runs_dir: str,
                horizon: int, lookback_days: int) -> None:
    """Run daily training pipeline: refresh data, build, train, evaluate, promote."""
    from ghtrader.research.pipeline import run_daily_pipeline

    log = structlog.get_logger()
    lock_keys: list[str] = []
    for s in symbols:
        lock_keys.append(f"ticks:symbol={s}")
        lock_keys.append(f"build:symbol={s},ticks_kind=raw")
        lock_keys.append(f"train:symbol={s},model={model},h={horizon}")
    _acquire_locks(lock_keys)
    log.info("daily_train.start", symbols=symbols, model=model)
    run_daily_pipeline(
        symbols=list(symbols),
        model_type=model,
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
        horizon=horizon,
        lookback_days=lookback_days,
    )


# ---------------------------------------------------------------------------
# sweep (Ray-based parallel hyperparameter search)
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbol", "-s", required=True, help="Symbol to sweep on")
@click.option("--model", "-m", default="deeplob",
              type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer"]),
              help="Model type to sweep")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
@click.option("--runs-dir", default="runs", help="Runs directory")
@click.option("--n-trials", default=20, type=int, help="Number of trials")
@click.option("--n-cpus", default=8, type=int, help="CPUs per trial")
@click.option("--n-gpus", default=1, type=int, help="GPUs per trial")
@click.pass_context
def sweep(ctx: click.Context, symbol: str, model: str,
          data_dir: str, artifacts_dir: str, runs_dir: str,
          n_trials: int, n_cpus: int, n_gpus: int) -> None:
    """Run Ray-based hyperparameter sweep."""
    from ghtrader.research.pipeline import run_hyperparam_sweep

    log = structlog.get_logger()
    _acquire_locks([f"sweep:symbol={symbol},model={model}"])
    log.info("sweep.start", symbol=symbol, model=model, n_trials=n_trials)
    run_hyperparam_sweep(
        symbol=symbol,
        model_type=model,
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
        n_trials=n_trials,
        n_cpus_per_trial=n_cpus,
        n_gpus_per_trial=n_gpus,
    )


# ---------------------------------------------------------------------------
# dashboard (SSH-only web control plane)
# ---------------------------------------------------------------------------

@main.command()
@click.option("--host", default="127.0.0.1", show_default=True, help="Bind host (use 127.0.0.1 for SSH-only access)")
@click.option("--port", default=8000, type=int, show_default=True, help="Bind port")
@click.option("--reload/--no-reload", default=False, show_default=True, help="Auto-reload on code changes (dev only)")
@click.option("--token", default=None, help="Optional access token for the dashboard")
@click.pass_context
def dashboard(ctx: click.Context, host: str, port: int, reload: bool, token: str | None) -> None:
    """Start the SSH-only web dashboard (FastAPI)."""
    if token:
        os.environ["GHTRADER_DASHBOARD_TOKEN"] = token

    import uvicorn

    uvicorn.run(
        "ghtrader.control.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


# ---------------------------------------------------------------------------
# main-schedule (build main roll schedule from TqSdk mapping)
# ---------------------------------------------------------------------------

@main.command("main-schedule")
@click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.pass_context
def main_schedule(
    ctx: click.Context,
    variety: str,
    start: datetime,
    end: datetime,
    data_dir: str,
) -> None:
    """Build a main-contract roll schedule (date -> underlying contract)."""
    from ghtrader.data.main_schedule import build_main_schedule

    log = structlog.get_logger()
    _acquire_locks([f"main_schedule:var={variety.lower()}"])
    res = build_main_schedule(
        var=variety,
        start=start.date(),
        end=end.date(),
        data_dir=Path(data_dir),
    )
    log.info(
        "main_schedule.done",
        schedule_table=str(res.questdb_table),
        schedule_hash=res.schedule_hash,
        rows=len(res.schedule),
    )


# ---------------------------------------------------------------------------
# main-l5 (materialize derived main-with-depth ticks)
# ---------------------------------------------------------------------------

@main.command("main-l5")
@click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
@click.option(
    "--symbol",
    "derived_symbol",
    default="",
    help="Derived symbol (default: KQ.m@SHFE.<var>)",
)
@click.option("--data-dir", default="data", help="Data directory root")
@click.pass_context
def main_l5(
    ctx: click.Context,
    variety: str,
    derived_symbol: str,
    data_dir: str,
) -> None:
    """Build derived main_l5 ticks from schedule + TqSdk L5 per-day download."""
    from ghtrader.data.main_l5 import build_main_l5

    log = structlog.get_logger()
    var_l = variety.lower().strip()
    ds = (derived_symbol or "").strip() or f"KQ.m@SHFE.{var_l}"
    _acquire_locks([f"main_l5:symbol={ds}"])
    res = build_main_l5(
        var=var_l,
        derived_symbol=ds,
        exchange="SHFE",
        data_dir=str(data_dir),
    )
    log.info(
        "main_l5.done",
        derived_symbol=res.derived_symbol,
        schedule_hash=str(res.schedule_hash),
        rows_total=int(res.rows_total),
        days_total=int(res.days_total),
    )


# ---------------------------------------------------------------------------
# audit (data integrity verification)
# ---------------------------------------------------------------------------

@main.command()
@click.option(
    "--scope",
    "scopes",
    multiple=True,
    default=["all"],
    show_default=True,
    type=click.Choice(["all", "ticks", "main_l5", "features", "labels", "completeness"]),
    help="What to audit (can pass multiple).",
)
@click.option("--symbol", "symbols", multiple=True, help="Optional symbol filter (ticks/main_l5 only). Can pass multiple.")
@click.option("--exchange", default="", help="(completeness) Exchange filter, e.g. SHFE")
@click.option("--var", "variety", default="", help="(completeness) Variety filter, e.g. cu")
@click.option("--refresh-catalog/--no-refresh-catalog", default=False, show_default=True, help="(completeness) Refresh TqSdk catalog (network)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs output directory")
@click.pass_context
def audit(
    ctx: click.Context,
    scopes: tuple[str, ...],
    symbols: tuple[str, ...],
    exchange: str,
    variety: str,
    refresh_catalog: bool,
    data_dir: str,
    runs_dir: str,
) -> None:
    """Audit data integrity and write a JSON report under runs/audit/."""
    _ = (ctx, scopes, symbols, exchange, variety, refresh_catalog, data_dir, runs_dir)
    raise click.ClickException("audit deferred (Phase-1/2)")
    from ghtrader.data.audit import run_audit

    log = structlog.get_logger()
    out_path, report = run_audit(
        data_dir=Path(data_dir),
        runs_dir=Path(runs_dir),
        scopes=list(scopes),
        dataset_version="v2",
        symbols=[str(s).strip() for s in symbols if str(s).strip()] or None,
        exchange=str(exchange).strip() or None,
        var=str(variety).strip() or None,
        refresh_catalog=bool(refresh_catalog),
    )
    log.info("audit.done", report_path=str(out_path), summary=report.get("summary", {}))

    summary = report.get("summary", {})
    if int(summary.get("errors", 0)) > 0:
        raise SystemExit(1)


# Command-heavy groups live in `ghtrader.cli_commands.*` and are registered here.


from ghtrader.cli_commands.db import register as _register_db
from ghtrader.cli_commands.features import register as _register_features

_register_features(main)
_register_db(main)


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

    # Ensure .env is loaded for runs_dir etc.
    load_config()

    runs_dir = get_runs_dir()
    db_path = _jobs_db_path(runs_dir)
    logs_dir = _logs_dir(runs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Log path: dashboard jobs already redirect stdout/stderr to a file; terminal jobs need a file handler.
    default_log_path = logs_dir / f"job-{job_id}.log"
    log_path_str = os.environ.get("GHTRADER_JOB_LOG_PATH", "").strip()
    log_path = Path(log_path_str) if log_path_str else default_log_path
    if source != "dashboard":
        os.environ["GHTRADER_JOB_LOG_PATH"] = str(log_path)

    from ghtrader.control.db import JobStore

    store = JobStore(db_path)

    # Create job record if missing (terminal sessions).
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
        # Ensure source/log path are populated.
        if not rec.log_path:
            store.update_job(job_id, log_path=log_path)
        if getattr(rec, "source", "") != source:
            store.update_job(job_id, source=source)

    store.update_job(job_id, status="running", pid=os.getpid(), started_at=datetime.now().isoformat(), error="")

    cancelled = {"flag": False}

    def _handle_term(signum: int, _frame: object) -> None:
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
    finally:
        _stop_job_heartbeat()
        # Release locks held by this job
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
