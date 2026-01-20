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
    level = logging.DEBUG if verbose else logging.INFO
    is_dashboard = os.environ.get("GHTRADER_JOB_SOURCE", "").strip() == "dashboard"
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
    from ghtrader.tq.ingest import download_historical_ticks

    log = structlog.get_logger()
    _acquire_locks([f"ticks:symbol={symbol}"])
    log.info("download.start", symbol=symbol, start=start.date(), end=end.date(), force=bool(force))
    lv = "v2"
    download_historical_ticks(
        symbol=symbol,
        start_date=start.date(),
        end_date=end.date(),
        data_dir=Path(data_dir),
        chunk_days=int(chunk_days),
        dataset_version=lv,  # type: ignore[arg-type]
        force=bool(force),
    )
    log.info("download.done", symbol=symbol)


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
    from ghtrader.tq.ingest import download_contract_range as _download_contract_range

    log = structlog.get_logger()
    _acquire_locks([f"ticks_range:exchange={exchange},var={variety.lower()}"])
    log.info(
        "download_contract_range.start",
        exchange=exchange,
        var=variety,
        start_contract=start_contract,
        end_contract=end_contract,
        chunk_days=chunk_days,
        start_date=(start_date.date().isoformat() if start_date else ""),
        end_date=(end_date.date().isoformat() if end_date else ""),
    )

    _download_contract_range(
        exchange=exchange,
        var=variety,
        start_contract=start_contract,
        end_contract=end_contract,
        data_dir=Path(data_dir),
        chunk_days=chunk_days,
        start_date=start_date.date() if start_date else None,
        end_date=end_date.date() if end_date else None,
        dataset_version="v2",
    )
    log.info("download_contract_range.done")


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
    """Check remote contract updates and fill forward (active + recently expired)."""
    import json

    from ghtrader.data.update import run_update as _run_update

    _ = ctx
    log = structlog.get_logger()
    _acquire_locks([f"ticks_range:exchange={exchange},var={variety.lower()}"])
    log.info("update.start", exchange=exchange, var=variety, symbols=list(symbols), recent_expired_days=recent_expired_days)

    out_path, report = _run_update(
        exchange=exchange,
        var=variety,
        data_dir=Path(data_dir),
        runs_dir=Path(runs_dir),
        symbols=list(symbols) if symbols else None,
        recent_expired_trading_days=int(recent_expired_days),
        refresh_catalog=bool(refresh_catalog),
        catalog_ttl_seconds=int(catalog_ttl_seconds),
        chunk_days=int(chunk_days),
    )
    click.echo(json.dumps({"ok": bool(report.get("ok", False)), "report_path": str(out_path), "run_id": str(report.get("run_id") or "")}))
    if not bool(report.get("ok", False)):
        raise SystemExit(1)


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
    import json as json_mod

    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import INDEX_TABLE_V2, ensure_index_tables

    log = structlog.get_logger()

    cfg = make_questdb_query_config_from_env()

    ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)

    # Query summary from index table
    import psycopg

    ex = str(exchange).upper().strip()
    var_filter = f"{ex}.{variety.lower()}%" if variety else f"{ex}.%"

    sql = f"""
    SELECT
        symbol,
        count() AS n_days,
        min(trading_day) AS first_day,
        max(trading_day) AS last_day,
        sum(rows_total) AS total_rows,
        max(CASE WHEN l5_present THEN 1 ELSE 0 END) AS has_l5
    FROM {INDEX_TABLE_V2}
    WHERE symbol LIKE %s AND dataset_version = 'v2' AND ticks_kind = 'raw'
    GROUP BY symbol
    ORDER BY symbol
    """

    results: list[dict] = []
    try:
        with psycopg.connect(
            user=cfg.pg_user,
            password=cfg.pg_password,
            host=cfg.host,
            port=cfg.pg_port,
            dbname=cfg.pg_dbname,
            connect_timeout=2,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [var_filter])
                for row in cur.fetchall():
                    results.append({
                        "symbol": str(row[0]),
                        "n_days": int(row[1]) if row[1] else 0,
                        "first_day": str(row[2]) if row[2] else None,
                        "last_day": str(row[3]) if row[3] else None,
                        "total_rows": int(row[4]) if row[4] else 0,
                        "has_l5": bool(row[5]),
                    })
    except Exception as e:
        log.error("data.status.query_failed", error=str(e))
        raise SystemExit(1)

    if as_json:
        click.echo(json_mod.dumps({"symbols": results, "count": len(results)}, indent=2))
    else:
        click.echo(f"Data Status ({ex}, variety={variety or 'all'})")
        click.echo("-" * 60)
        click.echo(f"{'Symbol':<20} {'Days':>6} {'First':>12} {'Last':>12} {'L5':>4}")
        click.echo("-" * 60)
        for r in results:
            l5_str = "Y" if r["has_l5"] else "N"
            click.echo(f"{r['symbol']:<20} {r['n_days']:>6} {r['first_day'] or '':>12} {r['last_day'] or '':>12} {l5_str:>4}")
        click.echo("-" * 60)
        click.echo(f"Total: {len(results)} symbols")


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
    import json as json_mod

    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import (
        INDEX_TABLE_V2,
        bootstrap_symbol_day_index_from_ticks,
        bootstrap_symbol_day_index_parallel,
        ensure_index_tables,
    )
    from ghtrader.tq.catalog import get_contract_catalog

    log = structlog.get_logger()

    cfg = make_questdb_query_config_from_env()

    ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)

    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()

    # Get contract list from catalog
    cat = get_contract_catalog(exchange=ex, var=var, runs_dir=get_runs_dir(), refresh=False)
    if not cat.get("ok"):
        log.error("data.rebuild_index.catalog_failed", error=cat.get("error"))
        raise SystemExit(1)

    contracts = [str((c or {}).get("symbol") or "") for c in (cat.get("contracts") or [])]
    contracts = [c for c in contracts if c]

    log.info("data.rebuild_index.start", exchange=ex, variety=var, n_symbols=len(contracts), parallel=parallel)

    if parallel:
        result = bootstrap_symbol_day_index_parallel(
            cfg=cfg,
            ticks_table="ghtrader_ticks_raw_v2",
            symbols=contracts,
            dataset_version="v2",
            ticks_kind="raw",
            index_table=INDEX_TABLE_V2,
            n_workers=workers,
        )
    else:
        result = bootstrap_symbol_day_index_from_ticks(
            cfg=cfg,
            ticks_table="ghtrader_ticks_raw_v2",
            symbols=contracts,
            dataset_version="v2",
            ticks_kind="raw",
            index_table=INDEX_TABLE_V2,
        )

    if as_json:
        click.echo(json_mod.dumps(result, indent=2))
    else:
        click.echo(f"Index Rebuild Complete")
        click.echo(f"  Rows upserted: {result.get('rows', 0)}")
        click.echo(f"  Time: {result.get('seconds', 0):.2f}s")
        if parallel:
            click.echo(f"  Workers: {result.get('workers_used', 0)}")


@data_group.command("index-bootstrap")
@click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
@click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g., cu, au, ag)")
@click.option("--parallel/--no-parallel", default=True, show_default=True, help="Use parallel bootstrap")
@click.option("--workers", default=None, type=int, help="Number of parallel workers (default: auto)")
@click.option("--data-dir", default="data", show_default=True, help="Data directory root")
@click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def data_index_bootstrap(
    ctx: click.Context,
    exchange: str,
    variety: str,
    parallel: bool,
    workers: int | None,
    data_dir: str,
    runs_dir: str,
    as_json: bool,
) -> None:
    """
    Bootstrap QuestDB symbol-day index from tick data.

    This scans ghtrader_ticks_raw_v2 and populates ghtrader_symbol_day_index_v2
    for the selected exchange/variety. Required when the index is empty or
    incomplete before Verify/Fill can compute meaningful completeness.

    This is an alias for 'data rebuild-index' with explicit data-dir/runs-dir support.
    """
    import json as json_mod

    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import (
        INDEX_TABLE_V2,
        bootstrap_symbol_day_index_from_ticks,
        bootstrap_symbol_day_index_parallel,
        ensure_index_tables,
    )
    from ghtrader.tq.catalog import get_contract_catalog

    log = structlog.get_logger()

    cfg = make_questdb_query_config_from_env()

    ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)

    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()

    # Get contract list from catalog
    cat = get_contract_catalog(exchange=ex, var=var, runs_dir=Path(runs_dir), refresh=False)
    if not cat.get("ok"):
        log.error("data.index_bootstrap.catalog_failed", error=cat.get("error"))
        raise SystemExit(1)

    contracts = [str((c or {}).get("symbol") or "") for c in (cat.get("contracts") or [])]
    contracts = [c for c in contracts if c]

    log.info("data.index_bootstrap.start", exchange=ex, variety=var, n_symbols=len(contracts), parallel=parallel)

    if parallel:
        result = bootstrap_symbol_day_index_parallel(
            cfg=cfg,
            ticks_table="ghtrader_ticks_raw_v2",
            symbols=contracts,
            dataset_version="v2",
            ticks_kind="raw",
            index_table=INDEX_TABLE_V2,
            n_workers=workers,
        )
    else:
        result = bootstrap_symbol_day_index_from_ticks(
            cfg=cfg,
            ticks_table="ghtrader_ticks_raw_v2",
            symbols=contracts,
            dataset_version="v2",
            ticks_kind="raw",
            index_table=INDEX_TABLE_V2,
        )

    if as_json:
        click.echo(json_mod.dumps(result, indent=2))
    else:
        click.echo(f"Index Bootstrap Complete")
        click.echo(f"  Exchange: {ex}")
        click.echo(f"  Variety: {var}")
        click.echo(f"  Symbols processed: {len(contracts)}")
        click.echo(f"  Rows upserted: {result.get('rows', 0)}")
        click.echo(f"  Time: {result.get('seconds', 0):.2f}s")
        if parallel:
            click.echo(f"  Workers: {result.get('workers_used', 0)}")


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
    import threading
    from datetime import timezone, timedelta

    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import (
        INDEX_TABLE_V2,
        bootstrap_symbol_day_index_from_ticks,
        ensure_index_tables,
        list_symbols_from_index,
        query_contract_coverage_from_index,
    )
    from ghtrader.tq.catalog import get_contract_catalog
    from ghtrader.tq.l5_probe import load_probe_result

    _ = ctx
    log = structlog.get_logger()

    ex = str(exchange).upper().strip() or "SHFE"
    v = str(variety).lower().strip() or "cu"
    lv = "v2"
    dd = Path(data_dir)
    rd = Path(runs_dir)
    refresh_cat = bool(int(refresh_catalog or 0))
    q_full = bool(int(questdb_full or 0))

    log.info(
        "contracts_snapshot_build.start",
        exchange=ex,
        var=v,
        dataset_version=lv,
        refresh_catalog=bool(refresh_cat),
        questdb_full=bool(q_full),
        data_dir=str(dd),
        runs_dir=str(rd),
    )

    _acquire_locks([f"contracts_snapshot:exchange={ex},var={v}"])
    log.info("contracts_snapshot_build.lock_acquired", exchange=ex, var=v)

    def _snapshot_path() -> Path:
        return rd / "control" / "cache" / "contracts_snapshot" / f"contracts_exchange={ex}_var={v}.json"

    # Use shared JSON IO helpers (PRD redundancy cleanup)
    _write_json_atomic = _write_json_atomic_shared
    _read_json = _read_json_shared

    def _questdb_cov_cache_path(*, table: str) -> Path:
        tbl_safe = str(table).strip().replace("/", "_")
        return rd / "control" / "cache" / "questdb_coverage" / f"contracts_exchange={ex}_var={v}_table={tbl_safe}.json"

    t0 = time.time()
    log.info("contracts_snapshot_build.stage_start", stage="catalog", refresh_catalog=bool(refresh_cat))
    t_cat0 = time.time()
    if refresh_cat:
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=rd, refresh=True)
    else:
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=rd, refresh=False, allow_stale_cache=True, offline=True)
    t_cat1 = time.time()

    catalog_ok = bool(cat.get("ok", False))
    catalog_error = str(cat.get("error") or "")
    catalog_cached_at = cat.get("cached_at")
    catalog_source = cat.get("source")

    base_contracts = list(cat.get("contracts") or [])
    log.info(
        "contracts_snapshot_build.stage_done",
        stage="catalog",
        ms=int((t_cat1 - t_cat0) * 1000),
        ok=bool(catalog_ok),
        source=str(catalog_source or ""),
        cached_at=str(catalog_cached_at or ""),
        n_contracts=int(len(base_contracts)),
        error=(catalog_error if not catalog_ok else ""),
    )

    tbl = f"ghtrader_ticks_raw_{lv}"
    cfg = make_questdb_query_config_from_env()

    # Union: TqSdk catalog + QuestDB index symbols (QuestDB-first; avoids filesystem scans).
    log.info("contracts_snapshot_build.stage_start", stage="symbol_union")
    t_union0 = time.time()
    by_sym: dict[str, dict[str, Any]] = {}
    for r in base_contracts:
        sym = str((r or {}).get("symbol") or "").strip()
        if sym:
            by_sym[sym] = dict(r)

    added_index = 0
    try:
        ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
        idx_syms = list_symbols_from_index(
            cfg=cfg,
            dataset_version=lv,
            ticks_kind="raw",
            prefix=f"{ex}.{v}",
            index_table=INDEX_TABLE_V2,
            connect_timeout_s=2,
        )
        for sym in idx_syms:
            if sym and sym not in by_sym:
                by_sym[sym] = {"symbol": sym, "expired": None, "expire_datetime": None, "catalog_source": "questdb_index"}
                added_index += 1
    except Exception:
        pass
    t_union1 = time.time()
    log.info(
        "contracts_snapshot_build.stage_done",
        stage="symbol_union",
        ms=int((t_union1 - t_union0) * 1000),
        n_symbols=int(len(by_sym)),
        n_index_added=int(added_index),
    )

    merged_contracts = [by_sym[s] for s in sorted(by_sym.keys())]
    syms = list(by_sym.keys())

    if not merged_contracts:
        err_msg = catalog_error or ("tqsdk_catalog_unavailable" if refresh_cat else "catalog_cache_missing_or_stale")
        payload = {
            "ok": False,
            "error": err_msg,
            "exchange": ex,
            "var": v,
            "dataset_version": lv,
            "contracts": [],
            "catalog_ok": False,
            "catalog_error": err_msg,
            "catalog_cached_at": catalog_cached_at,
            "catalog_source": catalog_source,
            "snapshot_cached_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_cached_at_unix": float(time.time()),
        }
        _write_json_atomic(_snapshot_path(), payload)
        click.echo(json.dumps({"ok": False, "error": err_msg, "snapshot_path": str(_snapshot_path())}))
        raise SystemExit(1)

    cov_latest: dict[str, dict[str, Any]] = {}
    cov_full_cached: dict[str, dict[str, Any]] = {}
    cov: dict[str, dict[str, Any]] = {}
    questdb_info: dict[str, Any] = {"ok": False, "table": tbl, "coverage_mode": "latest"}

    log.info("contracts_snapshot_build.stage_start", stage="questdb_coverage", mode="index", n_symbols=int(len(syms)))
    t_db_latest0 = time.time()
    try:
        ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
        cov_latest = query_contract_coverage_from_index(cfg=cfg, symbols=syms, dataset_version=lv, ticks_kind="raw", index_table=INDEX_TABLE_V2)
        questdb_info = {"ok": True, "table": tbl, "index_table": INDEX_TABLE_V2, "coverage_mode": "index"}
        if not cov_latest:
            # Bootstrap may not have been run yet. Fall back to a fast "last-only" query so the UI still
            # shows freshness while the index gets populated by ingest or an explicit bootstrap job.
            from ghtrader.questdb.queries import query_contract_last_coverage

            try:
                win_days = int(os.environ.get("GHTRADER_CONTRACTS_QUESTDB_LAST_WINDOW_DAYS", "60") or "60")
            except Exception:
                win_days = 60
            win_days = max(1, min(int(win_days), 365))
            today = datetime.now(timezone.utc).date()
            recent_days = [(today - timedelta(days=i)).isoformat() for i in range(int(win_days))]
            cov_latest = query_contract_last_coverage(
                cfg=cfg,
                table=tbl,
                symbols=syms,
                dataset_version=lv,
                ticks_kind="raw",
                recent_days=recent_days,
            )
            questdb_info["coverage_mode"] = "index_empty_fallback_latest"
            questdb_info["index_empty"] = True
    except Exception as e:
        # Index may be unavailable; try a best-effort last-only query for freshness.
        index_err = str(e)
        cov_latest = {}
        questdb_info = {"ok": False, "table": tbl, "index_table": INDEX_TABLE_V2, "coverage_mode": "index", "error": index_err}
        try:
            from ghtrader.questdb.queries import query_contract_last_coverage

            try:
                win_days = int(os.environ.get("GHTRADER_CONTRACTS_QUESTDB_LAST_WINDOW_DAYS", "60") or "60")
            except Exception:
                win_days = 60
            win_days = max(1, min(int(win_days), 365))
            today = datetime.now(timezone.utc).date()
            recent_days = [(today - timedelta(days=i)).isoformat() for i in range(int(win_days))]
            cov_latest = query_contract_last_coverage(
                cfg=cfg,
                table=tbl,
                symbols=syms,
                dataset_version=lv,
                ticks_kind="raw",
                recent_days=recent_days,
            )
            questdb_info = {
                "ok": True,
                "table": tbl,
                "index_table": INDEX_TABLE_V2,
                "coverage_mode": "fallback_latest",
                "index_error": index_err,
                "index_empty": True,
            }
        except Exception:
            # Keep the original index error.
            pass
    t_db_latest1 = time.time()
    questdb_latest_ms = int((t_db_latest1 - t_db_latest0) * 1000)
    log.info(
        "contracts_snapshot_build.stage_done",
        stage="questdb_coverage",
        mode="index",
        ms=int(questdb_latest_ms),
        ok=bool(questdb_info.get("ok")),
        table=str(questdb_info.get("table") or ""),
        error=str(questdb_info.get("error") or ""),
        n_covered=int(len(cov_latest)),
    )

    # Merge last-only coverage with any cached full report (if present).
    full_cache_path = _questdb_cov_cache_path(table=tbl)
    full_cache = _read_json(full_cache_path)
    if full_cache and isinstance(full_cache.get("coverage"), dict):
        for k, vv in full_cache.get("coverage", {}).items():
            if not isinstance(k, str) or not isinstance(vv, dict):
                continue
            cov_full_cached[str(k)] = dict(vv)
        if cov_full_cached:
            questdb_info["full_cached_at"] = str(full_cache.get("cached_at") or "")
            questdb_info["full_cached_at_unix"] = float(full_cache.get("cached_at_unix") or 0.0)
            questdb_info["coverage_mode"] = "index+cached_full"

    # Fallback: reuse QuestDB coverage embedded in the previous snapshot (cheap; helps keep ranges visible
    # for inactive/expired symbols without re-querying all history).
    cov_prev_snapshot: dict[str, dict[str, Any]] = {}
    prev_snap = _read_json(_snapshot_path())
    if prev_snap and isinstance(prev_snap.get("contracts"), list):
        for rr in prev_snap.get("contracts") or []:
            if not isinstance(rr, dict):
                continue
            sym = str(rr.get("symbol") or "").strip()
            qc = rr.get("questdb_coverage")
            if sym and isinstance(qc, dict):
                cov_prev_snapshot[sym] = dict(qc)
        if cov_prev_snapshot and not cov_full_cached:
            try:
                questdb_info["prev_snapshot_cached_at"] = str(prev_snap.get("snapshot_cached_at") or prev_snap.get("local_checked_at") or "")
            except Exception:
                pass
            questdb_info["coverage_mode"] = "index+prev_snapshot"

    if bool(questdb_info.get("ok")):
        for sym in syms:
            latest = cov_latest.get(sym) or {}
            full = cov_full_cached.get(sym) or cov_prev_snapshot.get(sym) or {}
            merged = dict(full) if full else {}
            if not merged and latest:
                merged = dict(latest)
            else:
                for key in [
                    "last_tick_day",
                    "last_tick_ns",
                    "last_tick_ts",
                    "last_l5_day",
                    "last_l5_ns",
                    "last_l5_ts",
                ]:
                    if key in latest and latest.get(key) is not None:
                        merged[key] = latest.get(key)
            if merged:
                cov[sym] = merged

    # Precompute "today trading day" (no network).
    cal0 = []
    today_trading = None
    try:
        from ghtrader.data.trading_calendar import get_trading_calendar

        cal0 = get_trading_calendar(data_dir=dd, refresh=False, allow_download=False)
        today0 = datetime.now(timezone.utc).date()
        today_trading = _last_trading_day_leq(cal0, today0) if cal0 else None
        if today_trading is None:
            today_trading = today0 if today0.weekday() < 5 else (today0 - timedelta(days=1))
    except Exception:
        cal0 = []
        today_trading = datetime.now(timezone.utc).date()
    today_trading_s = today_trading.isoformat() if hasattr(today_trading, "isoformat") else str(today_trading)

    # Completeness vs expected (QuestDB index/no-data) (best-effort).
    comp_by_sym: dict[str, dict[str, Any]] = {}
    comp_info: dict[str, Any] = {"ok": False, "mode": "skipped"}
    log.info("contracts_snapshot_build.stage_start", stage="completeness", mode="index_no_data", n_symbols=int(len(syms)))
    t_comp0 = time.time()
    if bool(questdb_info.get("ok")):
        try:
            from ghtrader.data.completeness import compute_day_level_completeness

            comp_payload = compute_day_level_completeness(
                exchange=ex,
                variety=v,
                symbols=syms,
                contracts=None,
                start_override=None,
                end_override=None,
                refresh_catalog=False,
                allow_download_calendar=False,
                data_dir=dd,
                runs_dir=rd,
                cfg=cfg,
                include_day_sets=False,
            )
            for rr in comp_payload.get("contracts") or []:
                if isinstance(rr, dict) and str(rr.get("symbol") or "").strip():
                    comp_by_sym[str(rr.get("symbol") or "").strip()] = dict(rr)
            comp_info = {
                "ok": True,
                "mode": "index_no_data",
                "generated_at": comp_payload.get("generated_at"),
                "summary": dict(comp_payload.get("summary") or {}),
            }
        except Exception as e:
            comp_info = {"ok": False, "mode": "failed", "error": str(e)}
    else:
        comp_info = {"ok": False, "mode": "questdb_offline", "error": str(questdb_info.get("error") or "")}
    t_comp1 = time.time()
    comp_ms = int((t_comp1 - t_comp0) * 1000)
    try:
        comp_summary = comp_info.get("summary") if isinstance(comp_info, dict) else {}
        comp_summary = comp_summary if isinstance(comp_summary, dict) else {}
    except Exception:
        comp_summary = {}
    log.info(
        "contracts_snapshot_build.stage_done",
        stage="completeness",
        mode="index_no_data",
        ms=int(comp_ms),
        ok=bool(comp_info.get("ok")),
        symbols_with_window=int(comp_summary.get("symbols_with_window") or 0),
        symbols_with_missing=int(comp_summary.get("symbols_with_missing") or 0),
        missing_days=int(comp_summary.get("missing_days") or 0),
    )

    def _build_contract_rows(
        *,
        cov_by_symbol: dict[str, dict[str, Any]],
        comp_by_symbol: dict[str, dict[str, Any]],
        questdb_info_in: dict[str, Any],
    ) -> list[dict[str, Any]]:
        now_s = time.time()
        try:
            lag_min = float(
                os.environ.get(
                    "GHTRADER_CONTRACTS_DB_TICK_LAG_STALE_MINUTES",
                    os.environ.get("GHTRADER_CONTRACTS_LOCAL_TICK_LAG_STALE_MINUTES", "120"),
                )
                or "120"
            )
        except Exception:
            lag_min = 120.0
        lag_threshold_sec = float(max(0.0, lag_min * 60.0))

        def _parse_day(vv: Any) -> Any:
            if vv in (None, ""):
                return None
            try:
                s = str(vv).strip()
            except Exception:
                return None
            if not s:
                return None
            try:
                return datetime.fromisoformat(s[:10]).date()
            except Exception:
                return None

        rows: list[dict[str, Any]] = []
        for c in merged_contracts:
            sym = str((c or {}).get("symbol") or "").strip()
            if not sym:
                continue

            expired_v = (c or {}).get("expired")
            expired_b = bool(expired_v) if expired_v is not None else None

            qc_raw = cov_by_symbol.get(sym) if cov_by_symbol else None
            qc = dict(qc_raw) if isinstance(qc_raw, dict) else None
            if isinstance(qc, dict):
                qc.pop("present_dates", None)  # avoid bloating snapshots with per-day sets
            comp = comp_by_symbol.get(sym) if comp_by_symbol else None

            # Expected window (always compute; completeness may be unavailable when index is empty).
            expected_first = None
            expected_last = None
            if isinstance(comp, dict):
                expected_first = str(comp.get("expected_first") or "").strip() or None
                expected_last = str(comp.get("expected_last") or "").strip() or None
            if expected_first is None or expected_last is None:
                open_day = _parse_day((c or {}).get("open_date"))
                exp_day = _parse_day((c or {}).get("expire_datetime"))
                expired_v = (c or {}).get("expired")
                expired_b2 = bool(expired_v) if expired_v is not None else None

                ef = open_day
                if ef is None and isinstance(qc, dict):
                    ef = _parse_day(qc.get("first_tick_day"))
                el = None
                if expired_b2 is False:
                    try:
                        el = datetime.fromisoformat(today_trading_s).date()
                    except Exception:
                        el = None
                else:
                    if exp_day is not None:
                        el = _last_trading_day_leq(cal0, exp_day) if cal0 else exp_day
                    if el is None and isinstance(qc, dict):
                        el = _parse_day(qc.get("last_tick_day"))

                if expected_first is None and ef is not None:
                    expected_first = ef.isoformat()
                if expected_last is None and el is not None:
                    expected_last = el.isoformat()

            row: dict[str, Any] = {
                "symbol": sym,
                "catalog_source": (c or {}).get("catalog_source"),
                "expired": expired_b,
                "expire_datetime": (c or {}).get("expire_datetime"),
                "open_date": (c or {}).get("open_date"),
                "expected_first": expected_first,
                "expected_last": expected_last,
                "questdb_coverage": qc,
                "completeness": comp,
            }

            # Optional remote hint only (cached).
            pr = load_probe_result(symbol=sym, runs_dir=rd)
            if pr:
                row["tqsdk_probe"] = {
                    "probed_day": pr.get("probed_day"),
                    "probe_day_source": pr.get("probe_day_source"),
                    "ticks_rows": pr.get("ticks_rows"),
                    "l5_present": pr.get("l5_present"),
                    "error": pr.get("error"),
                    "updated_at": pr.get("updated_at"),
                }
            else:
                row["tqsdk_probe"] = None

            # Expected last day for active freshness checks.
            exp_last = None
            if isinstance(comp, dict):
                exp_last = str(comp.get("expected_last") or "").strip() or None
            if exp_last is None and expired_b is False:
                exp_last = today_trading_s

            # DB status for UI convenience.
            db_status = "unknown"
            if not bool(questdb_info_in.get("ok")):
                db_status = "error"
            else:
                last_day = str((qc or {}).get("last_tick_day") or "").strip() if isinstance(qc, dict) else ""
                if not last_day:
                    db_status = "empty"
                else:
                    if expired_b is False and exp_last and last_day < exp_last:
                        db_status = "stale"
                    else:
                        db_status = "ok"
            row["db_status"] = db_status

            # High-level status (QuestDB-first).
            status = "unknown"
            if isinstance(comp, dict) and comp.get("index_missing") is True:
                status = "unindexed"
                row["stale_reason"] = row.get("stale_reason") or "index_missing"
            miss_n = None
            if isinstance(comp, dict) and comp.get("missing_days") is not None:
                try:
                    miss_n = int(comp.get("missing_days") or 0)
                except Exception:
                    miss_n = None
            if status == "unindexed":
                pass
            elif miss_n is not None:
                status = "missing" if miss_n > 0 else "complete"
            else:
                # Coarse defaults when completeness isn't available yet (e.g., index not bootstrapped).
                if db_status == "empty":
                    status = "missing"
                else:
                    if expired_b is True:
                        status = "complete" if (isinstance(qc, dict) and str(qc.get("last_tick_day") or "").strip()) else "missing"
                    elif expired_b is False:
                        status = "complete"

            # Active freshness (minute-level).
            if expired_b is False and isinstance(qc, dict):
                last_ts = str(qc.get("last_tick_ts") or "").strip()
                if last_ts:
                    try:
                        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                        age_sec = float(max(0.0, now_s - float(last_dt.timestamp())))
                        row["last_tick_age_sec"] = age_sec
                        if age_sec > lag_threshold_sec:
                            status = "stale"
                            row["stale_reason"] = "tick_lag"
                    except Exception:
                        pass
                if db_status == "stale":
                    status = "stale"
                    row["stale_reason"] = row.get("stale_reason") or "missing_trading_day"

            row["status"] = status
            rows.append(row)
        return rows

    contracts_rows = _build_contract_rows(cov_by_symbol=cov, comp_by_symbol=comp_by_sym, questdb_info_in=questdb_info)

    status: dict[str, Any] = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "dataset_version": lv,
        "contracts": contracts_rows,
        "questdb": questdb_info,
        "catalog_ok": bool(catalog_ok),
        "catalog_error": catalog_error,
        "catalog_cached_at": catalog_cached_at,
        "catalog_source": catalog_source,
        "snapshot_cached_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_cached_at_unix": float(time.time()),
        "snapshot_refresh_catalog": bool(refresh_cat),
        "completeness": comp_info,
        "timings_ms": {
            "catalog": int((t_cat1 - t_cat0) * 1000),
            "questdb": int(questdb_latest_ms),
            "questdb_latest": int(questdb_latest_ms),
            "questdb_full": 0,
            "completeness": int(comp_ms),
            "total": int((time.time() - t0) * 1000),
        },
    }

    log.info("contracts_snapshot_build.stage_start", stage="write_snapshot", phase="fast")
    out_path = _snapshot_path()
    _write_json_atomic(out_path, status)
    log.info("contracts_snapshot_build.stage_done", stage="write_snapshot", phase="fast", path=str(out_path))

    # Optional: bootstrap/refresh the QuestDB symbol-day index by scanning the canonical ticks table (slow),
    # then refresh the snapshot.
    questdb_full_ms = 0
    if q_full:
        if not bool(questdb_info.get("ok")):
            log.warning(
                "contracts_snapshot_build.questdb_full_skipped",
                exchange=ex,
                var=v,
                table=str(questdb_info.get("table") or ""),
                error=str(questdb_info.get("error") or ""),
            )
        else:
            log.info("contracts_snapshot_build.stage_start", stage="questdb_index_bootstrap", n_symbols=int(len(syms)))
            t_db_full0 = time.time()
            hb_started = time.time()
            hb_stop = threading.Event()

            def _hb_full() -> None:
                while not hb_stop.wait(5.0):
                    try:
                        log.info(
                            "contracts_snapshot_build.heartbeat",
                            stage="questdb_index_bootstrap",
                            elapsed_s=int(time.time() - hb_started),
                            n_symbols=int(len(syms)),
                        )
                    except Exception:
                        pass

            threading.Thread(target=_hb_full, name="contracts-snapshot-heartbeat-full", daemon=True).start()

            boot: dict[str, Any] = {}
            boot_ok = False
            boot_err = ""
            try:
                boot = bootstrap_symbol_day_index_from_ticks(
                    cfg=cfg,
                    ticks_table=tbl,
                    symbols=syms,
                    dataset_version=lv,
                    ticks_kind="raw",
                    index_table=INDEX_TABLE_V2,
                )
                boot_ok = bool(boot.get("ok", False))
                boot_err = str(boot.get("error") or "") if not boot_ok else ""
            except Exception as e:
                boot = {"ok": False, "error": str(e)}
                boot_ok = False
                boot_err = str(e)
            finally:
                hb_stop.set()
            t_db_full1 = time.time()
            questdb_full_ms = int((t_db_full1 - t_db_full0) * 1000)
            log.info(
                "contracts_snapshot_build.stage_done",
                stage="questdb_index_bootstrap",
                ms=int(questdb_full_ms),
                ok=bool(boot_ok),
                error=str(boot_err),
                rows=int(boot.get("rows") or 0),
            )

            if boot_ok:
                cov_full = query_contract_coverage_from_index(cfg=cfg, symbols=syms, dataset_version=lv, ticks_kind="raw", index_table=INDEX_TABLE_V2)
                try:
                    cov_cache_path = _questdb_cov_cache_path(table=tbl)
                    payload = {
                        "ok": True,
                        "exchange": ex,
                        "var": v,
                        "dataset_version": lv,
                        "table": str(tbl),
                        "index_table": str(INDEX_TABLE_V2),
                        "index_bootstrap": boot,
                        "cached_at": datetime.now(timezone.utc).isoformat(),
                        "cached_at_unix": float(time.time()),
                        "coverage": cov_full,
                    }
                    _write_json_atomic(cov_cache_path, payload)
                    log.info("contracts_snapshot_build.questdb_full_cache_written", path=str(cov_cache_path), n_rows=int(len(cov_full)))
                except Exception as e:
                    log.warning("contracts_snapshot_build.questdb_full_cache_write_failed", error=str(e))

                cov_final = dict(cov_full)

                questdb_info2 = dict(questdb_info)
                questdb_info2["coverage_mode"] = "index+bootstrapped"
                questdb_info2["index_bootstrap_at"] = datetime.now(timezone.utc).isoformat()
                questdb_info2["index_bootstrap"] = boot

                # Recompute completeness now that the index is bootstrapped, then re-write snapshot.
                comp_by_sym2: dict[str, dict[str, Any]] = {}
                comp_info2: dict[str, Any] = {"ok": False, "mode": "skipped"}
                log.info("contracts_snapshot_build.stage_start", stage="completeness", phase="full", mode="index_no_data", n_symbols=int(len(syms)))
                t_comp2_0 = time.time()
                try:
                    from ghtrader.data.completeness import compute_day_level_completeness

                    comp_payload2 = compute_day_level_completeness(
                        exchange=ex,
                        variety=v,
                        symbols=syms,
                        contracts=None,
                        start_override=None,
                        end_override=None,
                        refresh_catalog=False,
                        allow_download_calendar=False,
                        data_dir=dd,
                        runs_dir=rd,
                        cfg=cfg,
                        include_day_sets=False,
                    )
                    for rr in comp_payload2.get("contracts") or []:
                        if isinstance(rr, dict) and str(rr.get("symbol") or "").strip():
                            comp_by_sym2[str(rr.get("symbol") or "").strip()] = dict(rr)
                    comp_info2 = {
                        "ok": True,
                        "mode": "index_no_data",
                        "generated_at": comp_payload2.get("generated_at"),
                        "summary": dict(comp_payload2.get("summary") or {}),
                    }
                except Exception as e:
                    comp_info2 = {"ok": False, "mode": "failed", "error": str(e)}
                t_comp2_1 = time.time()
                comp_ms2 = int((t_comp2_1 - t_comp2_0) * 1000)

                try:
                    comp_summary2 = comp_info2.get("summary") if isinstance(comp_info2, dict) else {}
                    comp_summary2 = comp_summary2 if isinstance(comp_summary2, dict) else {}
                except Exception:
                    comp_summary2 = {}
                log.info(
                    "contracts_snapshot_build.stage_done",
                    stage="completeness",
                    phase="full",
                    mode="index_no_data",
                    ms=int(comp_ms2),
                    ok=bool(comp_info2.get("ok")),
                    symbols_with_window=int(comp_summary2.get("symbols_with_window") or 0),
                    symbols_with_missing=int(comp_summary2.get("symbols_with_missing") or 0),
                    missing_days=int(comp_summary2.get("missing_days") or 0),
                )

                contracts_rows2 = _build_contract_rows(cov_by_symbol=cov_final, comp_by_symbol=comp_by_sym2, questdb_info_in=questdb_info2)

                status2: dict[str, Any] = {
                    "ok": True,
                    "exchange": ex,
                    "var": v,
                    "dataset_version": lv,
                    "contracts": contracts_rows2,
                    "questdb": questdb_info2,
                    "catalog_ok": bool(catalog_ok),
                    "catalog_error": catalog_error,
                    "catalog_cached_at": catalog_cached_at,
                    "catalog_source": catalog_source,
                    "snapshot_cached_at": datetime.now(timezone.utc).isoformat(),
                    "snapshot_cached_at_unix": float(time.time()),
                    "snapshot_refresh_catalog": bool(refresh_cat),
                    "completeness": comp_info2,
                    "timings_ms": {
                        "catalog": int((t_cat1 - t_cat0) * 1000),
                        "questdb": int(questdb_latest_ms + questdb_full_ms),
                        "questdb_latest": int(questdb_latest_ms),
                        "questdb_full": int(questdb_full_ms),
                        "completeness": int(comp_ms2),
                        "total": int((time.time() - t0) * 1000),
                    },
                }

                log.info("contracts_snapshot_build.stage_start", stage="write_snapshot", phase="full")
                out_path = _snapshot_path()
                _write_json_atomic(out_path, status2)
                log.info("contracts_snapshot_build.stage_done", stage="write_snapshot", phase="full", path=str(out_path))
                status = status2

    log.info(
        "contracts_snapshot_build.done",
        exchange=ex,
        var=v,
        refresh_catalog=bool(refresh_cat),
        questdb_full=bool(q_full),
        contracts=int(len(status.get("contracts") or [])),
        path=str(out_path),
        timings_ms=status.get("timings_ms"),
    )
    click.echo(
        json.dumps(
            {
                "ok": True,
                "exchange": ex,
                "var": v,
                "contracts": int(len(status.get("contracts") or [])),
                "snapshot_path": str(out_path),
                "timings_ms": status.get("timings_ms", {}),
            }
        )
    )

# ---------------------------------------------------------------------------
# data (QuestDB-first completeness tooling) - helper functions
# ---------------------------------------------------------------------------


from ghtrader.data.trading_calendar import last_trading_day_leq as _last_trading_day_leq


def _trading_days_between(cal: list[date], start: date, end: date) -> list[date]:
    from bisect import bisect_left, bisect_right

    if end < start:
        return []
    if not cal:
        # Fallback (weekday-only) is handled by trading_calendar.get_trading_days in callers if needed.
        return []
    i0 = bisect_left(cal, start)
    i1 = bisect_right(cal, end)
    return cal[i0:i1]


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
    from datetime import timedelta, timezone

    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import (
        INDEX_TABLE_V2,
        NO_DATA_TABLE_V2,
        ensure_index_tables,
        fetch_no_data_days_by_symbol,
        fetch_present_days_by_symbol,
        query_contract_coverage_from_index,
    )
    from ghtrader.tq.catalog import get_contract_catalog
    from ghtrader.data.trading_calendar import get_trading_calendar, get_trading_days

    ex = str(exchange).upper().strip() or "SHFE"
    v = str(variety).lower().strip() or "cu"
    lv = "v2"

    cat = None
    by_sym: dict[str, dict[str, Any]] = {}
    if not symbols:
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=bool(refresh_catalog))
        if not bool(cat.get("ok", False)):
            raise RuntimeError(str(cat.get("error") or "tqsdk_catalog_failed"))
        for r in cat.get("contracts") or []:
            sym = str((r or {}).get("symbol") or "").strip()
            if sym:
                by_sym[sym] = dict(r)
    else:
        # Best-effort: enrich provided symbols with cached catalog metadata when available.
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=False, allow_stale_cache=True, offline=True)
        catalog_by_sym: dict[str, dict[str, Any]] = {}
        for r in cat.get("contracts") or []:
            sym = str((r or {}).get("symbol") or "").strip()
            if sym:
                catalog_by_sym[sym] = dict(r)
        # Only include explicitly requested symbols, enriched with catalog metadata if available
        for s in symbols:
            s_clean = str(s).strip()
            if s_clean in catalog_by_sym:
                by_sym[s_clean] = catalog_by_sym[s_clean]
            else:
                by_sym.setdefault(s_clean, {"symbol": s_clean, "expired": None, "expire_datetime": None})

    syms = sorted({str(s).strip() for s in by_sym.keys() if str(s).strip()})
    if not syms:
        raise RuntimeError("no_symbols")

    cfg = make_questdb_query_config_from_env()
    ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, no_data_table=NO_DATA_TABLE_V2, connect_timeout_s=2)

    # Coverage bounds (fast; index-backed).
    cov = query_contract_coverage_from_index(cfg=cfg, symbols=syms, dataset_version=lv, ticks_kind="raw", index_table=INDEX_TABLE_V2)

    # Trading calendar (cached; may download holidays only when explicitly allowed).
    cal = get_trading_calendar(data_dir=data_dir, refresh=False, allow_download=bool(allow_download))
    today = date.today()
    today_trading = _last_trading_day_leq(cal, today) if cal else None
    if today_trading is None:
        # Weekday-only fallback.
        today_trading = today if today.weekday() < 5 else (today - timedelta(days=1))

    # First pass: compute expected windows to establish a global query window.
    windows: dict[str, tuple[date, date]] = {}
    skipped_symbols: list[dict[str, Any]] = []
    for sym in syms:
        c = by_sym.get(sym) or {}
        qc = cov.get(sym) or {}

        open_dt_s = str(c.get("open_date") or "").strip()
        exp_dt_s = str(c.get("expire_datetime") or "").strip()
        expired_b = c.get("expired")

        expected_first: date | None = None
        if open_dt_s:
            try:
                expected_first = date.fromisoformat(open_dt_s[:10])
            except Exception:
                expected_first = None
        if expected_first is None:
            try:
                db_first_s = str(qc.get("first_tick_day") or "").strip()
                expected_first = date.fromisoformat(db_first_s) if db_first_s else None
            except Exception:
                expected_first = None

        expected_last: date | None = None
        if expired_b is False:
            expected_last = today_trading
        else:
            expire_day: date | None = None
            if exp_dt_s:
                try:
                    expire_day = date.fromisoformat(exp_dt_s[:10])
                except Exception:
                    expire_day = None
            expected_last = _last_trading_day_leq(cal, expire_day) if (cal and expire_day) else (expire_day if expire_day else None)
            if expected_last is None:
                try:
                    db_last_s = str(qc.get("last_tick_day") or "").strip()
                    expected_last = date.fromisoformat(db_last_s) if db_last_s else None
                except Exception:
                    expected_last = None

        # Last-resort fallback: infer date range from contract symbol (SHFE YYMM convention)
        if expected_first is None or expected_last is None:
            from ghtrader.data.contract_status import infer_contract_date_range

            inferred_start, inferred_end = infer_contract_date_range(sym)
            if expected_first is None and inferred_start is not None:
                expected_first = inferred_start
            if expected_last is None and inferred_end is not None:
                # Clamp inferred end to today_trading if contract appears active
                if expired_b is False or (expired_b is None and inferred_end >= today_trading):
                    expected_last = today_trading
                else:
                    expected_last = _last_trading_day_leq(cal, inferred_end) if cal else inferred_end

        # Apply explicit override window.
        if start_override is not None:
            expected_first = start_override if expected_first is None else max(expected_first, start_override)
        if end_override is not None:
            expected_last = end_override if expected_last is None else min(expected_last, end_override)

        # Track skipped symbols with reason instead of silent continue
        if expected_first is None or expected_last is None or expected_last < expected_first:
            reason = "missing_date_range"
            if expected_first is None and expected_last is None:
                reason = "no_catalog_or_questdb_data"
            elif expected_first is None:
                reason = "missing_start_date"
            elif expected_last is None:
                reason = "missing_end_date"
            elif expected_last < expected_first:
                reason = "invalid_date_range"
            skipped_symbols.append({
                "symbol": sym,
                "reason": reason,
                "expected_first": expected_first.isoformat() if expected_first else None,
                "expected_last": expected_last.isoformat() if expected_last else None,
                "has_catalog_data": bool(open_dt_s or exp_dt_s),
                "has_questdb_data": bool(qc.get("first_tick_day") or qc.get("last_tick_day")),
            })
            continue
        windows[sym] = (expected_first, expected_last)

    if not windows:
        return {
            "ok": True,
            "exchange": ex,
            "var": v,
            "dataset_version": lv,
            "index_table": INDEX_TABLE_V2,
            "no_data_table": NO_DATA_TABLE_V2,
            "symbols": syms,
            "skipped_symbols": skipped_symbols,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "contracts": [],
            "summary": {
                # Legacy/CLI summary fields
                "symbols": int(len(syms)),
                "symbols_with_window": 0,
                "symbols_with_missing": 0,
                "missing_days": 0,
                "symbols_skipped": len(skipped_symbols),
                # UI/workflow fields
                "symbols_total": int(len(syms)),
                "symbols_indexed": 0,
                "symbols_unindexed": 0,
                "index_healthy": False,
                "bootstrap_recommended": True,
                "symbols_complete": 0,
                "symbols_missing": 0,
            },
        }

    g0 = min(w[0] for w in windows.values())
    g1 = max(w[1] for w in windows.values())

    present_by = fetch_present_days_by_symbol(
        cfg=cfg, symbols=list(windows.keys()), start_day=g0, end_day=g1, dataset_version=lv, ticks_kind="raw", index_table=INDEX_TABLE_V2
    )
    no_data_by = fetch_no_data_days_by_symbol(
        cfg=cfg, symbols=list(windows.keys()), start_day=g0, end_day=g1, dataset_version=lv, ticks_kind="raw", no_data_table=NO_DATA_TABLE_V2
    )

    rows_out: list[dict[str, Any]] = []
    missing_total = 0
    symbols_with_missing = 0
    for sym in sorted(windows.keys()):
        w0, w1 = windows[sym]
        exp_days = _trading_days_between(cal, w0, w1) if cal else get_trading_days(market=ex, start=w0, end=w1, data_dir=data_dir, refresh=False)
        exp_set = set(exp_days)
        present = {d for d in (present_by.get(sym) or set()) if w0 <= d <= w1}
        ndays = {d for d in (no_data_by.get(sym) or set()) if w0 <= d <= w1}
        # Treat no-data markers as effective only when the day is not already present.
        ndays_eff = set(ndays) - set(present)
        # For active contracts, do not treat "today trading day" as no-data (it is often not final).
        try:
            c_meta = by_sym.get(sym) or {}
            is_active = c_meta.get("expired") is False
        except Exception:
            is_active = False
        if is_active:
            try:
                ndays_eff.discard(today_trading)
            except Exception:
                pass
        qc_raw = cov.get(sym)
        qc = dict(qc_raw) if isinstance(qc_raw, dict) else {}
        qc.pop("present_dates", None)  # never include bulky per-day sets in reports/snapshots

        # If the symbol-day index doesn't cover this symbol yet, we cannot compute completeness safely.
        # Mark as index_missing instead of treating all days as missing (prevents destructive repair/health runs).
        index_missing = not bool(qc)
        if index_missing:
            skipped_symbols.append(
                {
                    "symbol": sym,
                    "reason": "index_missing",
                    "expected_first": w0.isoformat(),
                    "expected_last": w1.isoformat(),
                }
            )
            rows_out.append(
                {
                    "symbol": sym,
                    "expected_first": w0.isoformat(),
                    "expected_last": w1.isoformat(),
                    "expected_days": int(len(exp_set)),
                    "present_days": None,
                    "no_data_days": int(len(ndays_eff)),
                    "missing_days": None,
                    "missing_first": None,
                    "missing_last": None,
                    "missing_sample": [],
                    "index_missing": True,
                    "questdb_coverage": qc,
                }
            )
            continue

        missing = sorted(exp_set - present - ndays_eff)
        miss_n = int(len(missing))
        missing_total += miss_n
        if miss_n > 0:
            symbols_with_missing += 1
        rows_out.append(
            {
                "symbol": sym,
                "expected_first": w0.isoformat(),
                "expected_last": w1.isoformat(),
                "expected_days": int(len(exp_set)),
                "present_days": int(len(present)),
                "no_data_days": int(len(ndays_eff)),
                "missing_days": miss_n,
                "missing_first": (missing[0].isoformat() if missing else None),
                "missing_last": (missing[-1].isoformat() if missing else None),
                "missing_sample": [d.isoformat() for d in missing[:20]],
                "index_missing": False,
                "questdb_coverage": qc,
            }
        )

    # Summary fields used by the dashboard UI (Data Hub workflow).
    symbols_total = int(len(syms))
    symbols_with_window = int(len(windows))
    symbols_unindexed = 0
    symbols_complete = 0
    symbols_missing = 0
    for rr in rows_out:
        if rr.get("index_missing") is True:
            symbols_unindexed += 1
            symbols_missing += 1
            continue
        md = rr.get("missing_days")
        try:
            miss_n = int(md) if md is not None else 0
        except Exception:
            miss_n = 0
        if miss_n > 0:
            symbols_missing += 1
        else:
            symbols_complete += 1
    symbols_indexed = int(max(0, symbols_with_window - symbols_unindexed))
    index_healthy = bool(symbols_unindexed == 0)
    bootstrap_recommended = bool(symbols_unindexed > 0)

    return {
        "ok": True,
        "exchange": ex,
        "var": v,
        "dataset_version": lv,
        "index_table": INDEX_TABLE_V2,
        "no_data_table": NO_DATA_TABLE_V2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog": {"ok": bool(cat and cat.get("ok")), "cached_at": (cat.get("cached_at") if isinstance(cat, dict) else None), "source": (cat.get("source") if isinstance(cat, dict) else None)},
        "symbols": syms,
        "skipped_symbols": skipped_symbols,
        "contracts": rows_out,
        "summary": {
            # Legacy/CLI summary fields
            "symbols": symbols_total,
            "symbols_with_window": symbols_with_window,
            "symbols_with_missing": int(symbols_with_missing),
            "missing_days": int(missing_total),
            "symbols_skipped": len(skipped_symbols),
            # UI/workflow fields
            "symbols_total": symbols_total,
            "symbols_indexed": symbols_indexed,
            "symbols_unindexed": int(symbols_unindexed),
            "index_healthy": bool(index_healthy),
            "bootstrap_recommended": bool(bootstrap_recommended),
            "symbols_complete": int(symbols_complete),
            "symbols_missing": int(symbols_missing),
        },
    }


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
    """
    Diagnose data integrity and quality (QuestDB-first).

    Writes a report under runs/control/reports/data_diagnose/.
    """
    import json

    _ = ctx
    ex = str(exchange).upper().strip() or "SHFE"
    v = str(variety).lower().strip() or "cu"
    dd = Path(data_dir)
    rd = Path(runs_dir)
    syms = [str(s).strip() for s in symbols if str(s).strip()]
    s0 = start.date() if start else None
    s1 = end.date() if end else None
    refresh_cat = bool(int(refresh_catalog or 0))

    from ghtrader.data.diagnose import run_diagnose, write_diagnose_report

    rep = run_diagnose(
        exchange=ex,
        var=v,
        symbols=syms if syms else None,
        thoroughness=str(thoroughness),
        check_workers=int(check_workers),
        force_full_scan=bool(force_full_scan),
        start=s0,
        end=s1,
        refresh_catalog=bool(refresh_cat),
        allow_download_calendar=True,
        data_dir=dd,
        runs_dir=rd,
    )
    out_path = write_diagnose_report(report=rep, runs_dir=rd)

    summary = dict(rep.summary or {})
    summary["ok"] = True
    summary["report_path"] = str(out_path)
    summary["run_id"] = str(rep.run_id)
    click.echo(json.dumps(summary, ensure_ascii=False, indent=2, default=str, sort_keys=True))
    if as_json:
        click.echo(json.dumps(rep.to_dict(), ensure_ascii=False, indent=2, default=str, sort_keys=True))


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
    """
    Execute a repair plan generated from a diagnose report.
    """
    import json

    _ = ctx
    dd = Path(data_dir)
    rd = Path(runs_dir)
    rp = Path(str(report_path)).expanduser().resolve()
    if not rp.exists():
        raise click.ClickException(f"Report not found: {rp}")

    from ghtrader.data.repair import execute_repair_plan, generate_repair_plan, load_diagnose_report

    rep = load_diagnose_report(rp)
    plan = generate_repair_plan(report=rep, include_refresh_catalog=bool(int(refresh_catalog or 0)), chunk_days=int(chunk_days))
    res = execute_repair_plan(
        plan=plan,
        dry_run=bool(dry_run),
        auto_only=bool(auto_only),
        data_dir=dd,
        runs_dir=rd,
        download_workers=int(download_workers),
    )
    out = {"ok": bool(res.ok), "run_id": str(res.run_id), "actions": res.actions}
    click.echo(json.dumps(out, ensure_ascii=False, indent=2, default=str, sort_keys=True))
    if not bool(res.ok):
        raise SystemExit(1)


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
    import json
    from datetime import timezone

    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import INDEX_TABLE_V2, ensure_index_tables, query_contract_coverage_from_index
    from ghtrader.tq.catalog import get_contract_catalog

    _ = ctx
    log = structlog.get_logger()

    ex = str(exchange).upper().strip() or "SHFE"
    v = str(variety).lower().strip() or "cu"
    dd = Path(data_dir)
    rd = Path(runs_dir)
    refresh_cat = bool(int(refresh_catalog or 0))
    syms_in = [str(s).strip() for s in symbols if str(s).strip()]

    if syms_in:
        syms = syms_in
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=rd, refresh=False, allow_stale_cache=True, offline=True)
    else:
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=rd, refresh=bool(refresh_cat))
        if not bool(cat.get("ok", False)):
            raise RuntimeError(str(cat.get("error") or "tqsdk_catalog_failed"))
        syms = [str(r.get("symbol") or "").strip() for r in (cat.get("contracts") or []) if str(r.get("symbol") or "").strip()]

    if not syms:
        raise RuntimeError("no_symbols")

    cfg = make_questdb_query_config_from_env()
    ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
    cov = query_contract_coverage_from_index(cfg=cfg, symbols=syms, dataset_version="v2", ticks_kind="raw", index_table=INDEX_TABLE_V2)

    rows: list[dict[str, Any]] = []
    firsts: list[str] = []
    for sym in sorted(syms):
        qc = cov.get(sym) or {}
        f = str(qc.get("first_l5_day") or "").strip() or None
        l = str(qc.get("last_l5_day") or "").strip() or None
        n = qc.get("l5_days")
        if f:
            firsts.append(f)
        rows.append({"symbol": sym, "first_l5_day": f, "last_l5_day": l, "l5_days": n})

    variety_first = min(firsts) if firsts else None
    payload = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "dataset_version": "v2",
        "index_table": INDEX_TABLE_V2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog": {"ok": bool(cat.get("ok", False)), "cached_at": cat.get("cached_at"), "source": cat.get("source")},
        "variety_first_l5_day": variety_first,
        "symbols_with_l5": int(len([r for r in rows if r.get("first_l5_day")])),
        "symbols": int(len(rows)),
        "rows": rows,
    }

    out_dir = rd / "control" / "reports" / "l5_start"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    out_path = out_dir / f"l5_start_exchange={ex}_var={v}_{run_id}.json"
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(out_path)

    summary = {
        "ok": True,
        "exchange": ex,
        "var": v,
        "variety_first_l5_day": variety_first,
        "symbols": int(len(rows)),
        "symbols_with_l5": int(payload.get("symbols_with_l5") or 0),
        "report_path": str(out_path),
    }
    log.info("data.l5_start.done", **summary)
    click.echo(json.dumps(summary, ensure_ascii=False, indent=2, default=str, sort_keys=True))
    if as_json:
        click.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str, sort_keys=True))

# ---------------------------------------------------------------------------
# probe-l5 (sample-based TqSdk L5 availability probe)
# ---------------------------------------------------------------------------


@main.command("probe-l5")
@click.option("--symbol", "-s", "symbols", required=True, multiple=True, help="Symbol(s) to probe (e.g., SHFE.cu2003)")
@click.option("--data-dir", default="data", help="Data directory root (for calendar/active-ranges cache)")
@click.option(
    "--probe-date",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Optional probe date (YYYY-MM-DD). If omitted, uses local last day if available; else quote expire (expired contracts); else latest trading day.",
)
@click.option("--json", "as_json", is_flag=True, help="Print JSON results to stdout")
@click.pass_context
def probe_l5(ctx: click.Context, symbols: tuple[str, ...], data_dir: str, probe_date: datetime | None, as_json: bool) -> None:
    """
    Probe TqSdk tick series for a symbol/day to infer whether L5 depth is present.

    Results are written under: runs/control/cache/tqsdk_l5_probe/
    """
    import json

    from ghtrader.tq.l5_probe import probe_l5_for_symbol

    log = structlog.get_logger()
    sym_list = [str(s).strip() for s in symbols if str(s).strip()]
    if not sym_list:
        raise click.ClickException("At least one --symbol is required")

    # Serialize with per-symbol locks so probes don't race downloads for the same symbol.
    _acquire_locks([f"ticks:symbol={s}" for s in sym_list])

    runs_dir = get_runs_dir()
    dd = Path(data_dir)
    pd_day = probe_date.date() if probe_date else None

    results: list[dict[str, Any]] = []
    for sym in sym_list:
        log.info("probe_l5.start", symbol=sym, probe_day=str(pd_day) if pd_day else "auto")
        res = probe_l5_for_symbol(symbol=sym, data_dir=dd, runs_dir=runs_dir, probe_day=pd_day)
        results.append(res)
        log.info(
            "probe_l5.done",
            symbol=sym,
            probed_day=res.get("probed_day"),
            ticks_rows=res.get("ticks_rows"),
            l5_present=res.get("l5_present"),
            error=res.get("error"),
        )

    if as_json:
        click.echo(json.dumps({"results": results}, ensure_ascii=False, indent=2, sort_keys=True))


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
    from ghtrader.tq.ingest import run_live_recorder

    log = structlog.get_logger()
    _acquire_locks([f"ticks:symbol={s}" for s in symbols])
    log.info("record.start", symbols=symbols)
    lv = "v2"
    run_live_recorder(symbols=list(symbols), data_dir=Path(data_dir), dataset_version=lv)  # type: ignore[arg-type]


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
# main-schedule (build SHFE main roll schedule from ticks; QuestDB-backed)
# ---------------------------------------------------------------------------

@main.command("main-schedule")
@click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
@click.option("--threshold", default=1.1, type=float, show_default=True, help="Switch threshold (OI multiplier)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.pass_context
def main_schedule(
    ctx: click.Context,
    variety: str,
    start: datetime,
    end: datetime,
    threshold: float,
    data_dir: str,
) -> None:
    """Build a main-contract roll schedule (date -> underlying contract)."""
    from ghtrader.data.main_schedule import build_shfe_main_schedule

    log = structlog.get_logger()
    _acquire_locks([f"main_schedule:var={variety.lower()}"])
    res = build_shfe_main_schedule(
        var=variety,
        start=start.date(),
        end=end.date(),
        rule_threshold=threshold,
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
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing derived dataset")
@click.pass_context
def main_l5(
    ctx: click.Context,
    variety: str,
    derived_symbol: str,
    overwrite: bool,
) -> None:
    """Build derived main_l5 ticks for the L5 era only (QuestDB-backed detection)."""
    from ghtrader.data.main_l5 import build_main_l5_l5_era_only

    log = structlog.get_logger()
    var_l = variety.lower().strip()
    ds = (derived_symbol or "").strip() or f"KQ.m@SHFE.{var_l}"
    _acquire_locks([f"main_l5:symbol={ds}"])
    res = build_main_l5_l5_era_only(
        var=var_l,
        derived_symbol=ds,
        exchange="SHFE",
        overwrite=bool(overwrite),
    )
    log.info(
        "main_l5.done",
        derived_symbol=res.derived_symbol,
        dataset_version=res.dataset_version,
        l5_start_date=res.l5_start_date.isoformat(),
        schedule_hash=str(res.schedule_hash),
        schedule_rows_used=int(res.schedule_rows_used),
        rows_total=int(res.materialization.rows_total),
        days_processed=int(res.materialization.days_processed),
        days_skipped=int(res.materialization.days_skipped),
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
