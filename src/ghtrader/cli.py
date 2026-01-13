"""
ghTrader CLI: unified entrypoint for all operations.

Subcommands:
- download: fetch historical L5 ticks via TqSdk Pro (tq_dl)
- record: run live tick recorder
- build: generate features and labels from Parquet lake
- train: train models (baseline or deep)
- backtest: run TqSdk backtest harness
- paper: run paper-trading loop with online calibrator
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
import uuid
from datetime import date, datetime
from pathlib import Path

import click
import structlog

from ghtrader.config import get_runs_dir, load_config

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_path = os.environ.get("GHTRADER_JOB_LOG_PATH", "").strip()
    if log_path:
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
@click.option(
    "--lake-version",
    default=None,
    type=click.Choice(["v1", "v2"]),
    help="Tick lake version to use (default: env GHTRADER_LAKE_VERSION or v1)",
)
@click.pass_context
def download(ctx: click.Context, symbol: str, start: datetime, end: datetime,
             data_dir: str, chunk_days: int, lake_version: str | None) -> None:
    """Download historical L5 ticks for a symbol and write to Parquet lake."""
    from ghtrader.tq_ingest import download_historical_ticks
    from ghtrader.config import get_lake_version

    log = structlog.get_logger()
    _acquire_locks([f"ticks:symbol={symbol}"])
    log.info("download.start", symbol=symbol, start=start.date(), end=end.date())
    lv = lake_version or get_lake_version()
    download_historical_ticks(
        symbol=symbol,
        start_date=start.date(),
        end_date=end.date(),
        data_dir=Path(data_dir),
        chunk_days=int(chunk_days),
        lake_version=lv,  # type: ignore[arg-type]
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
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--chunk-days", default=5, type=int, help="Days per download chunk")
@click.option("--refresh-akshare/--no-refresh-akshare", default=False, show_default=True, help="Refresh cached akshare daily data")
@click.option(
    "--lake-version",
    default=None,
    type=click.Choice(["v1", "v2"]),
    help="Tick lake version to use (default: env GHTRADER_LAKE_VERSION or v1)",
)
@click.pass_context
def download_contract_range(
    ctx: click.Context,
    exchange: str,
    variety: str,
    start_contract: str,
    end_contract: str,
    data_dir: str,
    chunk_days: int,
    refresh_akshare: bool,
    lake_version: str | None,
) -> None:
    """Exhaustively backfill L5 ticks for a YYMM contract range (using akshare-inferred active ranges)."""
    from ghtrader.tq_ingest import download_contract_range as _download_contract_range
    from ghtrader.config import get_lake_version

    log = structlog.get_logger()
    _acquire_locks([f"ticks_range:exchange={exchange},var={variety.lower()}"])
    log.info(
        "download_contract_range.start",
        exchange=exchange,
        var=variety,
        start_contract=start_contract,
        end_contract=end_contract,
        chunk_days=chunk_days,
        refresh_akshare=refresh_akshare,
    )

    _download_contract_range(
        exchange=exchange,
        var=variety,
        start_contract=start_contract,
        end_contract=end_contract,
        data_dir=Path(data_dir),
        chunk_days=chunk_days,
        refresh_akshare=refresh_akshare,
        lake_version=(lake_version or get_lake_version()),  # type: ignore[arg-type]
    )
    log.info("download_contract_range.done")


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbols", "-s", required=True, multiple=True,
              help="Symbols to record (can specify multiple)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option(
    "--lake-version",
    default=None,
    type=click.Choice(["v1", "v2"]),
    help="Tick lake version to use (default: env GHTRADER_LAKE_VERSION or v1)",
)
@click.pass_context
def record(ctx: click.Context, symbols: tuple[str, ...], data_dir: str, lake_version: str | None) -> None:
    """Run live tick recorder (subscribes and appends to Parquet lake)."""
    from ghtrader.tq_ingest import run_live_recorder
    from ghtrader.config import get_lake_version

    log = structlog.get_logger()
    _acquire_locks([f"ticks:symbol={s}" for s in symbols])
    log.info("record.start", symbols=symbols)
    run_live_recorder(symbols=list(symbols), data_dir=Path(data_dir), lake_version=(lake_version or get_lake_version()))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbol", "-s", required=True, help="Symbol to build features for")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--horizons", default="10,50,200", help="Comma-separated label horizons (ticks)")
@click.option("--threshold-k", default=1, type=int, help="Label threshold in price ticks")
@click.option(
    "--ticks-lake",
    default="raw",
    type=click.Choice(["raw", "main_l5"]),
    show_default=True,
    help="Which ticks lake to read from (raw ticks vs derived main-with-depth ticks).",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    show_default=True,
    help="Overwrite existing features/labels outputs for this symbol (full rebuild). Default is incremental/resume.",
)
@click.option(
    "--lake-version",
    default=None,
    type=click.Choice(["v1", "v2"]),
    help="Tick lake version to use (default: env GHTRADER_LAKE_VERSION or v1)",
)
@click.pass_context
def build(ctx: click.Context, symbol: str, data_dir: str, horizons: str,
          threshold_k: int, ticks_lake: str, overwrite: bool, lake_version: str | None) -> None:
    """Build features and labels from Parquet lake."""
    from ghtrader.features import FactorEngine
    from ghtrader.labels import build_labels_for_symbol
    from ghtrader.config import get_lake_version

    log = structlog.get_logger()
    _acquire_locks([f"build:symbol={symbol},ticks_lake={ticks_lake}"])
    horizon_list = [int(h.strip()) for h in horizons.split(",")]
    log.info(
        "build.start",
        symbol=symbol,
        horizons=horizon_list,
        threshold_k=threshold_k,
        ticks_lake=ticks_lake,
        overwrite=overwrite,
    )

    # Build labels
    build_labels_for_symbol(
        symbol=symbol,
        data_dir=Path(data_dir),
        horizons=horizon_list,
        threshold_k=threshold_k,
        ticks_lake=ticks_lake,  # type: ignore[arg-type]
        overwrite=overwrite,
        lake_version=(lake_version or get_lake_version()),  # type: ignore[arg-type]
    )

    # Build features
    engine = FactorEngine()
    engine.build_features_for_symbol(
        symbol=symbol,
        data_dir=Path(data_dir),
        ticks_lake=ticks_lake,  # type: ignore[arg-type]
        overwrite=overwrite,
        lake_version=(lake_version or get_lake_version()),  # type: ignore[arg-type]
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
    from ghtrader.models import train_model

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
    from ghtrader.eval import run_backtest

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
    from ghtrader.online import run_paper_trading

    log = structlog.get_logger()
    log.info("paper.start", model=model, symbols=symbols)
    run_paper_trading(
        model_name=model,
        symbols=list(symbols),
        artifacts_dir=Path(artifacts_dir),
    )


# ---------------------------------------------------------------------------
# trade (paper/sim/live; subprocess-friendly)
# ---------------------------------------------------------------------------

@main.command()
@click.option(
    "--mode",
    default="paper",
    show_default=True,
    type=click.Choice(["paper", "sim", "live"]),
    help="Trading mode (paper=no orders, sim=simulated orders, live=real account; safety-gated).",
)
@click.option(
    "--sim-account",
    default="tqsim",
    show_default=True,
    type=click.Choice(["tqsim", "tqkq"]),
    help="Sim account type for paper/sim modes.",
)
@click.option(
    "--executor",
    default="targetpos",
    show_default=True,
    type=click.Choice(["targetpos", "direct"]),
    help="Execution style: TargetPosTask vs direct insert/cancel.",
)
@click.option(
    "--model",
    "-m",
    required=True,
    type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
    help="Model type to use for signals",
)
@click.option("--symbols", "-s", required=True, multiple=True, help="Symbols to trade (can specify multiple)")
@click.option("--data-dir", default="data", help="Data directory root (for schedule resolution)")
@click.option("--artifacts-dir", default="artifacts", help="Artifacts directory (for model loading)")
@click.option("--runs-dir", default="runs", help="Runs directory (snapshots/events under runs/trading/)")
@click.option("--horizon", default=50, type=int, help="Label horizon the model was trained on")
@click.option("--threshold-up", default=0.6, type=float, help="Probability threshold for long signal")
@click.option("--threshold-down", default=0.6, type=float, help="Probability threshold for short signal")
@click.option("--position-size", default=1, type=int, help="Lots per signal (±position_size)")
@click.option("--max-position", default=1, type=int, help="Max absolute net position per symbol")
@click.option("--max-order-size", default=1, type=int, help="Max lots per order (direct executor splits)")
@click.option("--max-ops-per-sec", default=10, type=int, help="Max order ops per second (insert/cancel)")
@click.option("--max-daily-loss", default=None, type=float, help="Kill-switch: max absolute loss from start balance")
@click.option(
    "--enforce-trading-time/--no-enforce-trading-time",
    default=True,
    show_default=True,
    help="Best-effort: avoid orders outside trading session",
)
@click.option("--tp-price", default="ACTIVE", show_default=True, type=click.Choice(["ACTIVE", "PASSIVE"]), help="TargetPosTask price mode")
@click.option("--tp-offset-priority", default="今昨,开", show_default=True, type=str, help="TargetPosTask offset priority (e.g. 今昨,开)")
@click.option("--direct-price-mode", default="ACTIVE", show_default=True, type=click.Choice(["ACTIVE", "PASSIVE"]), help="Direct executor price mode")
@click.option(
    "--direct-advanced",
    default="",
    show_default=True,
    type=click.Choice(["", "FAK", "FOK"]),
    help='Direct executor advanced order type ("FAK"/"FOK"; empty means none)',
)
@click.option(
    "--monitor-only/--no-monitor-only",
    default=False,
    show_default=True,
    help="Connect and record snapshots/events but never send orders (safe for real-account validation).",
)
@click.option(
    "--require-no-alive-orders",
    default="auto",
    show_default=True,
    type=click.Choice(["auto", "true", "false"]),
    help="Preflight: refuse to start if there are any ALIVE orders (auto=true for live order routing).",
)
@click.option(
    "--require-flat-start",
    default="auto",
    show_default=True,
    type=click.Choice(["auto", "true", "false"]),
    help="Preflight: require net position == 0 at startup (auto=true for live order routing).",
)
@click.option("--confirm-live", default="", type=str, help="Required for live: set to I_UNDERSTAND")
@click.option(
    "--snapshot-interval-sec",
    default=10.0,
    type=float,
    show_default=True,
    help="Account snapshot interval (seconds)",
)
@click.pass_context
def trade(
    ctx: click.Context,
    mode: str,
    sim_account: str,
    executor: str,
    model: str,
    symbols: tuple[str, ...],
    data_dir: str,
    artifacts_dir: str,
    runs_dir: str,
    horizon: int,
    threshold_up: float,
    threshold_down: float,
    position_size: int,
    max_position: int,
    max_order_size: int,
    max_ops_per_sec: int,
    max_daily_loss: float | None,
    enforce_trading_time: bool,
    tp_price: str,
    tp_offset_priority: str,
    direct_price_mode: str,
    direct_advanced: str,
    monitor_only: bool,
    require_no_alive_orders: str,
    require_flat_start: str,
    confirm_live: str,
    snapshot_interval_sec: float,
) -> None:
    """Run a trading job (paper/sim/live) suitable for dashboard subprocess execution."""
    from ghtrader.execution import RiskLimits
    from ghtrader.trade import TradeConfig, run_trade

    lock_keys = [f"trade:mode={mode}"] + [f"trade:symbol={s}" for s in symbols]
    _acquire_locks(lock_keys)

    limits = RiskLimits(
        max_abs_position=int(max_position),
        max_order_size=int(max_order_size),
        max_ops_per_sec=int(max_ops_per_sec),
        max_daily_loss=max_daily_loss,
        enforce_trading_time=bool(enforce_trading_time),
    )

    def _tri(x: str) -> bool | None:
        x = str(x or "auto").strip().lower()
        if x == "auto":
            return None
        if x == "true":
            return True
        if x == "false":
            return False
        return None

    cfg = TradeConfig(
        mode=mode,  # type: ignore[arg-type]
        monitor_only=bool(monitor_only),
        sim_account=sim_account,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        model_name=model,
        symbols=list(symbols),
        horizon=int(horizon),
        threshold_up=float(threshold_up),
        threshold_down=float(threshold_down),
        position_size=int(position_size),
        data_dir=Path(data_dir),
        artifacts_dir=Path(artifacts_dir),
        runs_dir=Path(runs_dir),
        require_no_alive_orders=_tri(require_no_alive_orders),
        require_flat_start=_tri(require_flat_start),
        limits=limits,
        targetpos_price=tp_price,
        targetpos_offset_priority=tp_offset_priority,
        direct_price_mode=direct_price_mode,  # type: ignore[arg-type]
        direct_advanced=direct_advanced or None,
        snapshot_interval_sec=float(snapshot_interval_sec),
    )
    run_trade(cfg, confirm_live=confirm_live or None)


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
    from ghtrader.benchmark import run_benchmark

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
    from ghtrader.benchmark import compare_models

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
    from ghtrader.pipeline import run_daily_pipeline

    log = structlog.get_logger()
    lock_keys: list[str] = []
    for s in symbols:
        lock_keys.append(f"ticks:symbol={s}")
        lock_keys.append(f"build:symbol={s},ticks_lake=raw")
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
    from ghtrader.pipeline import run_hyperparam_sweep

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
# main-schedule (build SHFE main roll schedule from akshare daily OI)
# ---------------------------------------------------------------------------

@main.command("main-schedule")
@click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
@click.option("--threshold", default=1.1, type=float, show_default=True, help="Switch threshold (OI multiplier)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--refresh-akshare/--no-refresh-akshare", default=False, show_default=True, help="Refresh cached akshare daily data")
@click.pass_context
def main_schedule(
    ctx: click.Context,
    variety: str,
    start: datetime,
    end: datetime,
    threshold: float,
    data_dir: str,
    refresh_akshare: bool,
) -> None:
    """Build a main-contract roll schedule (date -> underlying contract)."""
    from ghtrader.main_contract import build_shfe_main_schedule

    log = structlog.get_logger()
    _acquire_locks([f"main_schedule:var={variety.lower()}"])
    res = build_shfe_main_schedule(
        var=variety,
        start=start.date(),
        end=end.date(),
        rule_threshold=threshold,
        data_dir=Path(data_dir),
        refresh_akshare=refresh_akshare,
    )
    log.info(
        "main_schedule.done",
        schedule_path=str(res.schedule_path),
        manifest_path=str(res.manifest_path),
        schedule_hash=res.schedule_hash,
        rows=len(res.schedule),
    )


# ---------------------------------------------------------------------------
# main-depth (materialize derived main-with-depth ticks)
# ---------------------------------------------------------------------------

@main.command("main-depth")
@click.option("--symbol", "derived_symbol", required=True, help="Derived symbol (e.g., KQ.m@SHFE.cu)")
@click.option("--schedule-path", required=True, type=str, help="Path to schedule.parquet")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing derived dataset")
@click.option(
    "--lake-version",
    default=None,
    type=click.Choice(["v1", "v2"]),
    help="Tick lake version to use (default: env GHTRADER_LAKE_VERSION or v1)",
)
@click.pass_context
def main_depth(
    ctx: click.Context,
    derived_symbol: str,
    schedule_path: str,
    data_dir: str,
    overwrite: bool,
    lake_version: str | None,
) -> None:
    """Materialize a derived main-with-depth dataset from a schedule and raw contract ticks."""
    from ghtrader.main_depth import materialize_main_with_depth
    from ghtrader.config import get_lake_version

    log = structlog.get_logger()
    _acquire_locks([f"main_depth:symbol={derived_symbol}"])
    res = materialize_main_with_depth(
        derived_symbol=derived_symbol,
        schedule_path=Path(schedule_path),
        data_dir=Path(data_dir),
        overwrite=overwrite,
        lake_version=(lake_version or get_lake_version()),  # type: ignore[arg-type]
    )
    log.info(
        "main_depth.done",
        derived_root=str(res.derived_root),
        manifest_path=str(res.manifest_path),
        schedule_hash=res.schedule_hash,
        rows_total=res.rows_total,
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
    type=click.Choice(["all", "ticks", "main_l5", "features", "labels"]),
    help="What to audit (can pass multiple).",
)
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs output directory")
@click.option(
    "--lake-version",
    default=None,
    type=click.Choice(["v1", "v2"]),
    help="Tick lake version to audit (default: env GHTRADER_LAKE_VERSION or v1)",
)
@click.pass_context
def audit(ctx: click.Context, scopes: tuple[str, ...], data_dir: str, runs_dir: str, lake_version: str | None) -> None:
    """Audit data integrity and write a JSON report under runs/audit/."""
    from ghtrader.audit import run_audit
    from ghtrader.config import get_lake_version

    log = structlog.get_logger()
    out_path, report = run_audit(
        data_dir=Path(data_dir),
        runs_dir=Path(runs_dir),
        scopes=list(scopes),
        lake_version=(lake_version or get_lake_version()),  # type: ignore[arg-type]
    )
    log.info("audit.done", report_path=str(out_path), summary=report.get("summary", {}))

    summary = report.get("summary", {})
    if int(summary.get("errors", 0)) > 0:
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# db/sql (DuckDB lakehouse query layer)
# ---------------------------------------------------------------------------


@main.group("db")
@click.pass_context
def db_group(ctx: click.Context) -> None:
    """Database/query-layer utilities (DuckDB-backed lakehouse)."""
    _ = ctx


@db_group.command("init")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs directory root")
@click.option("--db-path", default="", help="DuckDB database file path (default: <data-dir>/ghtrader.duckdb)")
@click.option("--with-views/--no-with-views", default=True, show_default=True, help="Create/refresh Parquet-backed views")
@click.option("--with-metrics/--no-with-metrics", default=True, show_default=True, help="Index runs/ metrics JSON into DuckDB")
@click.pass_context
def db_init(
    ctx: click.Context,
    data_dir: str,
    runs_dir: str,
    db_path: str,
    with_views: bool,
    with_metrics: bool,
) -> None:
    """Initialize (or refresh) the DuckDB query/index database."""
    from ghtrader.db import DuckDBBackend, DuckDBConfig

    log = structlog.get_logger()
    data_root = Path(data_dir)
    runs_root = Path(runs_dir)
    db_file = Path(db_path) if str(db_path).strip() else (data_root / "ghtrader.duckdb")

    backend = DuckDBBackend(config=DuckDBConfig(db_path=db_file, read_only=False))
    with backend.connect() as con:
        created_views: list[str] = []
        if with_views:
            created_views = backend.init_views(con=con, data_dir=data_root)
        backend.init_metrics_tables(con=con)
        n_metrics = backend.ingest_runs_metrics(con=con, runs_dir=runs_root) if with_metrics else 0

    log.info(
        "db.init_done",
        db_path=str(db_file),
        views_created=len(created_views),
        metrics_rows=int(n_metrics),
    )


@db_group.command("ingest-metrics")
@click.option("--runs-dir", default="runs", help="Runs directory root")
@click.option("--data-dir", default="data", help="Data directory root (for default db path)")
@click.option("--db-path", default="", help="DuckDB database file path (default: <data-dir>/ghtrader.duckdb)")
@click.pass_context
def db_ingest_metrics(ctx: click.Context, runs_dir: str, data_dir: str, db_path: str) -> None:
    """Index runs/ metrics JSON into DuckDB (idempotent by path)."""
    from ghtrader.db import DuckDBBackend, DuckDBConfig

    log = structlog.get_logger()
    data_root = Path(data_dir)
    runs_root = Path(runs_dir)
    db_file = Path(db_path) if str(db_path).strip() else (data_root / "ghtrader.duckdb")

    backend = DuckDBBackend(config=DuckDBConfig(db_path=db_file, read_only=False))
    with backend.connect() as con:
        n = backend.ingest_runs_metrics(con=con, runs_dir=runs_root)
    log.info("db.ingest_metrics_done", db_path=str(db_file), rows=int(n))


@db_group.command("benchmark")
@click.option("--symbol", required=True, help="Symbol to benchmark (e.g. SHFE.cu2602 or KQ.m@SHFE.cu)")
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs directory root")
@click.option(
    "--ticks-lake",
    default="raw",
    type=click.Choice(["raw", "main_l5"]),
    show_default=True,
    help="Which ticks lake to benchmark (raw vs main_l5).",
)
@click.option(
    "--lake-version",
    default="v2",
    type=click.Choice(["v1", "v2"]),
    show_default=True,
    help="Which lake root to benchmark (v1=data/lake, v2=data/lake_v2).",
)
@click.option("--max-rows", default=200000, type=int, show_default=True, help="Max rows to load/ingest for the benchmark")
@click.option("--host", default="127.0.0.1", show_default=True, help="DB host (QuestDB/ClickHouse)")
@click.option("--questdb/--no-questdb", default=True, show_default=True, help="Run QuestDB benchmark (requires QuestDB running)")
@click.option("--clickhouse/--no-clickhouse", default=False, show_default=True, help="Run ClickHouse benchmark (requires ClickHouse running)")
@click.pass_context
def db_benchmark(
    ctx: click.Context,
    symbol: str,
    start: datetime,
    end: datetime,
    data_dir: str,
    runs_dir: str,
    ticks_lake: str,
    lake_version: str,
    max_rows: int,
    host: str,
    questdb: bool,
    clickhouse: bool,
) -> None:
    """Benchmark optional serving DBs (QuestDB/ClickHouse) on a real tick sample."""
    from ghtrader.db_bench import run_db_benchmark

    log = structlog.get_logger()
    out_path, report = run_db_benchmark(
        data_dir=Path(data_dir),
        runs_dir=Path(runs_dir),
        symbol=symbol,
        start_date=start.date(),
        end_date=end.date(),
        ticks_lake=ticks_lake,  # type: ignore[arg-type]
        lake_version=lake_version,  # type: ignore[arg-type]
        max_rows=int(max_rows),
        questdb=bool(questdb),
        clickhouse=bool(clickhouse),
        host=str(host),
    )
    log.info("db.benchmark_done", report_path=str(out_path), n_results=len(report.get("results") or []), n_errors=len(report.get("errors") or []))


@db_group.command("serve-sync")
@click.option("--backend", "backend_name", required=True, type=click.Choice(["questdb", "clickhouse"]), help="Serving DB backend")
@click.option("--table", default="", help="Destination table name (default: ghtrader_ticks_<ticks_lake>_<lake_version>)")
@click.option("--symbol", required=True, help="Symbol to sync (specific or derived)")
@click.option(
    "--ticks-lake",
    default="raw",
    type=click.Choice(["raw", "main_l5"]),
    show_default=True,
    help="Which ticks lake to sync (raw vs main_l5).",
)
@click.option(
    "--lake-version",
    default="v2",
    type=click.Choice(["v1", "v2"]),
    show_default=True,
    help="Which lake root to sync (v1=data/lake, v2=data/lake_v2).",
)
@click.option("--mode", default="incremental", type=click.Choice(["incremental", "backfill"]), show_default=True, help="Sync mode")
@click.option("--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
@click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs directory root (for state files)")
@click.option("--state-dir", default="", help="State directory (default: <runs-dir>/db_sync)")
@click.option("--host", default="127.0.0.1", show_default=True, help="DB host")
# QuestDB
@click.option("--questdb-ilp-port", default=9009, type=int, show_default=True, help="QuestDB ILP port")
@click.option("--questdb-pg-port", default=8812, type=int, show_default=True, help="QuestDB PGWire port")
@click.option("--questdb-pg-user", default="admin", show_default=True, help="QuestDB PGWire user")
@click.option("--questdb-pg-password", default="quest", show_default=True, help="QuestDB PGWire password")
@click.option("--questdb-pg-dbname", default="qdb", show_default=True, help="QuestDB PGWire dbname")
# ClickHouse
@click.option("--clickhouse-port", default=8123, type=int, show_default=True, help="ClickHouse HTTP port")
@click.option("--clickhouse-database", default="default", show_default=True, help="ClickHouse database")
@click.option("--clickhouse-user", default="default", show_default=True, help="ClickHouse username")
@click.option("--clickhouse-password", default="", show_default=True, help="ClickHouse password")
@click.pass_context
def db_serve_sync(
    ctx: click.Context,
    backend_name: str,
    table: str,
    symbol: str,
    ticks_lake: str,
    lake_version: str,
    mode: str,
    start: datetime | None,
    end: datetime | None,
    data_dir: str,
    runs_dir: str,
    state_dir: str,
    host: str,
    questdb_ilp_port: int,
    questdb_pg_port: int,
    questdb_pg_user: str,
    questdb_pg_password: str,
    questdb_pg_dbname: str,
    clickhouse_port: int,
    clickhouse_database: str,
    clickhouse_user: str,
    clickhouse_password: str,
) -> None:
    """Sync Parquet tick partitions into an optional serving DB (best-effort)."""
    from ghtrader.serving_db import ServingDBConfig, make_serving_backend, sync_ticks_to_serving_db

    log = structlog.get_logger()
    tbl = str(table).strip() or f"ghtrader_ticks_{ticks_lake}_{lake_version}"
    runs_root = Path(runs_dir)
    st_root = Path(state_dir) if str(state_dir).strip() else (runs_root / "db_sync")

    cfg = ServingDBConfig(
        backend=backend_name,  # type: ignore[arg-type]
        host=str(host),
        questdb_ilp_port=int(questdb_ilp_port),
        questdb_pg_port=int(questdb_pg_port),
        questdb_pg_user=str(questdb_pg_user),
        questdb_pg_password=str(questdb_pg_password),
        questdb_pg_dbname=str(questdb_pg_dbname),
        clickhouse_port=int(clickhouse_port),
        clickhouse_database=str(clickhouse_database),
        clickhouse_user=str(clickhouse_user),
        clickhouse_password=str(clickhouse_password),
    )
    backend = make_serving_backend(cfg)

    out = sync_ticks_to_serving_db(
        backend=backend,
        backend_type=cfg.backend,
        table=tbl,
        data_dir=Path(data_dir),
        symbol=str(symbol),
        ticks_lake=ticks_lake,  # type: ignore[arg-type]
        lake_version=lake_version,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        start_date=start.date() if start else None,
        end_date=end.date() if end else None,
        state_dir=st_root,
    )

    log.info(
        "db.serve_sync_done",
        backend=backend_name,
        table=tbl,
        symbol=symbol,
        ticks_lake=ticks_lake,
        lake_version=lake_version,
        mode=mode,
        ingested=int(out.get("ingested") or 0),
        skipped=int(out.get("skipped") or 0),
        seconds=float(out.get("seconds") or 0.0),
        state_path=str(out.get("state_path") or ""),
    )


@main.command("sql")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--runs-dir", default="runs", help="Runs directory root")
@click.option("--db-path", default="", help="DuckDB database file path (default: <data-dir>/ghtrader.duckdb)")
@click.option("--init/--no-init", default=True, show_default=True, help="Refresh views before running the query")
@click.option("--ingest-metrics/--no-ingest-metrics", default=False, show_default=True, help="Index runs/ metrics before query")
@click.option("--query", default="", help="SQL query string (SELECT/WITH only)")
@click.option("--query-file", default="", help="Path to a .sql file containing the query")
@click.option("--out", default="", help="Write results to a file (.csv or .parquet). If empty, prints a preview.")
@click.option("--limit", default=50, type=int, show_default=True, help="Rows to print when --out is empty")
@click.pass_context
def sql_cmd(
    ctx: click.Context,
    data_dir: str,
    runs_dir: str,
    db_path: str,
    init: bool,
    ingest_metrics: bool,
    query: str,
    query_file: str,
    out: str,
    limit: int,
) -> None:
    """Run a SQL query against the DuckDB lakehouse (Parquet-backed views)."""
    from ghtrader.db import DuckDBBackend, DuckDBConfig

    log = structlog.get_logger()
    data_root = Path(data_dir)
    runs_root = Path(runs_dir)
    db_file = Path(db_path) if str(db_path).strip() else (data_root / "ghtrader.duckdb")

    q = str(query).strip()
    if not q and str(query_file).strip():
        q = Path(query_file).read_text()
    q = str(q).strip()
    if not q:
        raise click.UsageError("Must pass --query or --query-file")

    backend = DuckDBBackend(config=DuckDBConfig(db_path=db_file, read_only=False))
    with backend.connect() as con:
        if init:
            backend.init_views(con=con, data_dir=data_root)
            backend.init_metrics_tables(con=con)
        if ingest_metrics:
            backend.ingest_runs_metrics(con=con, runs_dir=runs_root)

        df = backend.query_df(con=con, sql=q)

    if str(out).strip():
        out_path = Path(out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.suffix.lower() == ".csv":
            df.to_csv(out_path, index=False)
        elif out_path.suffix.lower() == ".parquet":
            df.to_parquet(out_path, index=False)
        else:
            raise click.UsageError("Unsupported --out type (use .csv or .parquet)")
        log.info("sql.wrote", out=str(out_path), rows=int(len(df)))
        return

    # Print preview
    if len(df) > int(limit):
        df2 = df.head(int(limit))
    else:
        df2 = df
    click.echo(df2.to_string(index=False))
    if len(df) > len(df2):
        click.echo(f"... {len(df) - len(df2)} more rows (use --out to export full result)")


def entrypoint() -> None:
    """
    CLI entrypoint with session auto-registration + strict locks.

    This is used by both:
    - terminal users running `ghtrader ...`
    - the dashboard spawning subprocess jobs (via env GHTRADER_JOB_ID)
    """
    # Ensure .env is loaded for runs_dir etc.
    load_config()

    runs_dir = get_runs_dir()
    db_path = _jobs_db_path(runs_dir)
    logs_dir = _logs_dir(runs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    job_id = os.environ.get("GHTRADER_JOB_ID", "").strip() or uuid.uuid4().hex[:12]
    os.environ["GHTRADER_JOB_ID"] = job_id
    source = os.environ.get("GHTRADER_JOB_SOURCE", "").strip() or "terminal"
    os.environ["GHTRADER_JOB_SOURCE"] = source

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
