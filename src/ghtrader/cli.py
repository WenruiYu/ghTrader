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
import sys
from datetime import date, datetime
from pathlib import Path

import click
import structlog

from ghtrader.config import load_config

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
    logging.basicConfig(format="%(message)s", level=level, stream=sys.stdout)


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
@click.pass_context
def download(ctx: click.Context, symbol: str, start: datetime, end: datetime,
             data_dir: str) -> None:
    """Download historical L5 ticks for a symbol and write to Parquet lake."""
    from ghtrader.tq_ingest import download_historical_ticks

    log = structlog.get_logger()
    log.info("download.start", symbol=symbol, start=start.date(), end=end.date())
    download_historical_ticks(
        symbol=symbol,
        start_date=start.date(),
        end_date=end.date(),
        data_dir=Path(data_dir),
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
) -> None:
    """Exhaustively backfill L5 ticks for a YYMM contract range (using akshare-inferred active ranges)."""
    from ghtrader.tq_ingest import download_contract_range as _download_contract_range

    log = structlog.get_logger()
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
    )
    log.info("download_contract_range.done")


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbols", "-s", required=True, multiple=True,
              help="Symbols to record (can specify multiple)")
@click.option("--data-dir", default="data", help="Data directory root")
@click.pass_context
def record(ctx: click.Context, symbols: tuple[str, ...], data_dir: str) -> None:
    """Run live tick recorder (subscribes and appends to Parquet lake)."""
    from ghtrader.tq_ingest import run_live_recorder

    log = structlog.get_logger()
    log.info("record.start", symbols=symbols)
    run_live_recorder(symbols=list(symbols), data_dir=Path(data_dir))


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

@main.command()
@click.option("--symbol", "-s", required=True, help="Symbol to build features for")
@click.option("--data-dir", default="data", help="Data directory root")
@click.option("--horizons", default="10,50,200", help="Comma-separated label horizons (ticks)")
@click.option("--threshold-k", default=1, type=int, help="Label threshold in price ticks")
@click.pass_context
def build(ctx: click.Context, symbol: str, data_dir: str, horizons: str,
          threshold_k: int) -> None:
    """Build features and labels from Parquet lake."""
    from ghtrader.features import FactorEngine
    from ghtrader.labels import build_labels_for_symbol

    log = structlog.get_logger()
    horizon_list = [int(h.strip()) for h in horizons.split(",")]
    log.info("build.start", symbol=symbol, horizons=horizon_list, threshold_k=threshold_k)

    # Build labels
    build_labels_for_symbol(
        symbol=symbol,
        data_dir=Path(data_dir),
        horizons=horizon_list,
        threshold_k=threshold_k,
    )

    # Build features
    engine = FactorEngine()
    engine.build_features_for_symbol(symbol=symbol, data_dir=Path(data_dir))

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


if __name__ == "__main__":
    main()
