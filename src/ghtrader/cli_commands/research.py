from __future__ import annotations

from datetime import datetime
from pathlib import Path

import click
import structlog

log = structlog.get_logger()


def register(main: click.Group) -> None:
    """
    Register research/model lifecycle commands on the root CLI group.
    """

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
    def build(ctx: click.Context, symbol: str, data_dir: str, horizons: str, threshold_k: int, ticks_kind: str, overwrite: bool) -> None:
        """Build features and labels (QuestDB-first)."""
        from ghtrader.cli import _acquire_locks
        from ghtrader.datasets.features import FactorEngine
        from ghtrader.datasets.labels import build_labels_for_symbol

        _ = ctx
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

        build_labels_for_symbol(
            symbol=symbol,
            data_dir=Path(data_dir),
            horizons=horizon_list,
            threshold_k=threshold_k,
            ticks_kind=ticks_kind,  # type: ignore[arg-type]
            overwrite=overwrite,
        )

        engine = FactorEngine()
        engine.build_features_for_symbol(
            symbol=symbol,
            data_dir=Path(data_dir),
            ticks_kind=ticks_kind,  # type: ignore[arg-type]
            overwrite=overwrite,
        )

        log.info("build.done", symbol=symbol)

    @main.command()
    @click.option(
        "--model",
        "-m",
        required=True,
        type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
        help="Model type to train",
    )
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
    @click.option("--num-workers", default=0, type=int, show_default=True, help="DataLoader workers (0=auto)")
    @click.option("--prefetch-factor", default=2, type=int, show_default=True, help="DataLoader prefetch factor")
    @click.option(
        "--pin-memory",
        default="auto",
        type=click.Choice(["auto", "true", "false"]),
        show_default=True,
        help="Pin DataLoader memory (auto=true on CUDA)",
    )
    @click.pass_context
    def train(
        ctx: click.Context,
        model: str,
        symbol: str,
        data_dir: str,
        artifacts_dir: str,
        horizon: int,
        gpus: int,
        epochs: int,
        batch_size: int,
        seq_len: int,
        lr: float,
        ddp: bool,
        num_workers: int,
        prefetch_factor: int,
        pin_memory: str,
    ) -> None:
        """Train a model (baseline or deep)."""
        from ghtrader.cli import _acquire_locks
        from ghtrader.research.models import train_model

        _ = ctx
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
            num_workers=int(num_workers),
            prefetch_factor=int(prefetch_factor),
            pin_memory=str(pin_memory),
        )

        deep_models = {"deeplob", "transformer", "tcn", "tlob", "ssm"}
        model_kwargs = {"seq_len": seq_len} if model in deep_models else {}
        pin_memory_opt: bool | None
        if str(pin_memory).lower() == "true":
            pin_memory_opt = True
        elif str(pin_memory).lower() == "false":
            pin_memory_opt = False
        else:
            pin_memory_opt = None

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
            num_workers=int(num_workers),
            prefetch_factor=int(prefetch_factor),
            pin_memory=pin_memory_opt,
            **model_kwargs,
        )
        log.info("train.done", model=model, symbol=symbol)

    @main.command()
    @click.option("--model", "-m", required=True, help="Model name/path to backtest")
    @click.option("--symbol", "-s", required=True, help="Symbol to backtest on")
    @click.option("--start", "-S", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Backtest start date")
    @click.option("--end", "-E", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Backtest end date")
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
    @click.option("--runs-dir", default="runs", help="Runs output directory")
    @click.pass_context
    def backtest(
        ctx: click.Context,
        model: str,
        symbol: str,
        start: datetime,
        end: datetime,
        data_dir: str,
        artifacts_dir: str,
        runs_dir: str,
    ) -> None:
        """Run TqSdk backtest harness with trained model."""
        from ghtrader.tq.eval import run_backtest

        _ = ctx
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

    @main.command()
    @click.option("--model", "-m", required=True, help="Model name/path for inference")
    @click.option("--symbols", "-s", required=True, multiple=True, help="Symbols to trade (can specify multiple)")
    @click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
    @click.pass_context
    def paper(ctx: click.Context, model: str, symbols: tuple[str, ...], artifacts_dir: str) -> None:
        """Run paper-trading loop with online calibrator (no real orders)."""
        from ghtrader.tq.paper import run_paper_trading

        _ = ctx
        log.info("paper.start", model=model, symbols=symbols)
        run_paper_trading(
            model_name=model,
            symbols=list(symbols),
            artifacts_dir=Path(artifacts_dir),
        )

    @main.command()
    @click.option(
        "--model",
        "-m",
        required=True,
        type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
        help="Model type to benchmark",
    )
    @click.option("--symbol", "-s", required=True, help="Symbol to benchmark on")
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
    @click.option("--runs-dir", default="runs", help="Runs directory")
    @click.option("--horizon", default=50, type=int, help="Label horizon")
    @click.pass_context
    def benchmark(ctx: click.Context, model: str, symbol: str, data_dir: str, artifacts_dir: str, runs_dir: str, horizon: int) -> None:
        """Run a benchmark for a model on a symbol."""
        from ghtrader.research.benchmark import run_benchmark

        _ = ctx
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

    @main.command()
    @click.option("--symbol", "-s", required=True, help="Symbol to compare on")
    @click.option("--models", "-m", default="logistic,xgboost,deeplob", help="Comma-separated list of models to compare")
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
    @click.option("--runs-dir", default="runs", help="Runs directory")
    @click.option("--horizon", default=50, type=int, help="Label horizon")
    @click.pass_context
    def compare(ctx: click.Context, symbol: str, models: str, data_dir: str, artifacts_dir: str, runs_dir: str, horizon: int) -> None:
        """Compare multiple models on the same dataset."""
        from ghtrader.research.benchmark import compare_models

        _ = ctx
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

        click.echo("\nModel Comparison Results:")
        click.echo(df.to_string(index=False))

    @main.command("daily-train")
    @click.option("--symbols", "-s", required=True, multiple=True, help="Symbols to train (can specify multiple)")
    @click.option(
        "--model",
        "-m",
        default="deeplob",
        type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
        help="Model type to train",
    )
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
    @click.option("--runs-dir", default="runs", help="Runs directory")
    @click.option("--horizon", default=50, type=int, help="Label horizon")
    @click.option("--lookback-days", default=30, type=int, help="Days of history to use")
    @click.pass_context
    def daily_train(
        ctx: click.Context,
        symbols: tuple[str, ...],
        model: str,
        data_dir: str,
        artifacts_dir: str,
        runs_dir: str,
        horizon: int,
        lookback_days: int,
    ) -> None:
        """Run daily training pipeline: refresh data, build, train, evaluate, promote."""
        from ghtrader.cli import _acquire_locks
        from ghtrader.research.pipeline import run_daily_pipeline

        _ = ctx
        lock_keys: list[str] = []
        for s in symbols:
            lock_keys.append(f"ticks:symbol={s}")
            lock_keys.append(f"build:symbol={s},ticks_kind=main_l5")
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

    @main.command("research-loop-template")
    @click.option("--symbol", "-s", required=True, help="Symbol for this research iteration")
    @click.option(
        "--model",
        "-m",
        default="deeplob",
        type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm", "lobert", "kanformer"]),
        show_default=True,
        help="Primary model under evaluation",
    )
    @click.option("--horizon", default=50, type=int, show_default=True, help="Prediction horizon")
    @click.option("--runs-dir", default="runs", show_default=True, help="Runs output directory")
    @click.option("--owner", default="", help="Iteration owner")
    @click.option("--hypothesis", default="", help="Initial hypothesis text")
    @click.pass_context
    def research_loop_template(
        ctx: click.Context,
        symbol: str,
        model: str,
        horizon: int,
        runs_dir: str,
        owner: str,
        hypothesis: str,
    ) -> None:
        """Scaffold a PRD-aligned research loop template (propose->implement->evaluate->learn)."""
        from ghtrader.research.loop import build_research_loop_template, write_research_loop_template

        _ = ctx
        tpl = build_research_loop_template(
            symbol=symbol,
            model=model,
            horizon=int(horizon),
            owner=owner,
            hypothesis=hypothesis,
        )
        out = write_research_loop_template(runs_dir=Path(runs_dir), template=tpl)
        log.info("research.loop_template.created", path=str(out), symbol=symbol, model=model, horizon=int(horizon))
        click.echo(str(out))

    @main.command("capacity-matrix")
    @click.option("--runs-dir", default="runs", show_default=True, help="Runs output directory")
    @click.option("--smoke/--no-smoke", default=True, show_default=True, help="Include environment smoke checks")
    @click.pass_context
    def capacity_matrix(ctx: click.Context, runs_dir: str, smoke: bool) -> None:
        """Generate capacity benchmark/regression matrix report scaffold."""
        from ghtrader.capacity import build_capacity_report, write_capacity_report

        _ = ctx
        report = build_capacity_report(include_smoke=bool(smoke))
        out = write_capacity_report(runs_dir=Path(runs_dir), report=report)
        log.info("capacity.matrix.generated", path=str(out), smoke=bool(smoke))
        click.echo(str(out))

    @main.command()
    @click.option("--symbol", "-s", required=True, help="Symbol to sweep on")
    @click.option(
        "--model",
        "-m",
        default="deeplob",
        type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer"]),
        help="Model type to sweep",
    )
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--artifacts-dir", default="artifacts", help="Artifacts directory")
    @click.option("--runs-dir", default="runs", help="Runs directory")
    @click.option("--n-trials", default=20, type=int, help="Number of trials")
    @click.option("--n-cpus", default=8, type=int, help="CPUs per trial")
    @click.option("--n-gpus", default=1, type=int, help="GPUs per trial")
    @click.pass_context
    def sweep(
        ctx: click.Context,
        symbol: str,
        model: str,
        data_dir: str,
        artifacts_dir: str,
        runs_dir: str,
        n_trials: int,
        n_cpus: int,
        n_gpus: int,
    ) -> None:
        """Run Ray-based hyperparameter sweep."""
        from ghtrader.cli import _acquire_locks
        from ghtrader.research.pipeline import run_hyperparam_sweep

        _ = ctx
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
