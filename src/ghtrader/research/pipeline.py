"""
Pipeline: daily training pipeline and Ray-based parallel sweeps.

Implements:
- Daily scheduled training: data refresh -> build -> train -> eval -> promote
- Hyperparameter sweeps using Ray for CPU/GPU utilization
- Latency instrumentation
"""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import numpy as np
import structlog

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Latency instrumentation
# ---------------------------------------------------------------------------

@dataclass
class LatencyTracker:
    """
    Track latency across the inference pipeline.
    
    Stages: ingest -> features -> inference -> decision
    """
    
    measurements: dict[str, list[float]] = field(default_factory=dict)
    thresholds: dict[str, float] = field(default_factory=lambda: {
        "ingest": 0.001,      # 1ms
        "features": 0.005,    # 5ms
        "inference": 0.010,   # 10ms
        "decision": 0.001,    # 1ms
        "total": 0.020,       # 20ms total budget
    })
    
    def __post_init__(self) -> None:
        self.measurements = {
            "ingest": [],
            "features": [],
            "inference": [],
            "decision": [],
            "total": [],
        }
    
    def record(self, stage: str, latency_sec: float) -> None:
        """Record a latency measurement."""
        if stage not in self.measurements:
            self.measurements[stage] = []
        self.measurements[stage].append(latency_sec)
    
    def get_stats(self, stage: str) -> dict[str, float]:
        """Get statistics for a stage."""
        values = self.measurements.get(stage, [])
        if not values:
            return {"mean": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
        
        arr = np.array(values)
        return {
            "mean": float(np.mean(arr)),
            "p50": float(np.percentile(arr, 50)),
            "p95": float(np.percentile(arr, 95)),
            "p99": float(np.percentile(arr, 99)),
            "max": float(np.max(arr)),
        }
    
    def check_budget(self) -> tuple[bool, dict[str, Any]]:
        """
        Check if latency budget is met.
        
        Returns:
            (passed, report dict)
        """
        report: dict[str, Any] = {}
        violations: list[str] = []
        
        for stage, threshold in self.thresholds.items():
            stats = self.get_stats(stage)
            report[stage] = stats
            
            if stats["p95"] > threshold:
                violations.append(f"{stage}: p95={stats['p95']*1000:.1f}ms > {threshold*1000:.1f}ms")
        
        passed = len(violations) == 0
        report["passed"] = passed
        report["violations"] = violations
        
        return passed, report
    
    def reset(self) -> None:
        """Reset all measurements."""
        for stage in self.measurements:
            self.measurements[stage].clear()


class LatencyContext:
    """Context manager for timing a stage."""
    
    def __init__(self, tracker: LatencyTracker, stage: str):
        self.tracker = tracker
        self.stage = stage
        self.start_time = 0.0
    
    def __enter__(self) -> LatencyContext:
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, *args: Any) -> None:
        elapsed = time.perf_counter() - self.start_time
        self.tracker.record(self.stage, elapsed)


# Global latency tracker
_latency_tracker = LatencyTracker()


def get_latency_tracker() -> LatencyTracker:
    """Get the global latency tracker."""
    return _latency_tracker


# ---------------------------------------------------------------------------
# Daily training pipeline
# ---------------------------------------------------------------------------

def run_daily_pipeline(
    symbols: list[str],
    model_type: str,
    data_dir: Path,
    artifacts_dir: Path,
    runs_dir: Path,
    horizon: int = 50,
    lookback_days: int = 30,
    backtest_days: int = 5,
) -> dict[str, Any]:
    """
    Run the daily training pipeline.
    
    Steps:
    1. Refresh data: download latest ticks for each symbol
    2. Build: generate features and labels
    3. Train: train model on historical data
    4. Evaluate: run walk-forward backtest
    5. Promote: if passes gates, promote to production
    
    Args:
        symbols: Instruments to train
        model_type: Model type
        data_dir: Data directory
        artifacts_dir: Artifacts directory
        runs_dir: Runs directory
        horizon: Label horizon
        lookback_days: Days of history for training
        backtest_days: Days for backtest evaluation
    
    Returns:
        Report dict with results for each symbol
    """
    from ghtrader.research.eval import PromotionGate, evaluate_and_promote
    from ghtrader.tq.eval import run_backtest
    from ghtrader.datasets.features import FactorEngine
    from ghtrader.datasets.labels import build_labels_for_symbol
    from ghtrader.research.models import train_model
    from ghtrader.tq.ingest import download_historical_ticks
    
    today = date.today()
    start_date = today - timedelta(days=lookback_days)
    backtest_start = today - timedelta(days=backtest_days)
    
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "symbols": symbols,
        "model_type": model_type,
        "results": {},
    }
    
    log.info("daily_pipeline.start", run_id=run_id, symbols=symbols)
    
    for symbol in symbols:
        symbol_report: dict[str, Any] = {"symbol": symbol, "steps": {}}
        
        try:
            # Step 1: Refresh data
            log.info("daily_pipeline.step", symbol=symbol, step="refresh_data")
            t0 = time.time()
            download_historical_ticks(
                symbol=symbol,
                start_date=start_date,
                end_date=today,
                data_dir=data_dir,
            )
            symbol_report["steps"]["refresh_data"] = {"status": "ok", "duration": time.time() - t0}
            
            # Step 2: Build features
            log.info("daily_pipeline.step", symbol=symbol, step="build_features")
            t0 = time.time()
            engine = FactorEngine()
            engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)
            symbol_report["steps"]["build_features"] = {"status": "ok", "duration": time.time() - t0}
            
            # Step 3: Build labels
            log.info("daily_pipeline.step", symbol=symbol, step="build_labels")
            t0 = time.time()
            build_labels_for_symbol(
                symbol=symbol,
                data_dir=data_dir,
                horizons=[horizon],
            )
            symbol_report["steps"]["build_labels"] = {"status": "ok", "duration": time.time() - t0}
            
            # Step 4: Train model
            log.info("daily_pipeline.step", symbol=symbol, step="train")
            t0 = time.time()
            candidate_path = train_model(
                model_type=model_type,
                symbol=symbol,
                data_dir=data_dir,
                artifacts_dir=artifacts_dir / "candidates",
                horizon=horizon,
            )
            symbol_report["steps"]["train"] = {"status": "ok", "duration": time.time() - t0}
            
            # Step 5: Backtest evaluation
            log.info("daily_pipeline.step", symbol=symbol, step="backtest")
            t0 = time.time()
            metrics = run_backtest(
                model_name=model_type,
                symbol=symbol,
                start_date=backtest_start,
                end_date=today,
                data_dir=data_dir,
                artifacts_dir=artifacts_dir / "candidates",
                runs_dir=runs_dir,
                horizon=horizon,
            )
            symbol_report["steps"]["backtest"] = {
                "status": "ok",
                "duration": time.time() - t0,
                "metrics": metrics.to_dict(),
            }
            
            # Step 6: Promotion gate
            log.info("daily_pipeline.step", symbol=symbol, step="promote")
            gate = PromotionGate()
            production_path = artifacts_dir / "production" / symbol / model_type / f"model_h{horizon}.pt"
            
            promoted = evaluate_and_promote(
                candidate_path=candidate_path,
                production_path=production_path,
                metrics=metrics,
                gate=gate,
            )
            symbol_report["steps"]["promote"] = {
                "status": "promoted" if promoted else "rejected",
                "gate_passed": promoted,
            }
            
            symbol_report["status"] = "success"
            
        except Exception as e:
            log.error("daily_pipeline.failed", symbol=symbol, error=str(e))
            symbol_report["status"] = "failed"
            symbol_report["error"] = str(e)
        
        report["results"][symbol] = symbol_report
    
    # Save report
    report_dir = runs_dir / "daily_pipeline" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    with open(report_dir / "report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    log.info("daily_pipeline.done", run_id=run_id, report_path=str(report_dir / "report.json"))
    
    return report


# ---------------------------------------------------------------------------
# Ray-based hyperparameter sweep
# ---------------------------------------------------------------------------

def run_hyperparam_sweep(
    symbol: str,
    model_type: str,
    data_dir: Path,
    artifacts_dir: Path,
    runs_dir: Path,
    n_trials: int = 20,
    n_cpus_per_trial: int = 4,
    n_gpus_per_trial: int = 1,
    horizon: int = 50,
) -> dict[str, Any]:
    """
    Run Ray-based hyperparameter sweep.
    
    Uses Ray Tune for distributed hyperparameter search.
    
    Args:
        symbol: Instrument to optimize
        model_type: Model type
        data_dir: Data directory
        artifacts_dir: Artifacts directory
        runs_dir: Runs directory
        n_trials: Number of trials
        n_cpus_per_trial: CPUs per trial
        n_gpus_per_trial: GPUs per trial
        horizon: Label horizon
    
    Returns:
        Best config and metrics
    """
    try:
        import ray
        from ray import tune
        from ray.tune.schedulers import ASHAScheduler
    except ImportError:
        log.error("sweep.ray_not_installed")
        raise RuntimeError("Ray is required for sweeps. Install with: pip install ray[tune]")
    
    from ghtrader.datasets.features import read_features_for_symbol, read_features_manifest
    from ghtrader.datasets.labels import read_labels_for_symbol, read_labels_manifest
    from ghtrader.research.models import create_model
    
    log.info("sweep.start", symbol=symbol, model_type=model_type, n_trials=n_trials)
    
    # Load data once
    features_df = read_features_for_symbol(data_dir, symbol)
    labels_df = read_labels_for_symbol(data_dir, symbol)
    df = features_df.merge(labels_df[["datetime", f"label_{horizon}"]], on="datetime")
    
    # Guardrails: ensure features/labels manifests align.
    manifest = read_features_manifest(data_dir, symbol)
    lab_manifest = read_labels_manifest(data_dir, symbol)
    if not manifest or not lab_manifest:
        raise ValueError("Missing QuestDB build metadata for features/labels. Run `ghtrader build` first.")
    tk_f = str(manifest.get("ticks_kind") or "")
    tk_l = str(lab_manifest.get("ticks_kind") or "")
    if tk_f != tk_l:
        raise ValueError(f"ticks_kind mismatch between features ({tk_f}) and labels ({tk_l}) for symbol={symbol}")
    if str(symbol).startswith("KQ.m@") and tk_f != "main_l5":
        raise ValueError("Continuous symbols (KQ.m@...) must use derived main_l5 ticks for sweep.")
    if tk_f == "main_l5":
        sf = dict(manifest.get("schedule") or {})
        sl = dict(lab_manifest.get("schedule") or {})
        hf = str(sf.get("hash") or "")
        hl = str(sl.get("hash") or "")
        if hf and hl and hf != hl:
            raise ValueError(f"schedule_hash mismatch between features ({hf}) and labels ({hl}) for symbol={symbol}")

    feature_cols = list(manifest.get("enabled_factors") or [])
    if feature_cols:
        feature_cols = [c for c in feature_cols if c in features_df.columns]
    else:
        exclude = {"symbol", "datetime", "underlying_contract", "segment_id"}
        feature_cols = [c for c in features_df.columns if c not in exclude]

    seg_all: np.ndarray | None = None
    if "segment_id" in df.columns:
        try:
            seg_all = pd.to_numeric(df["segment_id"], errors="coerce").fillna(-1).astype("int64").values
            if len(seg_all) > 0 and int(seg_all.min()) == int(seg_all.max()):
                seg_all = None
        except Exception:
            seg_all = None
    X = df[feature_cols].values
    y = df[f"label_{horizon}"].values
    
    # Define search space based on model type
    if model_type == "deeplob":
        search_space = {
            "hidden_dim": tune.choice([32, 64, 128]),
            "lr": tune.loguniform(1e-4, 1e-2),
            "batch_size": tune.choice([128, 256, 512]),
            "epochs": tune.choice([30, 50, 100]),
        }
    elif model_type == "transformer":
        search_space = {
            "d_model": tune.choice([32, 64, 128]),
            "n_heads": tune.choice([2, 4, 8]),
            "n_layers": tune.choice([2, 3, 4]),
            "lr": tune.loguniform(1e-4, 1e-2),
        }
    elif model_type in ["xgboost", "lightgbm"]:
        search_space = {
            "max_depth": tune.randint(3, 10),
            "learning_rate": tune.loguniform(0.01, 0.3),
            "n_estimators": tune.choice([100, 300, 500]),
        }
    else:
        search_space = {
            "C": tune.loguniform(0.01, 10),
        }
    
    def train_fn(config: dict) -> dict:
        """Training function for Ray Tune."""
        # Split data (time-based)
        n = len(X)
        train_end = int(n * 0.7)
        val_end = int(n * 0.85)
        
        X_train, y_train = X[:train_end], y[:train_end]
        X_val, y_val = X[train_end:val_end], y[train_end:val_end]
        seg_train = seg_all[:train_end] if seg_all is not None else None
        seg_val = seg_all[train_end:val_end] if seg_all is not None else None
        
        # Create and train model
        model = create_model(model_type, n_features=X.shape[1], **config)
        
        if model_type in ["deeplob", "transformer"]:
            model.fit(X_train, y_train, epochs=config.get("epochs", 50), segment_id=seg_train)
        else:
            model.fit(X_train, y_train, segment_id=seg_train)
        
        # Evaluate
        probs = model.predict_proba(X_val, segment_id=seg_val)
        if len(probs.shape) == 2:
            preds = probs.argmax(axis=1)
        else:
            preds = probs[-len(y_val):].argmax(axis=1)
        
        # Filter valid
        valid_mask = ~np.isnan(y_val)
        if len(probs.shape) == 2:
            valid_mask = valid_mask & (~np.isnan(probs).any(axis=1))
        accuracy = (preds[valid_mask] == y_val[valid_mask]).mean()
        
        return {"accuracy": accuracy}
    
    # Initialize Ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    
    # Run sweep
    scheduler = ASHAScheduler(
        metric="accuracy",
        mode="max",
        max_t=100,
        grace_period=10,
    )
    
    analysis = tune.run(
        train_fn,
        config=search_space,
        num_samples=n_trials,
        resources_per_trial={"cpu": n_cpus_per_trial, "gpu": n_gpus_per_trial},
        scheduler=scheduler,
        verbose=1,
    )
    
    # Get best config
    best_config = analysis.get_best_config(metric="accuracy", mode="max")
    best_result = analysis.get_best_trial(metric="accuracy", mode="max").last_result
    
    log.info("sweep.done", best_config=best_config, best_accuracy=best_result["accuracy"])
    
    # Save results
    sweep_dir = runs_dir / "sweeps" / symbol / model_type
    sweep_dir.mkdir(parents=True, exist_ok=True)
    
    with open(sweep_dir / "best_config.json", "w") as f:
        json.dump({
            "best_config": best_config,
            "best_accuracy": best_result["accuracy"],
            "n_trials": n_trials,
        }, f, indent=2)
    
    return {
        "best_config": best_config,
        "best_accuracy": best_result["accuracy"],
    }


# ---------------------------------------------------------------------------
# Offline L5 micro-simulator
# ---------------------------------------------------------------------------

@dataclass
class MicroSimConfig:
    """Configuration for offline L5 micro-simulator."""
    
    # Latency model (in ticks)
    order_latency_ticks: int = 2
    cancel_latency_ticks: int = 1
    
    # Fill model
    aggressive_fill_prob: float = 1.0  # Probability of fill for aggressive orders
    passive_queue_position: float = 0.5  # Assumed queue position (0=front, 1=back)
    
    # Costs
    commission_per_lot: float = 0.0
    slippage_ticks: float = 0.0
    
    # Volume limits
    max_participation_rate: float = 0.1  # Max fraction of volume to take


@dataclass
class MicroSimOrder:
    """Order in the micro-simulator."""
    
    order_id: int
    symbol: str
    direction: str  # "BUY" or "SELL"
    price: float
    volume: int
    submitted_tick: int
    is_active: bool = True
    filled_volume: int = 0
    fill_price: float = 0.0


@dataclass
class MicroSimState:
    """State of the micro-simulator."""
    
    tick_idx: int = 0
    position: int = 0
    cash: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    orders: list[MicroSimOrder] = field(default_factory=list)
    fills: list[dict] = field(default_factory=list)
    next_order_id: int = 1


class OfflineMicroSim:
    """
    Offline L5 micro-simulator for realistic HFT evaluation.
    
    Features:
    - Latency modeling (order/cancel delays)
    - Queue position modeling for passive orders
    - Partial fills
    - Slippage and commission
    """
    
    def __init__(self, config: MicroSimConfig | None = None):
        self.config = config or MicroSimConfig()
        self.state = MicroSimState()
        self.tick_data: list[dict] = []
    
    def load_ticks(self, data_dir: Path, symbol: str, start_date: date, end_date: date) -> None:
        """Load tick data for simulation."""
        import pandas as pd

        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.queries import fetch_ticks_for_symbol_day, list_trading_days_for_symbol

        _ = data_dir  # QuestDB is the canonical source

        lv = "v2"
        if not str(symbol).startswith("KQ.m@"):
            raise ValueError("raw ticks are deferred (Phase-1/2)")
        tl = "main_l5"
        table = "ghtrader_ticks_main_l5_v2"
        cfg = make_questdb_query_config_from_env()

        days = list_trading_days_for_symbol(
            cfg=cfg,
            table=table,
            symbol=str(symbol),
            start_day=start_date,
            end_day=end_date,
            dataset_version=lv,
            ticks_kind=tl,
        )

        frames: list[pd.DataFrame] = []
        for d in days:
            df_day = fetch_ticks_for_symbol_day(
                cfg=cfg,
                table=table,
                symbol=str(symbol),
                trading_day=d.isoformat(),
                dataset_version=lv,
                ticks_kind=tl,
                limit=None,
                order="asc",
                include_provenance=(tl == "main_l5"),
                connect_timeout_s=2,
            )
            if not df_day.empty:
                frames.append(df_day)

        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self.tick_data = df.to_dict("records")
        log.info("microsim.loaded", n_ticks=len(self.tick_data))
    
    def reset(self) -> None:
        """Reset simulator state."""
        self.state = MicroSimState()
    
    def submit_order(self, direction: str, price: float, volume: int) -> int:
        """Submit an order. Returns order ID."""
        order = MicroSimOrder(
            order_id=self.state.next_order_id,
            symbol="",
            direction=direction,
            price=price,
            volume=volume,
            submitted_tick=self.state.tick_idx,
        )
        self.state.orders.append(order)
        self.state.next_order_id += 1
        return order.order_id
    
    def cancel_order(self, order_id: int) -> bool:
        """Cancel an order. Returns True if successful."""
        for order in self.state.orders:
            if order.order_id == order_id and order.is_active:
                # Cancel happens after latency
                order.is_active = False
                return True
        return False
    
    def step(self) -> dict[str, Any]:
        """
        Advance simulator by one tick.
        
        Returns:
            Dict with tick info and any fills
        """
        if self.state.tick_idx >= len(self.tick_data):
            return {"done": True}
        
        tick = self.tick_data[self.state.tick_idx]
        fills_this_tick: list[dict] = []
        
        # Process orders that have passed latency
        for order in self.state.orders:
            if not order.is_active:
                continue
            
            ticks_since_submit = self.state.tick_idx - order.submitted_tick
            if ticks_since_submit < self.config.order_latency_ticks:
                continue
            
            # Check for fill
            filled = False
            fill_price = 0.0
            
            if order.direction == "BUY":
                # Aggressive: crosses the spread
                if order.price >= tick["ask_price1"]:
                    filled = True
                    fill_price = tick["ask_price1"] + self.config.slippage_ticks
                # Passive: check if price traded through
                elif order.price >= tick["last_price"]:
                    # Simplified: random fill based on queue position
                    if np.random.random() > self.config.passive_queue_position:
                        filled = True
                        fill_price = order.price
            else:  # SELL
                if order.price <= tick["bid_price1"]:
                    filled = True
                    fill_price = tick["bid_price1"] - self.config.slippage_ticks
                elif order.price <= tick["last_price"]:
                    if np.random.random() > self.config.passive_queue_position:
                        filled = True
                        fill_price = order.price
            
            if filled:
                fill_volume = min(
                    order.volume - order.filled_volume,
                    int(tick["volume"] * self.config.max_participation_rate),
                )
                
                if fill_volume > 0:
                    order.filled_volume += fill_volume
                    order.fill_price = fill_price
                    
                    # Update position and cash
                    if order.direction == "BUY":
                        self.state.position += fill_volume
                        self.state.cash -= fill_volume * fill_price
                    else:
                        self.state.position -= fill_volume
                        self.state.cash += fill_volume * fill_price
                    
                    # Commission
                    self.state.cash -= fill_volume * self.config.commission_per_lot
                    
                    fill_record = {
                        "order_id": order.order_id,
                        "tick_idx": self.state.tick_idx,
                        "direction": order.direction,
                        "price": fill_price,
                        "volume": fill_volume,
                    }
                    fills_this_tick.append(fill_record)
                    self.state.fills.append(fill_record)
                    
                    if order.filled_volume >= order.volume:
                        order.is_active = False
        
        # Update unrealized PnL
        mid = (tick["bid_price1"] + tick["ask_price1"]) / 2
        self.state.unrealized_pnl = self.state.position * mid + self.state.cash
        
        self.state.tick_idx += 1
        
        return {
            "done": False,
            "tick": tick,
            "fills": fills_this_tick,
            "position": self.state.position,
            "unrealized_pnl": self.state.unrealized_pnl,
        }
    
    def run_strategy(
        self,
        strategy_fn: Callable[[dict, MicroSimState], tuple[str, float, int] | None],
    ) -> dict[str, Any]:
        """
        Run a strategy through the simulator.
        
        Args:
            strategy_fn: Function(tick, state) -> (direction, price, volume) or None
        
        Returns:
            Results dict with PnL, metrics, etc.
        """
        self.reset()
        
        while True:
            result = self.step()
            if result.get("done"):
                break
            
            # Get strategy decision
            decision = strategy_fn(result["tick"], self.state)
            if decision is not None:
                direction, price, volume = decision
                self.submit_order(direction, price, volume)
        
        # Compute final metrics
        return {
            "final_position": self.state.position,
            "realized_pnl": self.state.realized_pnl,
            "unrealized_pnl": self.state.unrealized_pnl,
            "total_pnl": self.state.unrealized_pnl,
            "n_fills": len(self.state.fills),
            "n_ticks": self.state.tick_idx,
        }
