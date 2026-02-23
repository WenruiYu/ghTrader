"""
Benchmarking harness: compute offline metrics + trading metrics + latency stats.

Produces a single JSON report per run with comprehensive model evaluation.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sklearn.metrics import accuracy_score, f1_score, log_loss

from ghtrader.datasets.features import FactorEngine, read_features_for_symbol
from ghtrader.datasets.labels import read_labels_for_symbol, create_walk_forward_splits, apply_split
from ghtrader.research.models import create_model, load_model
from ghtrader.research.pipeline import LatencyContext, LatencyTracker
from ghtrader.util.observability import get_store

log = structlog.get_logger()
_obs_store = get_store("research.benchmark")


# ---------------------------------------------------------------------------
# Metrics dataclasses
# ---------------------------------------------------------------------------

@dataclass
class OfflineMetrics:
    """Offline (classification) metrics."""
    
    accuracy: float = 0.0
    accuracy_std: float = 0.0
    f1_macro: float = 0.0
    f1_macro_std: float = 0.0
    f1_weighted: float = 0.0
    f1_weighted_std: float = 0.0
    log_loss: float = 0.0
    log_loss_std: float = 0.0
    
    # Per-class accuracy
    accuracy_down: float = 0.0
    accuracy_flat: float = 0.0
    accuracy_up: float = 0.0
    
    # Calibration (ECE - expected calibration error)
    ece: float = 0.0
    ece_std: float = 0.0
    
    n_samples: int = 0


@dataclass
class TradingMetrics:
    """Trading (PnL-based) metrics from backtest."""
    
    total_pnl: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    n_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_trade_pnl: float = 0.0


@dataclass
class LatencyMetrics:
    """Latency metrics from inference."""
    
    features_mean_ms: float = 0.0
    features_p95_ms: float = 0.0
    inference_mean_ms: float = 0.0
    inference_p95_ms: float = 0.0
    total_mean_ms: float = 0.0
    total_p95_ms: float = 0.0
    budget_passed: bool = True


@dataclass
class BenchmarkReport:
    """Complete benchmark report."""
    
    run_id: str
    timestamp: str
    model_type: str
    symbol: str
    horizon: int
    
    offline: OfflineMetrics = field(default_factory=OfflineMetrics)
    trading: TradingMetrics = field(default_factory=TradingMetrics)
    latency: LatencyMetrics = field(default_factory=LatencyMetrics)

    # Per-split results for walk-forward evaluation
    offline_splits: list[dict[str, Any]] = field(default_factory=list)
    
    config: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "model_type": self.model_type,
            "symbol": self.symbol,
            "horizon": self.horizon,
            "offline": asdict(self.offline),
            "trading": asdict(self.trading),
            "latency": asdict(self.latency),
            "offline_splits": self.offline_splits,
            "config": self.config,
        }
    
    def save(self, path: Path) -> None:
        """Save report to JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_ece(y_true: np.ndarray, y_probs: np.ndarray, n_bins: int = 10) -> float:
    """Compute Expected Calibration Error."""
    confidences = y_probs.max(axis=1)
    predictions = y_probs.argmax(axis=1)
    accuracies = (predictions == y_true).astype(float)
    
    bin_boundaries = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    
    for i in range(n_bins):
        in_bin = (confidences > bin_boundaries[i]) & (confidences <= bin_boundaries[i + 1])
        prop_in_bin = in_bin.mean()
        
        if prop_in_bin > 0:
            avg_confidence = confidences[in_bin].mean()
            avg_accuracy = accuracies[in_bin].mean()
            ece += prop_in_bin * abs(avg_accuracy - avg_confidence)
    
    return float(ece)


def compute_offline_metrics(y_true: np.ndarray, y_probs: np.ndarray) -> OfflineMetrics:
    """Compute offline classification metrics."""
    # Filter out NaN
    valid_mask = ~(np.isnan(y_true) | np.isnan(y_probs).any(axis=1))
    y_true = y_true[valid_mask].astype(int)
    y_probs = y_probs[valid_mask]
    y_pred = y_probs.argmax(axis=1)
    
    if len(y_true) == 0:
        return OfflineMetrics()
    
    metrics = OfflineMetrics(
        accuracy=float(accuracy_score(y_true, y_pred)),
        f1_macro=float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        f1_weighted=float(f1_score(y_true, y_pred, average="weighted", zero_division=0)),
        n_samples=len(y_true),
    )
    
    # Log loss (with clipping for numerical stability)
    y_probs_clipped = np.clip(y_probs, 1e-10, 1 - 1e-10)
    try:
        metrics.log_loss = float(log_loss(y_true, y_probs_clipped))
    except Exception:
        metrics.log_loss = float("nan")
    
    # Per-class accuracy
    for cls, name in [(0, "down"), (1, "flat"), (2, "up")]:
        mask = y_true == cls
        if mask.sum() > 0:
            acc = (y_pred[mask] == cls).mean()
            setattr(metrics, f"accuracy_{name}", float(acc))
    
    # ECE
    metrics.ece = compute_ece(y_true, y_probs)
    
    return metrics


def aggregate_offline_metrics(split_metrics: list[OfflineMetrics]) -> OfflineMetrics:
    """Aggregate per-split metrics into mean/std summary."""
    if not split_metrics:
        return OfflineMetrics()

    def _mean_std(vals: list[float]) -> tuple[float, float]:
        arr = np.array(vals, dtype=float)
        return float(np.mean(arr)), float(np.std(arr))

    acc_mean, acc_std = _mean_std([m.accuracy for m in split_metrics])
    f1m_mean, f1m_std = _mean_std([m.f1_macro for m in split_metrics])
    f1w_mean, f1w_std = _mean_std([m.f1_weighted for m in split_metrics])
    ll_mean, ll_std = _mean_std([m.log_loss for m in split_metrics])
    ece_mean, ece_std = _mean_std([m.ece for m in split_metrics])

    # Per-class accuracy (mean only)
    acc_d = float(np.mean([m.accuracy_down for m in split_metrics]))
    acc_f = float(np.mean([m.accuracy_flat for m in split_metrics]))
    acc_u = float(np.mean([m.accuracy_up for m in split_metrics]))

    n_samples = int(np.sum([m.n_samples for m in split_metrics]))

    return OfflineMetrics(
        accuracy=acc_mean,
        accuracy_std=acc_std,
        f1_macro=f1m_mean,
        f1_macro_std=f1m_std,
        f1_weighted=f1w_mean,
        f1_weighted_std=f1w_std,
        log_loss=ll_mean,
        log_loss_std=ll_std,
        accuracy_down=acc_d,
        accuracy_flat=acc_f,
        accuracy_up=acc_u,
        ece=ece_mean,
        ece_std=ece_std,
        n_samples=n_samples,
    )


def measure_inference_latency(
    model: Any,
    X: np.ndarray,
    n_samples: int = 100,
) -> LatencyMetrics:
    """Measure inference latency on random samples."""
    tracker = LatencyTracker()
    
    # Warm up
    if len(X) > 0:
        _ = model.predict_proba(X[:1])
    
    indices = np.random.choice(len(X), min(n_samples, len(X)), replace=False)
    
    for idx in indices:
        sample = X[idx : idx + 1]
        
        with LatencyContext(tracker, "inference"):
            _ = model.predict_proba(sample)
    
    inf_stats = tracker.get_stats("inference")
    _obs_store.observe(metric="inference.sample_loop", latency_s=float(inf_stats["mean"]), ok=True)
    
    return LatencyMetrics(
        inference_mean_ms=inf_stats["mean"] * 1000,
        inference_p95_ms=inf_stats["p95"] * 1000,
    )


# ---------------------------------------------------------------------------
# Main benchmark function
# ---------------------------------------------------------------------------

def run_benchmark(
    model_type: str,
    symbol: str,
    data_dir: Path,
    artifacts_dir: Path,
    runs_dir: Path,
    horizon: int = 50,
    include_trading: bool = False,
    n_splits: int = 5,
    min_train_samples: int = 10000,
    **model_kwargs: Any,
) -> BenchmarkReport:
    """
    Run a comprehensive benchmark for a model configuration.
    
    Args:
        model_type: Model type to benchmark
        symbol: Instrument code
        data_dir: Data directory
        artifacts_dir: Artifacts directory
        runs_dir: Runs directory
        horizon: Label horizon
        include_trading: Whether to run trading backtest (slow)
        **model_kwargs: Additional model configuration
    
    Returns:
        BenchmarkReport with all metrics
    """
    import uuid
    
    benchmark_started = time.time()
    run_id = uuid.uuid4().hex[:12]
    report = BenchmarkReport(
        run_id=run_id,
        timestamp=datetime.now().isoformat(),
        model_type=model_type,
        symbol=symbol,
        horizon=horizon,
        config={"n_splits": n_splits, "min_train_samples": min_train_samples, **model_kwargs},
    )
    
    log.info("benchmark.start", run_id=run_id, model_type=model_type, symbol=symbol)
    
    # Load data
    from ghtrader.datasets.features import read_features_manifest

    t_data = time.time()
    features_df = read_features_for_symbol(data_dir, symbol)
    labels_df = read_labels_for_symbol(data_dir, symbol)
    
    df = features_df.merge(labels_df[["datetime", f"label_{horizon}"]], on="datetime")
    _obs_store.observe(metric="run_benchmark.data_load", latency_s=(time.time() - t_data), ok=True)
    
    manifest = read_features_manifest(data_dir, symbol)
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
    
    log.info("benchmark.data_loaded", n_samples=len(df), n_features=len(feature_cols))
    
    # Walk-forward evaluation (multi-split)
    splits = create_walk_forward_splits(len(X), n_splits=n_splits, min_train_samples=min_train_samples)
    if not splits:
        log.error("benchmark.no_splits")
        _obs_store.observe(metric="run_benchmark.splits", ok=False)
        report_path = runs_dir / "benchmarks" / symbol / model_type / f"{run_id}.json"
        report.save(report_path)
        _obs_store.observe(metric="run_benchmark.total", latency_s=(time.time() - benchmark_started), ok=False)
        return report

    split_metrics: list[OfflineMetrics] = []
    last_model: Any | None = None
    last_X_test: np.ndarray | None = None

    for split in splits:
        X_train = X[split.train_start_idx:split.train_end_idx]
        y_train = y[split.train_start_idx:split.train_end_idx]
        X_test = X[split.test_start_idx:split.test_end_idx]
        y_test = y[split.test_start_idx:split.test_end_idx]
        seg_train = seg_all[split.train_start_idx:split.train_end_idx] if seg_all is not None else None
        seg_test = seg_all[split.test_start_idx:split.test_end_idx] if seg_all is not None else None

        log.info(
            "benchmark.split",
            split_id=split.split_id,
            train_size=len(X_train),
            test_size=len(X_test),
        )

        model = create_model(model_type, n_features=len(feature_cols), **model_kwargs)

        t0 = time.time()
        if model_type in ["deeplob", "transformer", "tcn", "tlob", "ssm"]:
            model.fit(X_train, y_train, epochs=30, batch_size=256, segment_id=seg_train)
        else:
            model.fit(X_train, y_train, segment_id=seg_train)
        train_time = time.time() - t0

        # Predict on test set
        y_probs = model.predict_proba(X_test, segment_id=seg_test)

        # Align outputs for sequence models (pad/trim)
        if y_probs.shape[0] < X_test.shape[0]:
            pad_size = X_test.shape[0] - y_probs.shape[0]
            y_probs = np.vstack([np.full((pad_size, 3), np.nan), y_probs])
        elif y_probs.shape[0] > X_test.shape[0]:
            y_probs = y_probs[-X_test.shape[0] :]

        m = compute_offline_metrics(y_test, y_probs)
        split_metrics.append(m)

        report.offline_splits.append(
            {
                "split_id": split.split_id,
                "train_size": int(len(X_train)),
                "test_size": int(len(X_test)),
                "train_time_sec": float(train_time),
                "offline": asdict(m),
            }
        )

        last_model = model
        last_X_test = X_test

    report.offline = aggregate_offline_metrics(split_metrics)
    log.info(
        "benchmark.offline_summary",
        accuracy_mean=f"{report.offline.accuracy:.3f}",
        accuracy_std=f"{report.offline.accuracy_std:.3f}",
        f1_macro_mean=f"{report.offline.f1_macro:.3f}",
        f1_macro_std=f"{report.offline.f1_macro_std:.3f}",
    )

    # Measure latency on the last split's model/test-set
    if last_model is not None and last_X_test is not None:
        report.latency = measure_inference_latency(last_model, last_X_test)
        log.info(
            "benchmark.latency",
            inference_mean_ms=f"{report.latency.inference_mean_ms:.2f}",
            inference_p95_ms=f"{report.latency.inference_p95_ms:.2f}",
        )
    
    # Trading metrics (optional, slow)
    if include_trading:
        try:
            # Save model temporarily
            temp_model_path = artifacts_dir / "temp" / symbol / model_type / f"model_h{horizon}.pt"
            model.save(temp_model_path)
            
            # This would require actual backtest infrastructure
            log.info("benchmark.trading_skipped", reason="requires_live_data")
        except Exception as e:
            log.warning("benchmark.trading_failed", error=str(e))
    
    # Save report
    report_path = runs_dir / "benchmarks" / symbol / model_type / f"{run_id}.json"
    report.save(report_path)
    
    log.info("benchmark.done", report_path=str(report_path))
    _obs_store.observe(metric="run_benchmark.total", latency_s=(time.time() - benchmark_started), ok=True)
    
    return report


def compare_models(
    model_types: list[str],
    symbol: str,
    data_dir: Path,
    artifacts_dir: Path,
    runs_dir: Path,
    horizon: int = 50,
) -> pd.DataFrame:
    """
    Compare multiple models on the same dataset.
    
    Returns a DataFrame with metrics for each model.
    """
    results: list[dict] = []
    
    for model_type in model_types:
        log.info("compare.running", model_type=model_type)
        
        try:
            report = run_benchmark(
                model_type=model_type,
                symbol=symbol,
                data_dir=data_dir,
                artifacts_dir=artifacts_dir,
                runs_dir=runs_dir,
                horizon=horizon,
            )
            
            results.append({
                "model": model_type,
                "accuracy": report.offline.accuracy,
                "f1_macro": report.offline.f1_macro,
                "log_loss": report.offline.log_loss,
                "ece": report.offline.ece,
                "inference_p95_ms": report.latency.inference_p95_ms,
            })
        except Exception as e:
            log.error("compare.failed", model_type=model_type, error=str(e))
            results.append({
                "model": model_type,
                "accuracy": None,
                "f1_macro": None,
                "log_loss": None,
                "ece": None,
                "inference_p95_ms": None,
            })
    
    df = pd.DataFrame(results)
    
    # Save comparison
    comparison_path = runs_dir / "benchmarks" / symbol / "comparison.csv"
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(comparison_path, index=False)
    
    log.info("compare.done", path=str(comparison_path))
    
    return df
