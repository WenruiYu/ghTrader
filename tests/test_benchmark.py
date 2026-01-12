from __future__ import annotations

from pathlib import Path

from ghtrader.benchmark import run_benchmark
from ghtrader.features import FactorEngine
from ghtrader.labels import build_labels_for_symbol


def test_run_benchmark_multi_split_reports_mean_std(synthetic_lake, tmp_path: Path):
    data_dir, symbol, _dates = synthetic_lake
    artifacts_dir = tmp_path / "artifacts"
    runs_dir = tmp_path / "runs"

    # Build derived datasets for benchmark
    # Use a horizon smaller than the per-day tick count so the last day still has
    # non-NaN labels (walk-forward test windows hit the tail).
    build_labels_for_symbol(symbol=symbol, data_dir=data_dir, horizons=[10], threshold_k=1)
    FactorEngine().build_features_for_symbol(symbol=symbol, data_dir=data_dir)

    report = run_benchmark(
        model_type="logistic",
        symbol=symbol,
        data_dir=data_dir,
        artifacts_dir=artifacts_dir,
        runs_dir=runs_dir,
        horizon=10,
        n_splits=2,
        min_train_samples=10,
    )

    assert report.offline.n_samples > 0
    assert report.offline.accuracy >= 0.0
    assert report.offline.accuracy_std >= 0.0
    assert len(report.offline_splits) > 0

