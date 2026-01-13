from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ghtrader.db import DuckDBBackend, DuckDBConfig
from ghtrader.features import FactorEngine
from ghtrader.labels import build_labels_for_symbol


def test_duckdb_views_and_simple_join(synthetic_lake, tmp_path: Path) -> None:
    data_dir, symbol, _dates = synthetic_lake

    # Build minimal features + labels so the DuckDB layer can query them.
    build_labels_for_symbol(symbol=symbol, data_dir=data_dir, horizons=[10], ticks_lake="raw")
    FactorEngine(enabled_factors=["return_10"]).build_features_for_symbol(symbol=symbol, data_dir=data_dir, ticks_lake="raw")

    backend = DuckDBBackend(config=DuckDBConfig(db_path=None, read_only=False))
    with backend.connect() as con:
        views = backend.init_views(con=con, data_dir=data_dir)
        assert "ticks_raw_v1" in views
        assert "features_all" in views
        assert "labels_all" in views

        # Basic tick count is stable.
        n_ticks = con.execute("SELECT COUNT(*) FROM ticks_raw_v1").fetchone()[0]
        assert int(n_ticks) > 0

        # Join features + labels by datetime; should align 1:1 for built ranges.
        q = f"""
        SELECT COUNT(*) AS n
        FROM features_all f
        JOIN labels_all l USING(datetime)
        WHERE f.symbol = '{symbol}' AND l.symbol = '{symbol}'
        """
        n = con.execute(q).fetchone()[0]
        assert int(n) > 0


def test_duckdb_run_metrics_ingest(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    # Fake backtest metrics: runs/<symbol>/<model>/<run_id>/metrics.json
    p_bt = runs_dir / "SHFE.cu2501" / "xgboost" / "20250101_000000" / "metrics.json"
    p_bt.parent.mkdir(parents=True, exist_ok=True)
    p_bt.write_text(json.dumps({"created_at": "2025-01-01T00:00:00Z", "total_pnl": 1.23}, indent=2))

    # Fake benchmark report: runs/benchmarks/<symbol>/<model>/<run_id>.json
    p_bm = runs_dir / "benchmarks" / "SHFE.cu2501" / "xgboost" / "run123.json"
    p_bm.parent.mkdir(parents=True, exist_ok=True)
    p_bm.write_text(json.dumps({"run_id": "run123", "timestamp": "2025-01-01T00:00:00", "horizon": 50}, indent=2))

    backend = DuckDBBackend(config=DuckDBConfig(db_path=None, read_only=False))
    with backend.connect() as con:
        backend.init_metrics_tables(con=con)
        n = backend.ingest_runs_metrics(con=con, runs_dir=runs_dir)
        assert int(n) == 2

        df = con.execute("SELECT kind, symbol, model, run_id FROM run_metrics ORDER BY kind").df()
        assert isinstance(df, pd.DataFrame)
        assert df["kind"].tolist() == ["backtest", "benchmark"]

