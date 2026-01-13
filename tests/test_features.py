from __future__ import annotations

from datetime import date

import numpy as np

from ghtrader.features import FactorEngine, MicropriceFactor, read_features_for_symbol
from ghtrader.integrity import read_sha256_sidecar
from ghtrader.lake import write_ticks_partition


def test_microprice_factor(small_synthetic_tick_df):
    f = MicropriceFactor()
    out = f.compute_batch(small_synthetic_tick_df)
    # compute_batch may return ndarray-like (via np.where) or Series; normalize to array
    arr = np.asarray(out)
    assert arr.shape[0] == len(small_synthetic_tick_df)
    assert np.isfinite(arr).any()


def test_build_features_writes_partitions_and_is_readable(synthetic_lake):
    data_dir, symbol, _dates = synthetic_lake

    engine = FactorEngine()
    out_root = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)
    assert out_root.exists()

    df = read_features_for_symbol(data_dir, symbol)
    assert len(df) > 0
    assert "datetime" in df.columns
    assert "spread" in df.columns


def test_build_features_is_idempotent_without_overwrite(synthetic_lake):
    """
    Incremental default: running build twice should not create duplicate per-day partitions.
    """
    data_dir, symbol, dates = synthetic_lake

    engine = FactorEngine()
    out_root = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)
    assert out_root.exists()

    # Capture per-date partition files
    day0 = out_root / f"date={dates[0].isoformat()}"
    day1 = out_root / f"date={dates[1].isoformat()}"
    files0 = sorted([p.name for p in day0.glob("*.parquet")])
    files1 = sorted([p.name for p in day1.glob("*.parquet")])
    assert files0 and files1

    # Second run should keep the same per-date file names (no duplicates)
    out_root2 = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)
    assert out_root2 == out_root
    files0b = sorted([p.name for p in day0.glob("*.parquet")])
    files1b = sorted([p.name for p in day1.glob("*.parquet")])
    assert files0b == files0
    assert files1b == files1


def test_build_features_appends_new_day_incrementally(tmp_path, small_synthetic_tick_df):
    """
    Incremental default: adding a new tick day should create only that new features partition,
    without rewriting existing days.
    """
    data_dir = tmp_path / "data"
    symbol = "SHFE.cu2501"

    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)
    d2 = date(2025, 1, 3)

    df0 = small_synthetic_tick_df.copy()
    df0["symbol"] = symbol
    write_ticks_partition(df0, data_dir=data_dir, symbol=symbol, dt=d0, part_id="d0")

    df1 = small_synthetic_tick_df.copy()
    df1["symbol"] = symbol
    df1["datetime"] = df1["datetime"].astype("int64") + 10_000_000_000  # shift 10s
    write_ticks_partition(df1, data_dir=data_dir, symbol=symbol, dt=d1, part_id="d1")

    engine = FactorEngine()
    out_root = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)

    p0 = out_root / f"date={d0.isoformat()}" / "part.parquet"
    p1 = out_root / f"date={d1.isoformat()}" / "part.parquet"
    assert p0.exists() and p1.exists()
    sha0_before = read_sha256_sidecar(p0)
    sha1_before = read_sha256_sidecar(p1)
    assert sha0_before and sha1_before

    # Append a new tick day
    df2 = small_synthetic_tick_df.copy()
    df2["symbol"] = symbol
    df2["datetime"] = df2["datetime"].astype("int64") + 20_000_000_000  # shift 20s
    write_ticks_partition(df2, data_dir=data_dir, symbol=symbol, dt=d2, part_id="d2")

    out_root2 = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)
    assert out_root2 == out_root

    p2 = out_root / f"date={d2.isoformat()}" / "part.parquet"
    assert p2.exists()

    # Existing partitions should not be rewritten
    assert read_sha256_sidecar(p0) == sha0_before
    assert read_sha256_sidecar(p1) == sha1_before


def test_build_features_backfill_recomputes_next_day(tmp_path, small_synthetic_tick_df):
    """
    Backfill guard: if a previously-missing tick day appears, we must recompute that day and
    also recompute the next available tick day (to refresh lookback-dependent rows).
    """
    data_dir = tmp_path / "data"
    symbol = "SHFE.cu2501"

    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)  # initially missing
    d2 = date(2025, 1, 3)

    # Day0 ticks ~70000
    df0 = small_synthetic_tick_df.copy()
    df0["symbol"] = symbol
    write_ticks_partition(df0, data_dir=data_dir, symbol=symbol, dt=d0, part_id="d0")

    # Day2 ticks ~70000 (later)
    df2 = small_synthetic_tick_df.copy()
    df2["symbol"] = symbol
    df2["datetime"] = df2["datetime"].astype("int64") + 20_000_000_000
    write_ticks_partition(df2, data_dir=data_dir, symbol=symbol, dt=d2, part_id="d2")

    engine = FactorEngine()
    out_root = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)

    p2 = out_root / f"date={d2.isoformat()}" / "part.parquet"
    assert p2.exists()
    sha2_before = read_sha256_sidecar(p2)
    assert sha2_before

    # Backfill Day1 with a very different price regime to ensure Day2 early-window features change.
    df1 = small_synthetic_tick_df.copy()
    df1["symbol"] = symbol
    df1["datetime"] = df1["datetime"].astype("int64") + 10_000_000_000
    df1["last_price"] = df1["last_price"] + 10_000.0
    df1["average"] = df1["average"] + 10_000.0
    df1["highest"] = df1["highest"] + 10_000.0
    df1["lowest"] = df1["lowest"] + 10_000.0
    for lvl in range(1, 6):
        df1[f"bid_price{lvl}"] = df1[f"bid_price{lvl}"] + 10_000.0
        df1[f"ask_price{lvl}"] = df1[f"ask_price{lvl}"] + 10_000.0
    write_ticks_partition(df1, data_dir=data_dir, symbol=symbol, dt=d1, part_id="d1")

    # Incremental rebuild should recompute D1 and also rewrite D2.
    out_root2 = engine.build_features_for_symbol(symbol=symbol, data_dir=data_dir)
    assert out_root2 == out_root
    assert (out_root / f"date={d1.isoformat()}" / "part.parquet").exists()
    assert read_sha256_sidecar(p2) != sha2_before

