from __future__ import annotations

from datetime import date

from ghtrader.labels import build_labels_for_symbol, read_labels_for_symbol
from ghtrader.integrity import read_sha256_sidecar
from ghtrader.lake import write_ticks_partition


def test_compute_mid_price_smoke(small_synthetic_tick_df):
    from ghtrader.labels import compute_mid_price

    mid = compute_mid_price(small_synthetic_tick_df)
    assert len(mid) == len(small_synthetic_tick_df)
    assert (mid > 0).all()


def test_build_labels_writes_partitions_and_is_readable(synthetic_lake):
    data_dir, symbol, _dates = synthetic_lake

    out_root = build_labels_for_symbol(
        symbol=symbol,
        data_dir=data_dir,
        horizons=[10, 50],
        threshold_k=1,
    )
    assert out_root.exists()

    df = read_labels_for_symbol(data_dir, symbol)
    assert len(df) > 0
    assert "datetime" in df.columns
    assert "mid" in df.columns
    assert "label_10" in df.columns
    assert "label_50" in df.columns


def test_build_labels_is_idempotent_without_overwrite(synthetic_lake):
    """
    Incremental default: running build twice should not create duplicate per-day partitions.
    """
    data_dir, symbol, dates = synthetic_lake

    out_root = build_labels_for_symbol(
        symbol=symbol,
        data_dir=data_dir,
        horizons=[10, 50],
        threshold_k=1,
    )
    assert out_root.exists()

    day0 = out_root / f"date={dates[0].isoformat()}"
    day1 = out_root / f"date={dates[1].isoformat()}"
    files0 = sorted([p.name for p in day0.glob("*.parquet")])
    files1 = sorted([p.name for p in day1.glob("*.parquet")])
    assert files0 and files1

    out_root2 = build_labels_for_symbol(
        symbol=symbol,
        data_dir=data_dir,
        horizons=[10, 50],
        threshold_k=1,
    )
    assert out_root2 == out_root
    files0b = sorted([p.name for p in day0.glob("*.parquet")])
    files1b = sorted([p.name for p in day1.glob("*.parquet")])
    assert files0b == files0
    assert files1b == files1


def test_build_labels_appends_new_day_and_recomputes_prev_day(tmp_path, small_synthetic_tick_df):
    """
    Labels have cross-day lookahead: when a new tick day is appended, we must recompute
    the previous day so its end-of-day horizons can use the new day's future ticks.
    """
    data_dir = tmp_path / "data"
    symbol = "SHFE.cu2501"

    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)

    # Start with only day0 ticks
    df0 = small_synthetic_tick_df.copy()
    df0["symbol"] = symbol
    write_ticks_partition(df0, data_dir=data_dir, symbol=symbol, dt=d0, part_id="d0")

    out_root = build_labels_for_symbol(symbol=symbol, data_dir=data_dir, horizons=[10, 50], threshold_k=1)
    p0 = out_root / f"date={d0.isoformat()}" / "part.parquet"
    assert p0.exists()
    sha0_before = read_sha256_sidecar(p0)
    assert sha0_before

    # Append day1 ticks
    df1 = small_synthetic_tick_df.copy()
    df1["symbol"] = symbol
    df1["datetime"] = df1["datetime"].astype("int64") + 10_000_000_000
    write_ticks_partition(df1, data_dir=data_dir, symbol=symbol, dt=d1, part_id="d1")

    out_root2 = build_labels_for_symbol(symbol=symbol, data_dir=data_dir, horizons=[10, 50], threshold_k=1)
    assert out_root2 == out_root
    p1 = out_root / f"date={d1.isoformat()}" / "part.parquet"
    assert p1.exists()

    # Day0 must be recomputed (lookahead now available); file should change.
    assert read_sha256_sidecar(p0) != sha0_before


