from __future__ import annotations

from ghtrader.labels import build_labels_for_symbol, read_labels_for_symbol


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

