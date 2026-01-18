from __future__ import annotations

import numpy as np


def test_compute_mid_price_smoke(small_synthetic_tick_df):
    from ghtrader.datasets.labels import compute_mid_price

    mid = compute_mid_price(small_synthetic_tick_df)
    assert len(mid) == len(small_synthetic_tick_df)
    assert (mid > 0).all()


def test_compute_multi_horizon_labels_smoke(small_synthetic_tick_df):
    from ghtrader.datasets.labels import compute_multi_horizon_labels

    out = compute_multi_horizon_labels(small_synthetic_tick_df, horizons=[10, 50], threshold_k=1, price_tick=1.0)
    assert list(out.columns) == ["mid", "label_10", "label_50"]
    assert len(out) == len(small_synthetic_tick_df)

    # Label ranges: {0,1,2} plus NaN tail where future is unavailable.
    for col, h in [("label_10", 10), ("label_50", 50)]:
        s = out[col]
        assert s.iloc[-h:].isna().all()
        assert set(s.dropna().unique()).issubset({0.0, 1.0, 2.0})

    # Some non-NaN labels should exist for horizon=10 with n=50 ticks.
    assert np.isfinite(out["label_10"].iloc[:-10].to_numpy(dtype="float64", copy=False)).any()


