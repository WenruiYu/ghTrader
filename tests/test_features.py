from __future__ import annotations

import numpy as np

from ghtrader.features import FactorEngine, MicropriceFactor, read_features_for_symbol


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

