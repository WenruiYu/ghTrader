from __future__ import annotations

import numpy as np

from ghtrader.features import FactorEngine, MicropriceFactor


def test_microprice_factor(small_synthetic_tick_df):
    f = MicropriceFactor()
    out = f.compute_batch(small_synthetic_tick_df)
    # compute_batch may return ndarray-like (via np.where) or Series; normalize to array
    arr = np.asarray(out)
    assert arr.shape[0] == len(small_synthetic_tick_df)
    assert np.isfinite(arr).any()


def test_factor_engine_compute_batch_smoke(small_synthetic_tick_df):
    eng = FactorEngine(enabled_factors=["spread", "microprice"])
    feats = eng.compute_batch(small_synthetic_tick_df)
    assert list(feats.columns) == ["spread", "microprice"]
    assert len(feats) == len(small_synthetic_tick_df)
    assert np.isfinite(np.asarray(feats["spread"])).any()


def test_factor_engine_compute_incremental_smoke(synthetic_tick_dict):
    eng = FactorEngine(enabled_factors=["spread", "microprice"])
    out = eng.compute_incremental(synthetic_tick_dict)
    assert set(out.keys()) == {"spread", "microprice"}
    assert all(isinstance(v, float) for v in out.values())

