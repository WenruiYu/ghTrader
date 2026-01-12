from __future__ import annotations

import numpy as np

from ghtrader.online import DelayedLabelBuffer, OnlineCalibrator


def test_online_calibrator_predict_proba_shape():
    c = OnlineCalibrator(n_classes=3, n_inputs=11)
    x = np.random.randn(11)
    p = c.predict_proba(x)
    assert p.shape == (3,)
    assert abs(float(p.sum()) - 1.0) < 1e-6


def test_delayed_label_buffer_emits_samples():
    buf = DelayedLabelBuffer(horizon_ticks=3, price_tick=1.0, threshold_k=1)
    x = np.random.randn(11)
    buf.add_prediction(features=x, mid_price=100.0)
    # 3 ticks later, should emit a label
    buf.tick(100.0)
    buf.tick(100.0)
    ready = buf.tick(102.0)
    assert len(ready) == 1
    feat, label = ready[0]
    assert feat.shape == (11,)
    assert label in (0, 1, 2)

