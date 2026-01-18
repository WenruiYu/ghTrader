from __future__ import annotations

import numpy as np
import pandas as pd


def test_compute_field_null_rates_df_counts_nan_and_non_positive_prices() -> None:
    from ghtrader.data.field_quality import compute_field_null_rates_df

    df = pd.DataFrame(
        {
            "bid_price1": [1.0] * 90 + [np.nan] * 10,
            "ask_price1": [2.0] * 95 + [0.0] * 5,  # 0.0 should count as missing for prices
            "last_price": [1.5] * 100,
            "volume": [10.0] * 100,
            "open_interest": [100.0] * 100,
        }
    )
    rates = compute_field_null_rates_df(df, ["bid_price1", "ask_price1", "last_price"])
    assert abs(rates["bid_price1"] - 0.10) < 1e-9
    assert abs(rates["ask_price1"] - 0.05) < 1e-9
    assert abs(rates["last_price"] - 0.00) < 1e-9


def test_detect_consecutive_missing_segments_df_detects_long_run() -> None:
    from ghtrader.data.field_quality import detect_consecutive_missing_segments_df

    n = 250
    bid = np.ones(n, dtype="float64")
    bid[50:161] = np.nan  # 111 consecutive missing
    df = pd.DataFrame({"datetime": np.arange(n, dtype="int64"), "bid_price1": bid})

    segs = detect_consecutive_missing_segments_df(df, "bid_price1", min_run_len=100)
    assert len(segs) == 1
    s = segs[0]
    assert s["start_idx"] == 50
    assert s["end_idx"] == 160
    assert s["run_len"] == 111
    assert s["start_datetime_ns"] == 50
    assert s["end_datetime_ns"] == 160


def test_outlier_detectors_are_nonzero_on_constructed_anomalies() -> None:
    from ghtrader.data.field_quality import (
        count_price_jump_outliers_df,
        count_spread_anomaly_outliers_df,
        count_volume_spike_outliers_df,
    )

    n = 1200
    # Construct mid-price with small variability so rolling std is non-zero.
    mid = 100.0 + (np.arange(n) % 2) * 0.01
    # Inject a large jump
    mid[800] = mid[799] + 5.0
    bid = mid - 0.01
    ask = mid + 0.01

    volume = np.ones(n, dtype="float64") * 10.0
    volume[900] = 1000.0  # spike

    # Spread anomaly: widen spread at one point
    ask2 = ask.copy()
    ask2[1000] = bid[1000] + 10.0

    df = pd.DataFrame(
        {
            "bid_price1": bid,
            "ask_price1": ask2,
            "volume": volume,
        }
    )

    assert count_price_jump_outliers_df(df) >= 1
    assert count_volume_spike_outliers_df(df) >= 1
    assert count_spread_anomaly_outliers_df(df) >= 1

