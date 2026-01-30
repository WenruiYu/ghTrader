from __future__ import annotations

import pandas as pd


def test_compute_orderbook_invariants_counts():
    from ghtrader.data.field_quality import compute_orderbook_invariants

    df = pd.DataFrame(
        {
            "bid_price1": [100, 100, 100],
            "ask_price1": [101, 99, 101],
            "bid_price2": [99, 101, 98],
            "ask_price2": [102, 100, 100],
            "bid_volume1": [1, -1, 1],
            "ask_volume1": [1, 1, 1],
            "volume": [10, 10, 10],
            "last_price": [100, 100, -1],
        }
    )

    out = compute_orderbook_invariants(df)

    assert out["bid_ask_cross_violations"] == 1
    assert out["bid_order_violations"] == 1
    assert out["ask_order_violations"] == 1
    assert out["negative_volume_violations"] == 1
    assert out["negative_price_violations"] == 1
