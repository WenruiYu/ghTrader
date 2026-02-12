from __future__ import annotations

import pandas as pd


def test_compute_fill_labels_for_day_basic() -> None:
    from ghtrader.fill_labels import compute_fill_labels_for_day

    df = pd.DataFrame(
        {
            "datetime": [1, 2, 3, 4],
            "bid_price1": [99.0, 100.0, 101.0, 99.0],
            "ask_price1": [100.0, 101.0, 102.0, 98.0],
            "bid_volume1": [10.0, 10.0, 10.0, 10.0],
            "ask_volume1": [12.0, 12.0, 12.0, 12.0],
            "row_hash": [11, 12, 13, 14],
        }
    )

    out = compute_fill_labels_for_day(
        df=df,
        symbol="KQ.m@SHFE.cu",
        trading_day="2025-01-01",
        horizons=[1, 3],
        price_levels=[0],
        price_tick=1.0,
    )

    assert len(out) == 8  # 4 ticks * 2 sides * 1 level

    bid0 = out[(out["datetime_ns"] == 1) & (out["side"] == "bid") & (out["price_level"] == 0)].iloc[0]
    assert bid0["fill_prob_1"] == 0.0
    assert pd.isna(bid0["time_to_fill_1"])
    assert bid0["fill_prob_3"] == 1.0
    assert int(bid0["time_to_fill_3"]) == 3

    ask0 = out[(out["datetime_ns"] == 1) & (out["side"] == "ask") & (out["price_level"] == 0)].iloc[0]
    assert ask0["fill_prob_1"] == 1.0
    assert int(ask0["time_to_fill_1"]) == 1


def test_compute_fill_labels_price_level_offset_and_queue_position() -> None:
    from ghtrader.fill_labels import compute_fill_labels_for_day

    df = pd.DataFrame(
        {
            "datetime": [10, 20],
            "bid_price1": [100.0, 100.0],
            "ask_price1": [101.0, 101.0],
            "bid_volume1": [5.0, 5.0],
            "ask_volume1": [7.0, 7.0],
            "row_hash": [1, 2],
        }
    )

    out = compute_fill_labels_for_day(
        df=df,
        symbol="KQ.m@SHFE.cu",
        trading_day="2025-01-02",
        horizons=[1],
        price_levels=[1],
        price_tick=1.0,
    )

    bid = out[(out["datetime_ns"] == 10) & (out["side"] == "bid")].iloc[0]
    ask = out[(out["datetime_ns"] == 10) & (out["side"] == "ask")].iloc[0]

    assert float(bid["order_price"]) == 99.0
    assert float(ask["order_price"]) == 102.0
    assert float(bid["queue_position"]) == 10.0  # bid_volume1 * (1 + level)
    assert float(ask["queue_position"]) == 14.0  # ask_volume1 * (1 + level)

