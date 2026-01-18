from __future__ import annotations

import pandas as pd


def test_compute_intervals_and_summary() -> None:
    from ghtrader.data.gap_detection import compute_inter_tick_intervals_ms, summarize_intervals_ms

    df = pd.DataFrame({"datetime": [0, 1_000_000_000, 3_000_000_000]})
    intervals = compute_inter_tick_intervals_ms(df)
    assert list(intervals.values) == [1000.0, 2000.0]

    stats = summarize_intervals_ms(intervals)
    assert stats["max_ms"] == 2000
    assert stats["median_ms"] == 1500


def test_gap_counts_and_largest_gap_metadata() -> None:
    from ghtrader.data.gap_detection import analyze_intraday_gaps_df

    df = pd.DataFrame({"datetime": [0, 1_000_000_000, 61_000_000_000, 62_000_000_000]})
    rep = analyze_intraday_gaps_df(df, abnormal_threshold_ms=60_000, critical_threshold_ms=5 * 60_000)
    assert rep["tick_count_actual"] == 4
    assert rep["abnormal_gaps_count"] >= 1
    assert rep["critical_gaps_count"] == 0
    assert rep["largest_gap_duration_ms"] == 60_000
    assert rep["largest_gap_end_ns"] == 61_000_000_000

