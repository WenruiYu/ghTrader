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


def test_gap_session_whitelist_excludes_cross_session_gap() -> None:
    from ghtrader.data.gap_detection import analyze_intraday_gaps_df

    ts1 = pd.Timestamp("2026-01-16 15:00:00", tz="Asia/Shanghai").value
    ts2 = pd.Timestamp("2026-01-16 21:05:00", tz="Asia/Shanghai").value
    df = pd.DataFrame({"datetime": [ts1, ts2]})
    rep = analyze_intraday_gaps_df(
        df,
        symbol="SHFE.cu2602",
        exchange="SHFE",
        var="cu",
        session_whitelist=True,
        abnormal_threshold_ms=60_000,
        critical_threshold_ms=5 * 60_000,
    )
    assert rep["abnormal_gaps_count"] == 0
    assert rep["critical_gaps_count"] == 0
    assert int(rep.get("session_filtered_intervals", 0)) >= 1


def test_session_core_mask_warmup_cooldown() -> None:
    from ghtrader.data.gap_detection import _session_ids_and_core_mask_for_datetimes

    ts1 = pd.Timestamp("2026-01-16 09:02:00", tz="Asia/Shanghai").value
    ts2 = pd.Timestamp("2026-01-16 09:07:00", tz="Asia/Shanghai").value
    ts3 = pd.Timestamp("2026-01-16 10:12:00", tz="Asia/Shanghai").value
    dt = pd.Series([ts1, ts2, ts3])
    session_ids, ok = _session_ids_and_core_mask_for_datetimes(
        dt,
        symbol="SHFE.cu2602",
        exchange="SHFE",
        var="cu",
        warmup_min=5,
        cooldown_min=5,
    )
    assert session_ids[0] > 0 and ok[0] is False
    assert session_ids[1] > 0 and ok[1] is True
    assert session_ids[2] > 0 and ok[2] is False


def test_expected_tick_count_range_from_samples() -> None:
    from ghtrader.data.gap_detection import _expected_tick_count_range_from_samples

    exp_min, exp_max, meta = _expected_tick_count_range_from_samples([100] * 10)
    assert exp_min == 100
    assert exp_max == 100
    assert meta["sample_count"] == 10
    assert meta["p10"] == 100
    assert meta["p90"] == 100

    exp_min, exp_max, meta = _expected_tick_count_range_from_samples([100] * 9)
    assert exp_min is None
    assert exp_max is None
    assert meta["sample_count"] == 9

