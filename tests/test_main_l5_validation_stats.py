from __future__ import annotations

from ghtrader.data.main_l5_validation_stats import compute_day_gap_stats


def _iso(sec: int) -> str:
    return f"ts-{int(sec)}"


def test_compute_day_gap_stats_event_mode_no_segment_under_threshold() -> None:
    intervals = [{"session": "day", "start_sec": 0, "end_sec": 5}]
    sec_counts = {1: 1, 4: 1}
    gap_bucket_defs = [("2_5", 2, 5), ("6_15", 6, 15), ("16_30", 16, 30), ("gt_30", 31, None)]

    out = compute_day_gap_stats(
        intervals=intervals,
        sec_counts=sec_counts,
        day_cadence="event",
        gap_threshold_s=5.0,
        gap_bucket_defs=gap_bucket_defs,
        max_segments_per_day=100,
        sec_to_iso=_iso,
    )

    assert out.expected_seconds == 6
    assert out.expected_seconds_strict == 4
    assert out.day_missing_seconds == 2
    assert out.day_segments_total == 0
    assert out.day_gap_buckets["2_5"] == 0
    assert out.gap_count_gt_30_total_delta == 0
    assert out.observed_segments_day == 1
    assert out.session_end_lag_s == 1


def test_compute_day_gap_stats_fixed_mode_bucket_and_half_seconds() -> None:
    intervals = [{"session": "day", "start_sec": 0, "end_sec": 5}]
    sec_counts = {1: 2, 4: 2}
    gap_bucket_defs = [("2_5", 2, 5), ("6_15", 6, 15), ("16_30", 16, 30), ("gt_30", 31, None)]

    out = compute_day_gap_stats(
        intervals=intervals,
        sec_counts=sec_counts,
        day_cadence="fixed_0p5s",
        gap_threshold_s=2.0,
        gap_bucket_defs=gap_bucket_defs,
        max_segments_per_day=100,
        sec_to_iso=_iso,
    )

    assert out.day_missing_half_seconds == 2
    assert out.day_segments_total == 1
    assert out.day_gap_buckets["2_5"] == 1
    assert out.gap_bucket_totals_delta["2_5"] == 1
    assert out.day_missing_segments[0]["start_ts"] == "ts-2"
    assert out.day_missing_segments[0]["end_ts"] == "ts-3"

