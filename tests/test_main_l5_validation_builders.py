from __future__ import annotations

from datetime import date


def _sec_to_iso(sec: int) -> str:
    return f"ts-{int(sec)}"


def test_build_day_validation_artifacts_manifest_empty() -> None:
    from ghtrader.data.main_l5_validation_builders import build_day_validation_artifacts

    day_out, summary_row, gap_rows = build_day_validation_artifacts(
        symbol="KQ.m@SHFE.cu",
        trading_day=date(2026, 1, 1),
        underlying_contract="SHFE.cu2602",
        cadence_mode="event",
        two_plus_ratio=0.0,
        expected_seconds=10,
        expected_seconds_strict=8,
        observed_seconds_in_sessions=6,
        seconds_with_one_tick=3,
        seconds_with_two_plus=1,
        summary_seconds_with_ticks=6,
        summary_seconds_with_two_plus=1,
        ticks_outside_sessions_seconds=2,
        last_tick_sec=100,
        session_end_sec=110,
        session_end_lag_s=10,
        missing_seconds=2,
        missing_half_seconds=0,
        boundary_missing_seconds=1,
        missing_segments_total=1,
        missing_segments=[{"session": "day", "start_ts": "2026-01-01T00:00:00+00:00", "end_ts": "2026-01-01T00:00:01+00:00", "duration_s": 2, "tqsdk_status": "unchecked"}],
        gap_buckets={"2_5": 1, "6_15": 0, "16_30": 0, "gt_30": 0},
        gap_buckets_by_session={"day": {"2_5": 1, "6_15": 0, "16_30": 0, "gt_30": 0}},
        gap_count_gt_30s=0,
        missing_day=False,
        observed_segments=2,
        max_gap_s=2,
        gap_threshold_s=2.0,
        schedule_hash="hash1",
        row_counts={"2026-01-01": 0},
        sec_to_iso=_sec_to_iso,
    )

    assert day_out["manifest_rows"] == 0
    assert day_out["tqsdk_inference"] == "provider_empty_day"
    assert day_out["missing_segments_total"] == 1
    assert day_out["_has_gap"] is True
    assert day_out["_has_outside"] is True
    assert summary_row.total_segments == 3
    assert len(gap_rows) == 1
    assert gap_rows[0].duration_s == 2


def test_build_day_validation_artifacts_manifest_partial_and_half_flag() -> None:
    from ghtrader.data.main_l5_validation_builders import build_day_validation_artifacts

    day_out, summary_row, gap_rows = build_day_validation_artifacts(
        symbol="KQ.m@SHFE.cu",
        trading_day=date(2026, 1, 2),
        underlying_contract="SHFE.cu2603",
        cadence_mode="fixed_0p5s",
        two_plus_ratio=0.95,
        expected_seconds=10,
        expected_seconds_strict=8,
        observed_seconds_in_sessions=8,
        seconds_with_one_tick=0,
        seconds_with_two_plus=8,
        summary_seconds_with_ticks=8,
        summary_seconds_with_two_plus=8,
        ticks_outside_sessions_seconds=0,
        last_tick_sec=None,
        session_end_sec=None,
        session_end_lag_s=None,
        missing_seconds=0,
        missing_half_seconds=1,
        boundary_missing_seconds=0,
        missing_segments_total=0,
        missing_segments=[],
        gap_buckets={"2_5": 0, "6_15": 0, "16_30": 0, "gt_30": 0},
        gap_buckets_by_session={},
        gap_count_gt_30s=0,
        missing_day=False,
        observed_segments=1,
        max_gap_s=0,
        gap_threshold_s=2.0,
        schedule_hash="hash2",
        row_counts={"2026-01-02": 123},
        sec_to_iso=_sec_to_iso,
    )

    assert day_out["manifest_rows"] == 123
    assert day_out["tqsdk_inference"] == "unknown_partial"
    assert day_out["_has_half"] is True
    assert summary_row.missing_half_seconds == 1
    assert summary_row.seconds_with_ticks == 8
    assert len(gap_rows) == 0

