from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable

from ghtrader.questdb.main_l5_validate import MainL5ValidateGapRow, MainL5ValidateSummaryRow


def _manifest_day_inference(*, row_counts: dict[str, Any], trading_day: str) -> tuple[Any, str]:
    manifest_rows = None
    manifest_inference = "unknown"
    if row_counts:
        manifest_rows = row_counts.get(str(trading_day))
        if manifest_rows == 0:
            manifest_inference = "provider_empty_day"
        elif manifest_rows is not None:
            manifest_inference = "unknown_partial"
    return manifest_rows, manifest_inference


def build_day_validation_artifacts(
    *,
    symbol: str,
    trading_day: date,
    underlying_contract: str,
    cadence_mode: str,
    two_plus_ratio: float,
    expected_seconds: int,
    expected_seconds_strict: int,
    observed_seconds_in_sessions: int,
    seconds_with_one_tick: int,
    seconds_with_two_plus: int,
    summary_seconds_with_ticks: int,
    summary_seconds_with_two_plus: int,
    ticks_outside_sessions_seconds: int,
    last_tick_sec: int | None,
    session_end_sec: int | None,
    session_end_lag_s: int | None,
    missing_seconds: int,
    missing_half_seconds: int,
    boundary_missing_seconds: int,
    missing_segments_total: int,
    missing_segments: list[dict[str, Any]],
    gap_buckets: dict[str, int],
    gap_buckets_by_session: dict[str, dict[str, int]],
    gap_count_gt_30s: int,
    missing_day: bool,
    observed_segments: int,
    max_gap_s: int,
    gap_threshold_s: float,
    schedule_hash: str,
    row_counts: dict[str, Any],
    sec_to_iso: Callable[[int], str],
) -> tuple[dict[str, Any], MainL5ValidateSummaryRow, list[MainL5ValidateGapRow]]:
    day_iso = trading_day.isoformat()
    manifest_rows, manifest_inference = _manifest_day_inference(row_counts=row_counts, trading_day=day_iso)

    day_out = {
        "trading_day": day_iso,
        "underlying_contract": underlying_contract,
        "cadence_mode": cadence_mode,
        "two_plus_ratio": float(two_plus_ratio),
        "expected_seconds": int(expected_seconds),
        "expected_seconds_strict": int(expected_seconds_strict),
        "observed_seconds": int(observed_seconds_in_sessions),
        "seconds_with_one_tick": int(seconds_with_one_tick),
        "seconds_with_two_plus": int(seconds_with_two_plus),
        "ticks_outside_sessions_seconds": int(ticks_outside_sessions_seconds),
        "last_tick_ts": (sec_to_iso(last_tick_sec) if last_tick_sec is not None else None),
        "session_end_ts": (sec_to_iso(session_end_sec) if session_end_sec is not None else None),
        "session_end_lag_s": (int(session_end_lag_s) if session_end_lag_s is not None else None),
        "missing_seconds": int(missing_seconds),
        "missing_half_seconds": int(missing_half_seconds),
        "boundary_missing_seconds": int(boundary_missing_seconds),
        "missing_segments_total": int(missing_segments_total),
        "missing_segments": list(missing_segments),
        "missing_segments_truncated": bool(int(missing_segments_total) > len(missing_segments)),
        "missing_seconds_ratio": (
            float(missing_seconds) / float(expected_seconds)
            if int(expected_seconds) > 0
            else 0.0
        ),
        "gap_buckets": dict(gap_buckets),
        "gap_buckets_by_session": dict(gap_buckets_by_session),
        "gap_count_gt_30s": int(gap_count_gt_30s),
        "manifest_rows": manifest_rows,
        "tqsdk_inference": manifest_inference,
    }
    day_out["_has_gap"] = bool(int(missing_segments_total) > 0)
    day_out["_has_half"] = bool(int(missing_half_seconds) > 0 and str(cadence_mode) == "fixed_0p5s")
    day_out["_has_outside"] = bool(int(ticks_outside_sessions_seconds) > 0)
    day_out["_has_missing_day"] = bool(missing_day)

    total_segments = int(observed_segments) + int(missing_segments_total)
    summary_row = MainL5ValidateSummaryRow(
        symbol=str(symbol),
        trading_day=day_iso,
        cadence_mode=str(cadence_mode),
        expected_seconds=int(expected_seconds),
        expected_seconds_strict=int(expected_seconds_strict),
        seconds_with_ticks=int(summary_seconds_with_ticks),
        seconds_with_two_plus=int(summary_seconds_with_two_plus),
        two_plus_ratio=float(two_plus_ratio),
        observed_segments=int(observed_segments),
        total_segments=int(total_segments),
        missing_day=int(bool(missing_day)),
        missing_segments=int(missing_segments_total),
        missing_seconds=int(missing_seconds),
        missing_seconds_ratio=(
            float(missing_seconds) / float(expected_seconds)
            if int(expected_seconds) > 0
            else 0.0
        ),
        gap_bucket_2_5=int(gap_buckets.get("2_5") or 0),
        gap_bucket_6_15=int(gap_buckets.get("6_15") or 0),
        gap_bucket_16_30=int(gap_buckets.get("16_30") or 0),
        gap_bucket_gt_30=int(gap_buckets.get("gt_30") or 0),
        gap_count_gt_30=int(gap_count_gt_30s),
        missing_half_seconds=int(missing_half_seconds if str(cadence_mode) == "fixed_0p5s" else 0),
        last_tick_ts=(
            datetime.fromtimestamp(float(last_tick_sec), tz=timezone.utc)
            if last_tick_sec is not None
            else None
        ),
        session_end_lag_s=int(session_end_lag_s) if session_end_lag_s is not None else None,
        max_gap_s=int(max_gap_s),
        gap_threshold_s=float(gap_threshold_s),
        schedule_hash=str(schedule_hash),
    )

    gap_rows: list[MainL5ValidateGapRow] = []
    for seg in missing_segments:
        try:
            gap_rows.append(
                MainL5ValidateGapRow(
                    symbol=str(symbol),
                    trading_day=day_iso,
                    session=str(seg.get("session") or ""),
                    start_ts=datetime.fromisoformat(str(seg.get("start_ts"))),
                    end_ts=datetime.fromisoformat(str(seg.get("end_ts"))),
                    duration_s=int(seg.get("duration_s") or 0),
                    tqsdk_status=str(seg.get("tqsdk_status") or ""),
                    schedule_hash=str(schedule_hash),
                )
            )
        except Exception:
            continue

    return day_out, summary_row, gap_rows


__all__ = ["build_day_validation_artifacts"]

