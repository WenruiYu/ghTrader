from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class DayGapComputation:
    expected_seconds: int
    expected_seconds_strict: int
    observed_in_sessions: int
    observed_outside_sessions: int
    seconds_with_one: int
    seconds_with_two_plus: int
    missing_day: bool
    day_missing_segments: list[dict[str, Any]]
    day_missing_seconds: int
    day_missing_half_seconds: int
    day_boundary_missing: int
    day_segments_total: int
    max_gap_day: int
    observed_segments_day: int
    day_gap_buckets: dict[str, int]
    day_gap_buckets_by_session: dict[str, dict[str, int]]
    day_gap_count_gt_30: int
    session_end_sec: int | None
    last_tick_sec: int | None
    session_end_lag_s: int | None


def compute_day_gap_stats(
    *,
    intervals: list[dict[str, Any]],
    sec_counts: dict[int, int],
    day_cadence: str,
    gap_threshold_s: float,
    gap_bucket_defs: list[tuple[str, int, int | None]],
    max_segments_per_day: int,
    sec_to_iso: Callable[[int], str],
    gap_threshold_s_by_session: dict[str, float] | None = None,
) -> DayGapComputation:
    secs_sorted = sorted(sec_counts.keys())

    session_end_sec = max((int(it["end_sec"]) for it in intervals), default=None)
    last_tick_sec = None
    if secs_sorted and session_end_sec is not None:
        for sec in reversed(secs_sorted):
            for it in intervals:
                if int(it["start_sec"]) <= sec <= int(it["end_sec"]):
                    last_tick_sec = sec
                    break
            if last_tick_sec is not None:
                break
    session_end_lag_s = None
    if session_end_sec is not None and last_tick_sec is not None:
        session_end_lag_s = int(max(0, int(session_end_sec) - int(last_tick_sec)))

    expected_seconds = 0
    expected_seconds_strict = 0
    boundary_seconds: set[int] = set()
    for it in intervals:
        start_sec = int(it["start_sec"])
        end_sec = int(it["end_sec"])
        if end_sec < start_sec:
            continue
        expected_seconds += int(end_sec - start_sec + 1)
        boundary_seconds.add(start_sec)
        boundary_seconds.add(end_sec)
        eff_start = start_sec + 1
        eff_end = end_sec - 1
        if eff_end >= eff_start:
            expected_seconds_strict += int(eff_end - eff_start + 1)

    observed_in_sessions = 0
    observed_outside_sessions = 0
    seconds_with_one = 0
    seconds_with_two_plus = 0
    interval_idx = 0
    for sec in secs_sorted:
        while interval_idx < len(intervals) and sec > int(intervals[interval_idx]["end_sec"]):
            interval_idx += 1
        if interval_idx < len(intervals):
            start_sec = int(intervals[interval_idx]["start_sec"])
            end_sec = int(intervals[interval_idx]["end_sec"])
            if start_sec <= sec <= end_sec:
                observed_in_sessions += 1
                if sec not in boundary_seconds:
                    n = sec_counts.get(sec, 0)
                    if n == 1:
                        seconds_with_one += 1
                    elif n >= 2:
                        seconds_with_two_plus += 1
            else:
                observed_outside_sessions += 1
        else:
            observed_outside_sessions += 1

    missing_day = bool(observed_in_sessions == 0 and expected_seconds > 0)

    day_missing_segments: list[dict[str, Any]] = []
    day_missing_seconds = 0
    day_missing_half_seconds = 0
    day_boundary_missing = 0
    day_segments_total = 0
    max_gap_day = 0
    observed_segments_day = 0
    day_gap_buckets: dict[str, int] = {label: 0 for label, _, _ in gap_bucket_defs}
    day_gap_buckets_by_session: dict[str, dict[str, int]] = {}
    day_gap_count_gt_30 = 0

    by_session_threshold: dict[str, float] = {}
    if isinstance(gap_threshold_s_by_session, dict):
        for k, v in gap_threshold_s_by_session.items():
            sk = str(k or "").strip().lower()
            if not sk:
                continue
            try:
                by_session_threshold[sk] = max(0.5, float(v))
            except Exception:
                continue
    default_gap_threshold = max(0.5, float(gap_threshold_s))

    def gap_threshold_for_session(sess: str) -> float:
        key = str(sess or "").strip().lower()
        return float(by_session_threshold.get(key, default_gap_threshold))

    def record_gap(*, sess: str, gap_start: int, gap_end: int) -> None:
        nonlocal day_segments_total, max_gap_day, day_gap_count_gt_30
        if gap_end < gap_start:
            return
        duration = int(gap_end - gap_start + 1)
        max_gap_day = max(max_gap_day, duration)
        if float(duration) >= gap_threshold_for_session(sess):
            day_segments_total += 1
            bucket_label = None
            for label, start_s, end_s in gap_bucket_defs:
                if duration >= int(start_s) and (end_s is None or duration <= int(end_s)):
                    bucket_label = label
                    break
            if bucket_label:
                day_gap_buckets[bucket_label] = int(day_gap_buckets.get(bucket_label, 0)) + 1
                if sess:
                    sess_key = str(sess)
                    if sess_key not in day_gap_buckets_by_session:
                        day_gap_buckets_by_session[sess_key] = {label: 0 for label, _, _ in gap_bucket_defs}
                    day_gap_buckets_by_session[sess_key][bucket_label] += 1
            if duration > 30:
                day_gap_count_gt_30 += 1
            if len(day_missing_segments) < max_segments_per_day:
                day_missing_segments.append(
                    {
                        "session": sess,
                        "start_ts": sec_to_iso(gap_start),
                        "end_ts": sec_to_iso(gap_end),
                        "duration_s": duration,
                        "tqsdk_status": "unchecked",
                    }
                )

    for it in intervals:
        sess = str(it.get("session") or "")
        start_sec = int(it["start_sec"])
        end_sec = int(it["end_sec"])
        if sec_counts.get(start_sec, 0) == 0:
            day_boundary_missing += 1
        if sec_counts.get(end_sec, 0) == 0:
            day_boundary_missing += 1
        eff_start = start_sec + 1
        eff_end = end_sec - 1
        if eff_end < eff_start:
            continue
        left = bisect_left(secs_sorted, eff_start)
        right = bisect_right(secs_sorted, eff_end)
        secs_eff = secs_sorted[left:right]
        day_missing_seconds += int((eff_end - eff_start + 1) - len(secs_eff))
        if day_cadence == "fixed_0p5s":
            secs_two_plus = sum(1 for s in secs_eff if sec_counts.get(s, 0) >= 2)
            day_missing_half_seconds += int((eff_end - eff_start + 1) - secs_two_plus)

        if not secs_eff:
            record_gap(sess=sess, gap_start=eff_start, gap_end=eff_end)
            continue

        observed_segments_session = 1
        if secs_eff[0] > eff_start:
            record_gap(sess=sess, gap_start=eff_start, gap_end=secs_eff[0] - 1)
        for a, b in zip(secs_eff, secs_eff[1:]):
            if b - a > 1:
                gap_len = int(b - a - 1)
                if float(gap_len) >= gap_threshold_for_session(sess):
                    observed_segments_session += 1
                record_gap(sess=sess, gap_start=a + 1, gap_end=b - 1)
        if secs_eff[-1] < eff_end:
            record_gap(sess=sess, gap_start=secs_eff[-1] + 1, gap_end=eff_end)
        observed_segments_day += int(observed_segments_session)

    return DayGapComputation(
        expected_seconds=int(expected_seconds),
        expected_seconds_strict=int(expected_seconds_strict),
        observed_in_sessions=int(observed_in_sessions),
        observed_outside_sessions=int(observed_outside_sessions),
        seconds_with_one=int(seconds_with_one),
        seconds_with_two_plus=int(seconds_with_two_plus),
        missing_day=bool(missing_day),
        day_missing_segments=day_missing_segments,
        day_missing_seconds=int(day_missing_seconds),
        day_missing_half_seconds=int(day_missing_half_seconds),
        day_boundary_missing=int(day_boundary_missing),
        day_segments_total=int(day_segments_total),
        max_gap_day=int(max_gap_day),
        observed_segments_day=int(observed_segments_day),
        day_gap_buckets=day_gap_buckets,
        day_gap_buckets_by_session=day_gap_buckets_by_session,
        day_gap_count_gt_30=int(day_gap_count_gt_30),
        session_end_sec=(int(session_end_sec) if session_end_sec is not None else None),
        last_tick_sec=(int(last_tick_sec) if last_tick_sec is not None else None),
        session_end_lag_s=(int(session_end_lag_s) if session_end_lag_s is not None else None),
    )


__all__ = ["DayGapComputation", "compute_day_gap_stats"]

