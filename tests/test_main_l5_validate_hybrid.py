from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


def _session_payload() -> dict:
    return {
        "sessions": [
            {"session": "day", "start": "09:00:00", "end": "09:00:05"},
        ]
    }


def _sec(ts: datetime) -> int:
    return int(ts.replace(tzinfo=timezone.utc).timestamp())


def test_main_l5_validate_gap_threshold(monkeypatch):
    from ghtrader.data import main_l5_validation as mod

    day = date(2026, 1, 2)
    schedule = pd.DataFrame(
        [
            {
                "trading_day": day,
                "main_contract": "SHFE.cu2602",
                "segment_id": 0,
                "schedule_hash": "abc123",
            }
        ]
    )

    monkeypatch.setattr(mod, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(mod, "read_trading_sessions_cache", lambda **kwargs: _session_payload())
    monkeypatch.setattr(mod, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(mod, "ensure_main_l5_validate_summary_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_validate_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_tick_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "assert_main_l5_validate_tables_ready", lambda **kwargs: None)

    # Event-like cadence (low two-plus ratio)
    monkeypatch.setattr(
        mod,
        "_fetch_day_second_stats",
        lambda **kwargs: {day: {"seconds_with_ticks": 2, "seconds_with_one": 2, "seconds_with_two_plus": 0}},
    )

    # Effective range is 09:00:01-09:00:04 UTC; create a 2-second gap (< threshold)
    base = datetime(2026, 1, 2, 1, 0, 0, tzinfo=timezone.utc)  # 09:00 CST
    sec_counts = {
        _sec(base + pd.Timedelta(seconds=1)): 1,
        _sec(base + pd.Timedelta(seconds=4)): 1,
    }
    monkeypatch.setattr(mod, "_fetch_per_second_counts", lambda **kwargs: sec_counts)

    captured = {"summary": []}

    def _capture_summary(**kwargs):
        captured["summary"].extend(kwargs.get("rows") or [])
        return len(kwargs.get("rows") or [])

    monkeypatch.setattr(mod, "upsert_main_l5_validate_summary_rows", _capture_summary)
    monkeypatch.setattr(mod, "insert_main_l5_validate_gap_rows", lambda **kwargs: 0)
    monkeypatch.setattr(mod, "_write_report", lambda **kwargs: None)

    report, _ = mod.validate_main_l5(
        exchange="SHFE",
        variety="cu",
        derived_symbol="KQ.m@SHFE.cu",
        data_dir=Path("data"),
        runs_dir=Path("runs"),
        tqsdk_check=False,
        gap_threshold_s=5.0,
        strict_ratio=0.8,
    )

    assert report["missing_segments_total"] == 0
    assert abs(report["missing_seconds_ratio"] - (2 / 4)) < 1e-6
    assert report["gap_buckets_total"]["2_5"] == 0
    assert report["gap_buckets_total"]["6_15"] == 0
    assert report["gap_buckets_total"]["16_30"] == 0
    assert report["gap_buckets_total"]["gt_30"] == 0
    assert report["gap_count_gt_30s"] == 0
    assert captured["summary"][0].cadence_mode == "event"


def test_main_l5_validate_strict_cadence(monkeypatch):
    from ghtrader.data import main_l5_validation as mod

    day = date(2026, 1, 3)
    schedule = pd.DataFrame(
        [
            {
                "trading_day": day,
                "main_contract": "SHFE.cu2602",
                "segment_id": 0,
                "schedule_hash": "def456",
            }
        ]
    )

    monkeypatch.setattr(mod, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(mod, "read_trading_sessions_cache", lambda **kwargs: _session_payload())
    monkeypatch.setattr(mod, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(mod, "ensure_main_l5_validate_summary_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_validate_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_tick_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "assert_main_l5_validate_tables_ready", lambda **kwargs: None)

    # Strict cadence (high two-plus ratio)
    monkeypatch.setattr(
        mod,
        "_fetch_day_second_stats",
        lambda **kwargs: {day: {"seconds_with_ticks": 4, "seconds_with_one": 0, "seconds_with_two_plus": 4}},
    )

    base = datetime(2026, 1, 3, 1, 0, 0, tzinfo=timezone.utc)  # 09:00 CST
    sec_counts = {
        _sec(base + pd.Timedelta(seconds=1)): 2,
        _sec(base + pd.Timedelta(seconds=2)): 2,
        _sec(base + pd.Timedelta(seconds=3)): 2,
        _sec(base + pd.Timedelta(seconds=4)): 2,
    }
    monkeypatch.setattr(mod, "_fetch_per_second_counts", lambda **kwargs: sec_counts)

    captured = {"summary": []}

    def _capture_summary(**kwargs):
        captured["summary"].extend(kwargs.get("rows") or [])
        return len(kwargs.get("rows") or [])

    monkeypatch.setattr(mod, "upsert_main_l5_validate_summary_rows", _capture_summary)
    monkeypatch.setattr(mod, "insert_main_l5_validate_gap_rows", lambda **kwargs: 0)
    monkeypatch.setattr(mod, "_write_report", lambda **kwargs: None)

    report, _ = mod.validate_main_l5(
        exchange="SHFE",
        variety="cu",
        derived_symbol="KQ.m@SHFE.cu",
        data_dir=Path("data"),
        runs_dir=Path("runs"),
        tqsdk_check=False,
        gap_threshold_s=5.0,
        strict_ratio=0.8,
    )

    assert report["missing_half_seconds_total"] == 0
    assert captured["summary"][0].cadence_mode == "fixed_0p5s"


def test_main_l5_validate_gap_threshold_by_session(monkeypatch):
    from ghtrader.data import main_l5_validation as mod

    day = date(2026, 1, 5)
    schedule = pd.DataFrame(
        [
            {
                "trading_day": day,
                "main_contract": "SHFE.cu2602",
                "segment_id": 0,
                "schedule_hash": "gth-day",
            }
        ]
    )

    monkeypatch.setenv("GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S", "2")
    monkeypatch.setenv("GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S_DAY", "10")

    monkeypatch.setattr(mod, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(mod, "read_trading_sessions_cache", lambda **kwargs: _session_payload())
    monkeypatch.setattr(mod, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(mod, "ensure_main_l5_validate_summary_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_validate_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_tick_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "assert_main_l5_validate_tables_ready", lambda **kwargs: None)
    monkeypatch.setattr(
        mod,
        "_fetch_day_second_stats",
        lambda **kwargs: {day: {"seconds_with_ticks": 2, "seconds_with_one": 2, "seconds_with_two_plus": 0}},
    )

    base = datetime(2026, 1, 5, 1, 0, 0, tzinfo=timezone.utc)
    sec_counts = {
        _sec(base + pd.Timedelta(seconds=1)): 1,
        _sec(base + pd.Timedelta(seconds=4)): 1,
    }
    monkeypatch.setattr(mod, "_fetch_per_second_counts", lambda **kwargs: sec_counts)
    monkeypatch.setattr(mod, "upsert_main_l5_validate_summary_rows", lambda **kwargs: len(kwargs.get("rows") or []))
    monkeypatch.setattr(mod, "clear_main_l5_validate_gap_rows", lambda **kwargs: 0)
    monkeypatch.setattr(mod, "insert_main_l5_validate_gap_rows", lambda **kwargs: len(kwargs.get("rows") or []))
    monkeypatch.setattr(mod, "upsert_main_l5_tick_gaps_from_validate_summary", lambda **kwargs: 0)
    monkeypatch.setattr(mod, "_write_report", lambda **kwargs: None)

    report, _ = mod.validate_main_l5(
        exchange="SHFE",
        variety="cu",
        derived_symbol="KQ.m@SHFE.cu",
        data_dir=Path("data"),
        runs_dir=Path("runs"),
        tqsdk_check=False,
        strict_ratio=0.8,
    )

    assert report["gap_threshold_s"] == 2.0
    assert report["gap_threshold_s_by_session"]["day"] == 10.0
    assert report["missing_segments_total"] == 0


def test_main_l5_validate_missing_half_info_not_blocking(monkeypatch):
    from ghtrader.data import main_l5_validation as mod

    day = date(2026, 1, 6)
    schedule = pd.DataFrame(
        [
            {
                "trading_day": day,
                "main_contract": "SHFE.cu2602",
                "segment_id": 0,
                "schedule_hash": "half-info",
            }
        ]
    )

    monkeypatch.setenv("GHTRADER_L5_VALIDATE_MISSING_HALF_INFO_RATIO", "0.10")
    monkeypatch.setenv("GHTRADER_L5_VALIDATE_MISSING_HALF_BLOCK_RATIO", "0.50")

    monkeypatch.setattr(mod, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(mod, "read_trading_sessions_cache", lambda **kwargs: _session_payload())
    monkeypatch.setattr(mod, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(mod, "ensure_main_l5_validate_summary_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_validate_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_tick_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "assert_main_l5_validate_tables_ready", lambda **kwargs: None)
    monkeypatch.setattr(
        mod,
        "_fetch_day_second_stats",
        lambda **kwargs: {day: {"seconds_with_ticks": 4, "seconds_with_one": 0, "seconds_with_two_plus": 4}},
    )

    base = datetime(2026, 1, 6, 1, 0, 0, tzinfo=timezone.utc)
    sec_counts = {
        _sec(base + pd.Timedelta(seconds=1)): 2,
        _sec(base + pd.Timedelta(seconds=2)): 1,
        _sec(base + pd.Timedelta(seconds=3)): 2,
        _sec(base + pd.Timedelta(seconds=4)): 2,
    }
    monkeypatch.setattr(mod, "_fetch_per_second_counts", lambda **kwargs: sec_counts)
    monkeypatch.setattr(mod, "upsert_main_l5_validate_summary_rows", lambda **kwargs: len(kwargs.get("rows") or []))
    monkeypatch.setattr(mod, "clear_main_l5_validate_gap_rows", lambda **kwargs: 0)
    monkeypatch.setattr(mod, "insert_main_l5_validate_gap_rows", lambda **kwargs: len(kwargs.get("rows") or []))
    monkeypatch.setattr(mod, "upsert_main_l5_tick_gaps_from_validate_summary", lambda **kwargs: 0)
    monkeypatch.setattr(mod, "_write_report", lambda **kwargs: None)

    report, _ = mod.validate_main_l5(
        exchange="SHFE",
        variety="cu",
        derived_symbol="KQ.m@SHFE.cu",
        data_dir=Path("data"),
        runs_dir=Path("runs"),
        tqsdk_check=False,
        strict_ratio=0.8,
    )

    assert report["missing_half_seconds_total"] == 1
    assert report["missing_half_seconds_state"] == "info"
    assert report["state"] == "warn"
    assert report["ok"] is True


def test_exchange_event_shift_start():
    from ghtrader.data import exchange_events as evs
    from ghtrader.data import main_l5_validation as mod

    day = date(2019, 12, 26)
    prev = {day: date(2019, 12, 25)}
    tz = timezone.utc
    intervals = [
        {
            "session": "night",
            "start_sec": int(datetime(2019, 12, 25, 21, 0, tzinfo=tz).timestamp()),
            "end_sec": int(datetime(2019, 12, 26, 1, 0, tzinfo=tz).timestamp()),
            "start_ts": datetime(2019, 12, 25, 21, 0, tzinfo=tz).isoformat(),
            "end_ts": datetime(2019, 12, 26, 1, 0, tzinfo=tz).isoformat(),
        }
    ]
    events = evs.events_for_day(
        evs.load_exchange_events(data_dir=Path("."), exchange="SHFE", variety="cu"),
        day,
    )

    updated, applied = mod._apply_exchange_events(
        day=day,
        intervals=intervals,
        tz=tz,
        prev_trading_day=prev,
        events=events,
    )

    assert applied
    assert updated[0]["start_sec"] == int(datetime(2019, 12, 25, 22, 30, tzinfo=tz).timestamp())


def test_exchange_event_skip_night():
    from ghtrader.data import exchange_events as evs
    from ghtrader.data import main_l5_validation as mod

    day = date(2020, 2, 4)
    prev = {day: date(2020, 2, 3)}
    tz = timezone.utc
    intervals = [
        {
            "session": "night",
            "start_sec": int(datetime(2020, 2, 3, 21, 0, tzinfo=tz).timestamp()),
            "end_sec": int(datetime(2020, 2, 4, 1, 0, tzinfo=tz).timestamp()),
            "start_ts": datetime(2020, 2, 3, 21, 0, tzinfo=tz).isoformat(),
            "end_ts": datetime(2020, 2, 4, 1, 0, tzinfo=tz).isoformat(),
        },
        {
            "session": "day",
            "start_sec": int(datetime(2020, 2, 4, 1, 30, tzinfo=tz).timestamp()),
            "end_sec": int(datetime(2020, 2, 4, 7, 0, tzinfo=tz).timestamp()),
            "start_ts": datetime(2020, 2, 4, 1, 30, tzinfo=tz).isoformat(),
            "end_ts": datetime(2020, 2, 4, 7, 0, tzinfo=tz).isoformat(),
        },
    ]
    events = evs.events_for_day(
        evs.load_exchange_events(data_dir=Path("."), exchange="SHFE", variety="cu"),
        day,
    )

    updated, applied = mod._apply_exchange_events(
        day=day,
        intervals=intervals,
        tz=tz,
        prev_trading_day=prev,
        events=events,
    )

    assert applied
    assert all(it["session"] != "night" for it in updated)


def test_main_l5_validate_fail_fast_on_summary_persist_error(monkeypatch):
    from ghtrader.data import main_l5_validation as mod

    day = date(2026, 1, 4)
    schedule = pd.DataFrame(
        [
            {
                "trading_day": day,
                "main_contract": "SHFE.cu2602",
                "segment_id": 0,
                "schedule_hash": "xyz789",
            }
        ]
    )

    monkeypatch.setattr(mod, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(mod, "read_trading_sessions_cache", lambda **kwargs: _session_payload())
    monkeypatch.setattr(mod, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(mod, "ensure_main_l5_validate_summary_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_validate_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "ensure_main_l5_tick_gaps_table", lambda **kwargs: None)
    monkeypatch.setattr(mod, "assert_main_l5_validate_tables_ready", lambda **kwargs: None)
    monkeypatch.setattr(
        mod,
        "_fetch_day_second_stats",
        lambda **kwargs: {day: {"seconds_with_ticks": 2, "seconds_with_one": 1, "seconds_with_two_plus": 1}},
    )
    base = datetime(2026, 1, 4, 1, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        mod,
        "_fetch_per_second_counts",
        lambda **kwargs: {
            _sec(base + pd.Timedelta(seconds=1)): 1,
            _sec(base + pd.Timedelta(seconds=2)): 2,
        },
    )
    monkeypatch.setattr(
        mod,
        "upsert_main_l5_validate_summary_rows",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("placeholder mismatch")),
    )
    called = {"clear": False, "insert": False, "tick": False}
    monkeypatch.setattr(
        mod,
        "clear_main_l5_validate_gap_rows",
        lambda **kwargs: called.__setitem__("clear", True),
    )
    monkeypatch.setattr(
        mod,
        "insert_main_l5_validate_gap_rows",
        lambda **kwargs: called.__setitem__("insert", True),
    )
    monkeypatch.setattr(
        mod,
        "upsert_main_l5_tick_gaps_from_validate_summary",
        lambda **kwargs: called.__setitem__("tick", True),
    )
    monkeypatch.setattr(mod, "_write_report", lambda **kwargs: None)

    import pytest

    with pytest.raises(RuntimeError, match="summary persist failed"):
        mod.validate_main_l5(
            exchange="SHFE",
            variety="cu",
            derived_symbol="KQ.m@SHFE.cu",
            data_dir=Path("data"),
            runs_dir=Path("runs"),
            tqsdk_check=False,
            gap_threshold_s=5.0,
            strict_ratio=0.8,
        )

    assert called == {"clear": False, "insert": False, "tick": False}


def test_main_l5_validate_incremental_noop_when_up_to_date(monkeypatch):
    from ghtrader.data import main_l5_validation as mod

    day = date(2026, 1, 4)
    schedule = pd.DataFrame(
        [
            {
                "trading_day": day,
                "main_contract": "SHFE.cu2602",
                "segment_id": 0,
                "schedule_hash": "s1",
            }
        ]
    )

    monkeypatch.setattr(mod, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(mod, "get_last_validated_day", lambda **kwargs: day)
    monkeypatch.setattr(mod, "_write_report", lambda **kwargs: Path("noop.json"))

    report, out_path = mod.validate_main_l5(
        exchange="SHFE",
        variety="cu",
        derived_symbol="KQ.m@SHFE.cu",
        data_dir=Path("data"),
        runs_dir=Path("runs"),
        incremental=True,
    )

    assert str(out_path) == "noop.json"
    assert report["ok"] is True
    assert report["state"] == "noop"
    assert report["reason"] == "up_to_date"
    assert report["checked_days"] == 0
