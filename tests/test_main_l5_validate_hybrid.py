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


def test_session_start_override_shfe_delay():
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
    overrides = [ov for ov in mod._session_start_overrides(exchange="SHFE") if ov.get("trading_day") == day]

    updated = mod._apply_session_start_overrides(
        day=day,
        intervals=intervals,
        tz=tz,
        prev_trading_day=prev,
        overrides=overrides,
    )

    assert updated
    assert updated[0]["start_sec"] == int(datetime(2019, 12, 25, 22, 30, tzinfo=tz).timestamp())
