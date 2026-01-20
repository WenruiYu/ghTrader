from __future__ import annotations

from datetime import date

from ghtrader.data.main_schedule import build_daily_schedule_from_events


def test_build_daily_schedule_from_events_segments() -> None:
    events = [
        {"trading_day": date(2025, 1, 2), "underlying_symbol": "SHFE.cu2501"},
        {"trading_day": date(2025, 1, 4), "underlying_symbol": "SHFE.cu2502"},
    ]
    trading_days = [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]

    df = build_daily_schedule_from_events(events=events, trading_days=trading_days, exchange="SHFE")

    assert df["date"].tolist() == trading_days
    assert df["main_contract"].tolist() == ["SHFE.cu2501", "SHFE.cu2501", "SHFE.cu2502"]
    assert df["segment_id"].tolist() == [0, 0, 1]
