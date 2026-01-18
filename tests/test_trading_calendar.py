from __future__ import annotations

from datetime import date
from pathlib import Path

from ghtrader.data.trading_calendar import get_trading_days, is_trading_day


def test_trading_days_excludes_weekend(tmp_path: Path):
    # 2026-01-10/11 are Sat/Sun; should not appear in trading days.
    days = get_trading_days(
        market="SHFE",
        start=date(2026, 1, 9),
        end=date(2026, 1, 12),
        data_dir=tmp_path,
        refresh=False,
    )
    assert date(2026, 1, 10) not in days
    assert date(2026, 1, 11) not in days


def test_is_trading_day_weekend_false(tmp_path: Path):
    assert is_trading_day(day=date(2026, 1, 10), data_dir=tmp_path) is False

