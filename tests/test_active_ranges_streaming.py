from __future__ import annotations

from datetime import date

import pandas as pd

from ghtrader.tq_ingest import _infer_active_ranges_from_daily, _update_active_ranges_from_daily_frame


def test_streaming_active_ranges_matches_batch():
    # Synthetic 2-day data for CU contracts
    df = pd.DataFrame(
        [
            {"symbol": "CU2601", "date": "2025-01-02", "variety": "CU", "open_interest": 10, "volume": 0},
            {"symbol": "CU2602", "date": "2025-01-02", "variety": "CU", "open_interest": 0, "volume": 0},  # inactive
            {"symbol": "CU2601", "date": "2025-01-03", "variety": "CU", "open_interest": 20, "volume": 1},
            {"symbol": "CU2602", "date": "2025-01-03", "variety": "CU", "open_interest": 5, "volume": 0},
        ]
    )

    batch = _infer_active_ranges_from_daily(df, var_upper="CU")

    ranges: dict[str, tuple[date, date]] = {}
    for day, df_day in df.groupby("date"):
        _update_active_ranges_from_daily_frame(ranges, df_day, var_upper="CU")

    assert ranges == batch

