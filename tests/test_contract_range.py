from __future__ import annotations

import pandas as pd

from ghtrader.tq_ingest import _infer_active_ranges_from_daily, _iter_contract_yymms


def test_iter_contract_yymms_inclusive_months():
    yymms = _iter_contract_yymms("1601", "1603")
    assert yymms == ["1601", "1602", "1603"]


def test_infer_active_ranges_from_daily_uses_oi_or_volume():
    daily = pd.DataFrame(
        [
            {"date": "2025-01-01", "symbol": "CU2501", "open_interest": 0, "volume": 0, "variety": "CU"},
            {"date": "2025-01-02", "symbol": "CU2501", "open_interest": 10, "volume": 0, "variety": "CU"},
            {"date": "2025-01-03", "symbol": "CU2501", "open_interest": 0, "volume": 5, "variety": "CU"},
            {"date": "2025-01-04", "symbol": "CU2501", "open_interest": 0, "volume": 0, "variety": "CU"},
        ]
    )
    ranges = _infer_active_ranges_from_daily(daily, var_upper="CU")
    assert "CU2501" in ranges
    start, end = ranges["CU2501"]
    assert str(start) == "2025-01-02"
    assert str(end) == "2025-01-03"

