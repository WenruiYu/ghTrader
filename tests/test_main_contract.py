from __future__ import annotations

import pandas as pd

from ghtrader.main_contract import compute_shfe_main_schedule_from_daily


def test_shfe_main_schedule_switch_rule_next_day_effect():
    # 3 trading days, 2 contracts.
    # Day1: CU01 is main (oi 100) vs CU02 (oi 50)
    # Day2: CU02 exceeds CU01 * 1.1 (e.g. 120 > 100*1.1), so day3 should switch to CU02.
    daily = pd.DataFrame(
        [
            {"date": "2025-01-01", "symbol": "CU2501", "open_interest": 100, "variety": "CU"},
            {"date": "2025-01-01", "symbol": "CU2502", "open_interest": 50, "variety": "CU"},
            {"date": "2025-01-02", "symbol": "CU2501", "open_interest": 100, "variety": "CU"},
            {"date": "2025-01-02", "symbol": "CU2502", "open_interest": 120, "variety": "CU"},
            {"date": "2025-01-03", "symbol": "CU2501", "open_interest": 80, "variety": "CU"},
            {"date": "2025-01-03", "symbol": "CU2502", "open_interest": 140, "variety": "CU"},
        ]
    )

    sched = compute_shfe_main_schedule_from_daily(daily, var="cu", rule_threshold=1.1, market="SHFE")
    assert sched["date"].tolist() == [pd.to_datetime("2025-01-01").date(), pd.to_datetime("2025-01-02").date(), pd.to_datetime("2025-01-03").date()]

    # day1 uses itself (no prior day), chooses CU2501
    assert sched.loc[0, "main_contract"] == "SHFE.cu2501"
    # day2 decision uses day1 OI, still CU2501
    assert sched.loc[1, "main_contract"] == "SHFE.cu2501"
    # day3 decision uses day2 OI -> switch to CU2502
    assert sched.loc[2, "main_contract"] == "SHFE.cu2502"
    assert bool(sched.loc[2, "switch_flag"]) is True

