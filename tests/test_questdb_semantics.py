from __future__ import annotations

from datetime import date

import pandas as pd
import pytest


def test_compute_shfe_main_schedule_from_daily_switch_rule():
    """
    The schedule for day T is decided using end-of-day OI from day T-1.
    """
    from ghtrader.main_contract import compute_shfe_main_schedule_from_daily

    daily = pd.DataFrame(
        [
            # Day 1: CU2501 is main
            {"date": "2025-01-01", "symbol": "CU2501", "open_interest": 100, "volume": 0, "variety": "CU"},
            {"date": "2025-01-01", "symbol": "CU2502", "open_interest": 90, "volume": 0, "variety": "CU"},
            # Day 2: CU2502 becomes > 1.1x CU2501
            {"date": "2025-01-02", "symbol": "CU2501", "open_interest": 100, "volume": 0, "variety": "CU"},
            {"date": "2025-01-02", "symbol": "CU2502", "open_interest": 120, "volume": 0, "variety": "CU"},
            # Day 3: keep CU2502
            {"date": "2025-01-03", "symbol": "CU2501", "open_interest": 80, "volume": 0, "variety": "CU"},
            {"date": "2025-01-03", "symbol": "CU2502", "open_interest": 130, "volume": 0, "variety": "CU"},
        ]
    )

    sched = compute_shfe_main_schedule_from_daily(daily, var="cu", rule_threshold=1.1, market="SHFE")
    got = {row["date"].isoformat(): row["main_contract"] for _, row in sched.iterrows()}
    assert got["2025-01-01"] == "SHFE.cu2501"
    # Day 2 decision uses day-1 OI, so still CU2501
    assert got["2025-01-02"] == "SHFE.cu2501"
    # Day 3 decision uses day-2 OI, so switches to CU2502
    assert got["2025-01-03"] == "SHFE.cu2502"

    # Segment id increments on contract change
    seg = sched.set_index(sched["date"].astype(str))["segment_id"].to_dict()
    assert seg["2025-01-01"] == 0
    assert seg["2025-01-02"] == 0
    assert seg["2025-01-03"] == 1


def test_query_contract_coverage_merges_base_and_l5(monkeypatch: pytest.MonkeyPatch):
    import ghtrader.questdb_queries as qq

    symbols = ["SHFE.cu2501", "SHFE.cu2502"]

    def fake_bounds(*, l5_only: bool, **kwargs):
        if not l5_only:
            return {
                "SHFE.cu2501": {"first_day": "2025-01-01", "last_day": "2025-01-03", "n_days": 3},
                "SHFE.cu2502": {"first_day": "2025-01-02", "last_day": "2025-01-03", "n_days": 2},
            }
        return {
            "SHFE.cu2502": {"first_day": "2025-01-02", "last_day": "2025-01-03", "n_days": 2},
        }

    monkeypatch.setattr(qq, "query_symbol_day_bounds", fake_bounds)

    cov = qq.query_contract_coverage(
        cfg=qq.QuestDBQueryConfig(host="x", pg_port=0, pg_user="u", pg_password="p", pg_dbname="d"),
        table="t",
        symbols=symbols,
        lake_version="v2",
        ticks_lake="raw",
    )

    assert cov["SHFE.cu2501"]["first_tick_day"] == "2025-01-01"
    assert cov["SHFE.cu2501"]["tick_days"] == 3
    assert cov["SHFE.cu2501"]["first_l5_day"] is None
    assert cov["SHFE.cu2502"]["first_l5_day"] == "2025-01-02"
    assert cov["SHFE.cu2502"]["l5_days"] == 2

