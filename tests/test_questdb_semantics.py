from __future__ import annotations

from datetime import date

import pandas as pd
import pytest


def test_query_contract_coverage_merges_base_and_l5(monkeypatch: pytest.MonkeyPatch):
    import ghtrader.questdb.queries as qq

    symbols = ["SHFE.cu2501", "SHFE.cu2502"]

    calls = {"n": 0}

    def fake_bounds(*, l5_only: bool, **kwargs):
        # First call: base coverage. Second call: main_l5 coverage.
        calls["n"] += 1
        if calls["n"] == 1:
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
        dataset_version="v2",
        ticks_kind="main_l5",
    )

    assert cov["SHFE.cu2501"]["first_tick_day"] == "2025-01-01"
    assert cov["SHFE.cu2501"]["tick_days"] == 3
    assert cov["SHFE.cu2501"]["first_l5_day"] is None
    assert cov["SHFE.cu2502"]["first_l5_day"] == "2025-01-02"
    assert cov["SHFE.cu2502"]["l5_days"] == 2

