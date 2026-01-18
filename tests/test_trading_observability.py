from __future__ import annotations

import pytest


def test_snapshot_account_state_schema_v2_includes_equity_and_meta() -> None:
    from ghtrader.tq.runtime import snapshot_account_state

    class FakeAccount:
        balance = 100.0
        available = 90.0
        margin = 10.0
        float_profit = 5.0
        position_profit = 0.0
        risk_ratio = 0.12

    class FakeAPI:
        def get_account(self, account=None):
            return FakeAccount()

        def get_position(self, symbol: str, account=None):
            class P:
                volume_long = 1
                volume_short = 0
                volume_long_today = 1
                volume_short_today = 0
                volume_long_his = 0
                volume_short_his = 0
                float_profit_long = 1.0
                float_profit_short = 0.0
                open_price_long = 72000.0
                position_price_long = 72100.0

            return P()

        def get_order(self, account=None):
            return {}

    snap = snapshot_account_state(
        api=FakeAPI(),
        symbols=["SHFE.cu2602"],
        account=None,
        account_meta={"mode": "live", "monitor_only": True, "broker_configured": True},
    )

    assert int(snap.get("schema_version") or 0) >= 2
    assert snap["account"]["equity"] == pytest.approx(105.0)
    assert snap["account_meta"]["mode"] == "live"
    assert snap["positions"]["SHFE.cu2602"]["open_price_long"] == pytest.approx(72000.0)
