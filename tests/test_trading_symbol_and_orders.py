from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest


def test_resolve_symbol_passthrough(tmp_path: Path):
    from ghtrader.trading.symbol_resolver import resolve_trading_symbol

    data_dir = tmp_path / "data"
    sym = "SHFE.cu2602"
    assert resolve_trading_symbol(symbol=sym, data_dir=data_dir, trading_day=date(2025, 1, 2)) == sym


def test_resolve_continuous_symbol_prefers_rolls_schedule(tmp_path: Path):
    from ghtrader.trading.symbol_resolver import resolve_trading_symbol

    data_dir = tmp_path / "data"
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("ghtrader.questdb.client.make_questdb_query_config_from_env", lambda: object())

    def fake_resolve_main_contract(*, exchange: str, variety: str, trading_day: date, **_kwargs):
        assert exchange == "SHFE"
        assert variety == "cu"
        if trading_day >= date(2025, 1, 3):
            return "SHFE.cu2503", 1, "h"
        return "SHFE.cu2502", 0, "h"

    monkeypatch.setattr("ghtrader.questdb.main_schedule.resolve_main_contract", fake_resolve_main_contract)

    assert resolve_trading_symbol(symbol="KQ.m@SHFE.cu", data_dir=data_dir, trading_day=date(2025, 1, 2)) == "SHFE.cu2502"
    assert resolve_trading_symbol(symbol="KQ.m@SHFE.cu", data_dir=data_dir, trading_day=date(2025, 1, 4)) == "SHFE.cu2503"
    monkeypatch.undo()


def test_plan_direct_orders_shfe_close_today_split():
    from ghtrader.trading.execution import plan_direct_orders_to_target

    # Close a 5-lot long on SHFE: 2 today + 3 history
    intents = plan_direct_orders_to_target(
        exchange="SHFE",
        current_long_today=2,
        current_long_his=3,
        current_short_today=0,
        current_short_his=0,
        target_net=0,
        max_order_size=100,
    )
    assert [(i.direction, i.offset, i.volume) for i in intents] == [
        ("SELL", "CLOSETODAY", 2),
        ("SELL", "CLOSE", 3),
    ]


def test_plan_direct_orders_cross_zero_split_close_then_open():
    from ghtrader.trading.execution import plan_direct_orders_to_target

    # From short 5 (1 today + 4 his) to long 2: buy close 5 then buy open 2
    intents = plan_direct_orders_to_target(
        exchange="SHFE",
        current_long_today=0,
        current_long_his=0,
        current_short_today=1,
        current_short_his=4,
        target_net=2,
        max_order_size=100,
    )
    assert [(i.direction, i.offset, i.volume) for i in intents] == [
        ("BUY", "CLOSETODAY", 1),
        ("BUY", "CLOSE", 4),
        ("BUY", "OPEN", 2),
    ]


def test_plan_direct_orders_respects_max_order_size():
    from ghtrader.trading.execution import plan_direct_orders_to_target

    # Need buy 7, but max order size is 3 -> should split opens
    intents = plan_direct_orders_to_target(
        exchange="DCE",
        current_long_today=0,
        current_long_his=0,
        current_short_today=0,
        current_short_his=0,
        target_net=7,
        max_order_size=3,
    )
    assert sum(i.volume for i in intents) == 7
    assert all(i.volume <= 3 for i in intents)
    assert all(i.direction == "BUY" and i.offset == "OPEN" for i in intents)

