from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest


def _write_schedule(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_resolve_symbol_passthrough(tmp_path: Path):
    from ghtrader.symbol_resolver import resolve_trading_symbol

    data_dir = tmp_path / "data"
    sym = "SHFE.cu2602"
    assert resolve_trading_symbol(symbol=sym, data_dir=data_dir, trading_day=date(2025, 1, 2)) == sym


def test_resolve_continuous_symbol_prefers_rolls_schedule(tmp_path: Path):
    from ghtrader.symbol_resolver import resolve_trading_symbol

    data_dir = tmp_path / "data"
    sched = data_dir / "rolls" / "shfe_main_schedule" / "var=cu" / "schedule.parquet"
    _write_schedule(
        sched,
        [
            {"date": date(2025, 1, 2), "main_contract": "SHFE.cu2502"},
            {"date": date(2025, 1, 3), "main_contract": "SHFE.cu2503"},
        ],
    )

    assert resolve_trading_symbol(symbol="KQ.m@SHFE.cu", data_dir=data_dir, trading_day=date(2025, 1, 2)) == "SHFE.cu2502"
    assert resolve_trading_symbol(symbol="KQ.m@SHFE.cu", data_dir=data_dir, trading_day=date(2025, 1, 4)) == "SHFE.cu2503"


def test_resolve_continuous_symbol_falls_back_to_derived_schedule_copy(tmp_path: Path):
    from ghtrader.symbol_resolver import resolve_trading_symbol

    data_dir = tmp_path / "data"
    derived_sched = data_dir / "lake" / "main_l5" / "ticks" / "symbol=KQ.m@SHFE.cu" / "schedule.parquet"
    _write_schedule(
        derived_sched,
        [
            {"date": date(2025, 2, 17), "main_contract": "SHFE.cu2505"},
        ],
    )

    assert resolve_trading_symbol(symbol="KQ.m@SHFE.cu", data_dir=data_dir, trading_day=date(2025, 2, 18)) == "SHFE.cu2505"


def test_plan_direct_orders_shfe_close_today_split():
    from ghtrader.execution import plan_direct_orders_to_target

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
    from ghtrader.execution import plan_direct_orders_to_target

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
    from ghtrader.execution import plan_direct_orders_to_target

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

