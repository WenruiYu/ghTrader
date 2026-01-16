from __future__ import annotations

from datetime import date
from pathlib import Path

from ghtrader.symbol_resolver import resolve_trading_symbol


def test_resolve_trading_symbol_passthrough_specific_contract(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    assert resolve_trading_symbol(symbol="SHFE.cu2502", data_dir=data_dir, trading_day=date(2025, 1, 2)) == "SHFE.cu2502"


def test_resolve_trading_symbol_uses_questdb_schedule(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    alias = "KQ.m@SHFE.cu"

    monkeypatch.setattr("ghtrader.questdb_client.make_questdb_query_config_from_env", lambda: object())

    calls: dict[str, object] = {}

    def fake_resolve_main_contract(*, exchange: str, variety: str, trading_day: date, **_kwargs):
        calls["exchange"] = exchange
        calls["variety"] = variety
        calls["trading_day"] = trading_day
        return "SHFE.cu2502", 0, "h"

    monkeypatch.setattr("ghtrader.questdb_main_schedule.resolve_main_contract", fake_resolve_main_contract)

    assert resolve_trading_symbol(symbol=alias, data_dir=data_dir, trading_day=date(2025, 1, 2)) == "SHFE.cu2502"
    assert calls["exchange"] == "SHFE"
    assert calls["variety"] == "cu"
    assert calls["trading_day"] == date(2025, 1, 2)

