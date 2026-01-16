from __future__ import annotations

import sys
import types
from datetime import date
from pathlib import Path

import pandas as pd

from ghtrader.tqsdk_l5_probe import probe_l5_for_symbol


def _install_fake_tqsdk(monkeypatch, *, api_cls: type) -> None:
    """
    Install a fake `tqsdk` module so probe_l5_for_symbol can run offline.
    """
    m = types.ModuleType("tqsdk")
    m.TqApi = api_cls  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "tqsdk", m)


def test_probe_l5_auto_uses_questdb_last_day(
    monkeypatch, synthetic_data_dir: Path, small_synthetic_tick_df: pd.DataFrame, tmp_path: Path
) -> None:
    sym = "SHFE.cu2404"
    d0 = date(2024, 4, 15)

    import ghtrader.tqsdk_l5_probe as mod

    # Avoid network calendar download; weekday-only fallback is fine for this unit test.
    monkeypatch.setattr(mod, "get_trading_calendar", lambda *, data_dir, refresh=False: [])
    # QuestDB-first: probe day should come from index bounds when available.
    monkeypatch.setattr(mod, "_questdb_last_day", lambda *, symbol: d0)
    # Avoid requiring real TqSdk credentials.
    monkeypatch.setattr(mod, "get_tqsdk_auth", lambda: None)

    calls: dict[str, object] = {}

    class FakeApi:
        def __init__(self, auth=None, disable_print: bool = True):
            _ = auth
            _ = disable_print

        def get_quote(self, symbol: str):
            calls["get_quote_symbol"] = symbol
            return {"expire_datetime": "2024-04-15 15:00:00"}

        def get_tick_data_series(self, symbol: str, start_dt, end_dt):
            calls["tick_symbol"] = symbol
            calls["start_dt"] = start_dt
            calls["end_dt"] = end_dt
            return pd.DataFrame(
                {
                    # Only L2-5 columns are needed for _df_has_l5().
                    "bid_price2": [1.0],
                    "ask_price2": [2.0],
                    "bid_price3": [1.0],
                    "ask_price3": [2.0],
                    "bid_price4": [1.0],
                    "ask_price4": [2.0],
                    "bid_price5": [1.0],
                    "ask_price5": [2.0],
                }
            )

        def close(self) -> None:
            return None

    _install_fake_tqsdk(monkeypatch, api_cls=FakeApi)

    out = probe_l5_for_symbol(symbol=sym, data_dir=synthetic_data_dir, runs_dir=tmp_path / "runs", probe_day=None)

    assert out["probed_day"] == d0.isoformat()
    assert out["probe_day_source"] == "questdb_max"
    assert int(out["ticks_rows"]) == 1
    assert out["l5_present"] is True
    assert calls["start_dt"] == d0


def test_probe_l5_auto_uses_quote_expire_when_no_local_data(monkeypatch, synthetic_data_dir: Path, tmp_path: Path) -> None:
    sym = "SHFE.cu1999"
    expire_day = date(2019, 12, 16)

    import ghtrader.tqsdk_l5_probe as mod

    monkeypatch.setattr(mod, "get_trading_calendar", lambda *, data_dir, refresh=False: [])
    monkeypatch.setattr(mod, "get_tqsdk_auth", lambda: None)

    calls: dict[str, object] = {}

    class FakeApi:
        def __init__(self, auth=None, disable_print: bool = True):
            _ = auth
            _ = disable_print

        def get_quote(self, symbol: str):
            calls["get_quote_symbol"] = symbol
            return {"expire_datetime": "2019-12-16 15:00:00"}

        def get_tick_data_series(self, symbol: str, start_dt, end_dt):
            calls["start_dt"] = start_dt
            calls["end_dt"] = end_dt
            return pd.DataFrame(
                {
                    "bid_price2": [float("nan")],
                    "ask_price2": [float("nan")],
                    "bid_price3": [float("nan")],
                    "ask_price3": [float("nan")],
                    "bid_price4": [float("nan")],
                    "ask_price4": [float("nan")],
                    "bid_price5": [float("nan")],
                    "ask_price5": [float("nan")],
                }
            )

        def close(self) -> None:
            return None

    _install_fake_tqsdk(monkeypatch, api_cls=FakeApi)

    out = probe_l5_for_symbol(symbol=sym, data_dir=synthetic_data_dir, runs_dir=tmp_path / "runs", probe_day=None)

    assert out["probe_day_source"] == "quote_expire"
    assert out["probed_day"] == expire_day.isoformat()
    assert calls["start_dt"] == expire_day
    assert out["l5_present"] is False
