from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_snapshot_account_state_schema_v2_includes_equity_and_meta() -> None:
    from ghtrader.tq_runtime import snapshot_account_state

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


def test_diff_orders_alive_emits_order_update_and_trade_fill() -> None:
    from ghtrader.trade import _diff_orders_alive

    prev = {
        "o1": {
            "order_id": "o1",
            "symbol": "SHFE.cu2602",
            "direction": "BUY",
            "offset": "OPEN",
            "price_type": "LIMIT",
            "limit_price": 10.0,
            "volume_orign": 2,
            "volume_left": 2,
            "last_msg": "",
        }
    }
    cur = {
        "o1": {
            "order_id": "o1",
            "symbol": "SHFE.cu2602",
            "direction": "BUY",
            "offset": "OPEN",
            "price_type": "LIMIT",
            "limit_price": 10.0,
            "volume_orign": 2,
            "volume_left": 1,  # filled 1
            "last_msg": "",
        }
    }
    evs = _diff_orders_alive(prev=prev, cur=cur, ts="t0")
    types = [e.get("type") for e in evs]
    assert "order_update" in types
    assert "trade_fill" in types
    fill = next(e for e in evs if e.get("type") == "trade_fill")
    assert int(fill.get("volume") or 0) == 1


def test_run_trade_monitor_only_runs_without_models_and_writes_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import ghtrader.trade as tr

    class FakeShutdown:
        def __init__(self):
            self.requested = True

        def install(self):
            return None

    class FakeAPI:
        def get_quote(self, symbol: str):
            class Q:
                datetime = ""

            return Q()

        def get_tick_serial(self, symbol: str):
            return object()

        def get_position(self, symbol: str, account=None):
            class P:
                volume_long_today = 0
                volume_long_his = 0
                volume_short_today = 0
                volume_short_his = 0
                volume_long = 0
                volume_short = 0
                float_profit_long = 0.0
                float_profit_short = 0.0

            return P()

        def get_account(self, account=None):
            class A:
                balance = 1.0
                available = 1.0
                margin = 0.0
                float_profit = 0.0
                position_profit = 0.0
                risk_ratio = 0.0

            return A()

        def get_order(self, account=None):
            return {}

        def wait_update(self):
            return None

        def is_changing(self, obj) -> bool:
            return False

        def close(self):
            return None

    # Ensure stable run_id for assertions
    monkeypatch.setattr(tr.time, "strftime", lambda *_args, **_kwargs: "r1")
    monkeypatch.setattr(tr, "GracefulShutdown", FakeShutdown)
    monkeypatch.setattr(tr, "create_tq_account", lambda **kwargs: object())
    monkeypatch.setattr(tr, "create_tq_api", lambda **kwargs: FakeAPI())

    cfg = tr.TradeConfig(
        mode="live",
        monitor_only=True,
        executor="targetpos",
        model_name="xgboost",
        symbols=["SHFE.cu2602"],
        data_dir=tmp_path / "data",
        artifacts_dir=tmp_path / "artifacts",  # empty => models missing
        runs_dir=tmp_path / "runs",
        snapshot_interval_sec=99999.0,
    )
    tr.run_trade(cfg, confirm_live="")

    run_root = tmp_path / "runs" / "trading" / "r1"
    assert (run_root / "snapshots.jsonl").exists()
    assert (run_root / "events.jsonl").exists()
    assert (run_root / "state.json").exists()

    # signals_disabled is emitted when models are missing in monitor-only
    events = (run_root / "events.jsonl").read_text(encoding="utf-8").splitlines()
    parsed = [json.loads(ln) for ln in events if ln.strip()]
    assert any(e.get("type") == "signals_disabled" for e in parsed)

