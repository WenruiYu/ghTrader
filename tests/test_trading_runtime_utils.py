from __future__ import annotations

import json
from datetime import date
from pathlib import Path


def test_clamp_target_position():
    from ghtrader.execution import clamp_target_position

    assert clamp_target_position(5, max_abs_position=2) == 2
    assert clamp_target_position(-5, max_abs_position=2) == -2
    assert clamp_target_position(1, max_abs_position=2) == 1


def test_order_rate_limiter(monkeypatch):
    import ghtrader.execution as ex

    t = {"v": 100.0}

    def fake_monotonic() -> float:
        return float(t["v"])

    monkeypatch.setattr(ex.time, "monotonic", fake_monotonic)
    lim = ex.OrderRateLimiter(max_ops_per_sec=2)

    assert lim.allow() is True
    assert lim.allow() is True
    assert lim.allow() is False  # third within same second blocked

    t["v"] += 1.01
    assert lim.allow() is True  # window moved


def test_trade_run_writer_writes_files(tmp_path: Path):
    from ghtrader.tq_runtime import TradeRunWriter

    runs_dir = tmp_path / "runs"
    w = TradeRunWriter(run_id="r1", runs_dir=runs_dir)
    w.write_config({"mode": "paper"})
    w.append_snapshot({"ts": "t0", "account": {"balance": 1.0}})
    w.append_event({"ts": "t0", "type": "x"})

    cfg_path = runs_dir / "trading" / "r1" / "run_config.json"
    snap_path = runs_dir / "trading" / "r1" / "snapshots.jsonl"
    evt_path = runs_dir / "trading" / "r1" / "events.jsonl"
    assert cfg_path.exists()
    assert snap_path.exists()
    assert evt_path.exists()

    cfg = json.loads(cfg_path.read_text())
    assert cfg["mode"] == "paper"

    snap_line = snap_path.read_text().strip().splitlines()[-1]
    assert json.loads(snap_line)["account"]["balance"] == 1.0


def test_maybe_roll_execution_symbol_uses_schedule(tmp_path: Path):
    import pandas as pd

    from ghtrader.symbol_resolver import resolve_trading_symbol
    from ghtrader.trade import maybe_roll_execution_symbol

    data_dir = tmp_path / "data"
    sched_dir = data_dir / "rolls" / "shfe_main_schedule" / "var=cu"
    sched_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "date": [date(2026, 1, 2), date(2026, 1, 3)],
            "main_contract": ["SHFE.cu2602", "SHFE.cu2603"],
        }
    )
    df.to_parquet(sched_dir / "schedule.parquet", index=False)

    alias = "KQ.m@SHFE.cu"
    old_td = date(2026, 1, 2)
    new_td = date(2026, 1, 3)
    old_exec = resolve_trading_symbol(symbol=alias, data_dir=data_dir, trading_day=old_td)
    assert old_exec == "SHFE.cu2602"

    rolled = maybe_roll_execution_symbol(
        requested_symbol=alias,
        current_execution_symbol=old_exec,
        old_trading_day=old_td,
        new_trading_day=new_td,
        data_dir=data_dir,
    )
    assert rolled == "SHFE.cu2603"

    assert (
        maybe_roll_execution_symbol(
            requested_symbol=alias,
            current_execution_symbol=old_exec,
            old_trading_day=old_td,
            new_trading_day=old_td,
            data_dir=data_dir,
        )
        is None
    )
    assert (
        maybe_roll_execution_symbol(
            requested_symbol="SHFE.cu2602",
            current_execution_symbol="SHFE.cu2602",
            old_trading_day=old_td,
            new_trading_day=new_td,
            data_dir=data_dir,
        )
        is None
    )


def test_online_history_required_for_deep_model_stub():
    from ghtrader.trade import online_history_required, online_model_seq_len

    class FakeDeep:
        seq_len = 7

    m = FakeDeep()
    assert online_model_seq_len(model_name="deeplob", model=m) == 7
    assert online_history_required(model_name="deeplob", model=m) == 8
    # Tabular models always require 1 regardless of seq_len attribute.
    assert online_model_seq_len(model_name="xgboost", model=m) == 1
    assert online_history_required(model_name="xgboost", model=m) == 1


def test_run_trade_monitor_only_does_not_instantiate_executor(monkeypatch, tmp_path: Path):
    import ghtrader.trade as tr
    from ghtrader.features import DEFAULT_FACTORS

    called = {"tp": 0, "direct": 0}

    class TP:
        def __init__(self, *args, **kwargs):
            called["tp"] += 1

    class Direct:
        def __init__(self, *args, **kwargs):
            called["direct"] += 1

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

    class FakeModel:
        def __init__(self, n_features: int):
            self.n_features = int(n_features)

        def predict_proba(self, X):
            raise AssertionError("predict_proba should not be called when no ticks change")

    monkeypatch.setattr(tr, "TargetPosExecutor", TP)
    monkeypatch.setattr(tr, "DirectOrderExecutor", Direct)
    monkeypatch.setattr(tr, "GracefulShutdown", FakeShutdown)
    monkeypatch.setattr(tr, "create_tq_account", lambda **kwargs: object())
    monkeypatch.setattr(tr, "create_tq_api", lambda **kwargs: FakeAPI())
    monkeypatch.setattr(
        tr,
        "_load_models",
        lambda model_name, symbols, artifacts_dir, horizon: {s: FakeModel(len(DEFAULT_FACTORS)) for s in symbols},
    )

    cfg = tr.TradeConfig(
        mode="live",
        monitor_only=True,
        executor="targetpos",
        model_name="xgboost",
        symbols=["SHFE.cu2602"],
        data_dir=tmp_path / "data",
        artifacts_dir=tmp_path / "artifacts",
        runs_dir=tmp_path / "runs",
        snapshot_interval_sec=99999.0,
    )
    tr.run_trade(cfg, confirm_live="")
    assert called["tp"] == 0
    assert called["direct"] == 0

