from __future__ import annotations

import json
from pathlib import Path


def test_clamp_target_position():
    from ghtrader.trading.execution import clamp_target_position

    assert clamp_target_position(5, max_abs_position=2) == 2
    assert clamp_target_position(-5, max_abs_position=2) == -2
    assert clamp_target_position(1, max_abs_position=2) == 1


def test_order_rate_limiter(monkeypatch):
    import ghtrader.trading.execution as ex

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


def test_gateway_writer_writes_files(tmp_path: Path) -> None:
    from ghtrader.tq.gateway import GatewayWriter

    runs_dir = tmp_path / "runs"
    w = GatewayWriter(runs_dir=runs_dir, profile="p1")
    w.set_health(ok=True, connected=False)
    w.set_effective(mode="idle", symbols=[], executor="targetpos")
    w.append_snapshot({"ts": "t0", "account": {"balance": 1.0, "equity": 1.0}})
    w.append_event({"type": "x"})

    root = runs_dir / "gateway" / "account=p1"
    assert (root / "state.json").exists()
    assert (root / "snapshots.jsonl").exists()
    assert (root / "events.jsonl").exists()

    state = json.loads((root / "state.json").read_text(encoding="utf-8"))
    assert state["account_profile"] == "p1"
    assert state["last_snapshot"]["account"]["balance"] == 1.0

