from __future__ import annotations

import json
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

