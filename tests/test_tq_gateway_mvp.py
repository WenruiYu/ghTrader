from __future__ import annotations

import json
from pathlib import Path

from ghtrader.tq.gateway import (
    GatewayDesired,
    GatewayWriter,
    read_gateway_desired,
    read_gateway_targets,
    targets_path,
    write_gateway_desired,
)


def test_gateway_desired_roundtrip(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    prof = "Alt-01"

    desired = GatewayDesired(
        mode="sim",
        symbols=["SHFE.cu2602", "SHFE.au2602"],
        executor="direct",
        sim_account="tqkq",
        confirm_live="",
        max_abs_position=2,
        max_order_size=3,
        max_ops_per_sec=7,
        max_daily_loss=123.0,
        enforce_trading_time=False,
    )
    write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=desired)

    got = read_gateway_desired(runs_dir=runs_dir, profile=prof)
    assert got.mode == "sim"
    assert got.executor == "direct"
    assert got.sim_account == "tqkq"
    assert got.symbols_list() == ["SHFE.cu2602", "SHFE.au2602"]
    assert got.max_abs_position == 2
    assert got.max_order_size == 3
    assert got.max_ops_per_sec == 7
    assert got.max_daily_loss == 123.0
    assert got.enforce_trading_time is False


def test_gateway_targets_parse(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    prof = "default"
    p = targets_path(runs_dir=runs_dir, profile=prof)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "updated_at": "2026-01-01T00:00:00Z",
                "targets": {"SHFE.cu2602": 1, "SHFE.au2602": "2", "": 3, "BAD": "x"},
                "meta": {"model": "xgboost"},
            }
        ),
        encoding="utf-8",
    )

    out = read_gateway_targets(runs_dir=runs_dir, profile=prof)
    assert out["targets"] == {"SHFE.cu2602": 1, "SHFE.au2602": 2}
    assert out["meta"]["model"] == "xgboost"


def test_gateway_writer_updates_state(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    prof = "ALT"
    w = GatewayWriter(runs_dir=runs_dir, profile=prof)
    w.set_health(ok=True, connected=False, error="")
    w.set_effective(mode="idle", symbols=[], executor="targetpos")
    w.append_event({"type": "test_event", "k": 1})
    w.append_snapshot({"schema_version": 2, "ts": "2026-01-01T00:00:00Z", "account": {"balance": 1.0}, "positions": {}, "orders_alive": []})

    state_path = runs_dir / "gateway" / "account=alt" / "state.json"
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["schema_version"] == 1
    assert state["account_profile"] == "alt"
    assert isinstance(state.get("recent_events"), list)
    assert state["last_snapshot"]["schema_version"] == 2

