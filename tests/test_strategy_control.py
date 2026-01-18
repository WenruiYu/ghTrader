from __future__ import annotations

from pathlib import Path

from ghtrader.trading.strategy_control import (
    StrategyDesired,
    StrategyStateWriter,
    read_strategy_desired,
    strategy_state_path,
    write_strategy_desired,
)


def test_strategy_desired_roundtrip(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    d1 = StrategyDesired(
        mode="run",
        symbols=["SHFE.cu2602", "SHFE.au2602"],
        model_name="xgboost",
        horizon=20,
        threshold_up=0.61,
        threshold_down=0.59,
        position_size=2,
        artifacts_dir="artifacts",
        poll_interval_sec=0.25,
    )
    write_strategy_desired(runs_dir=runs_dir, profile="alt", desired=d1)

    d2 = read_strategy_desired(runs_dir=runs_dir, profile="alt")
    assert d2.mode == "run"
    assert d2.symbols_list() == ["SHFE.cu2602", "SHFE.au2602"]
    assert d2.model_name == "xgboost"
    assert int(d2.horizon) == 20
    assert float(d2.threshold_up) == 0.61
    assert float(d2.threshold_down) == 0.59
    assert int(d2.position_size) == 2
    assert str(d2.artifacts_dir) == "artifacts"


def test_strategy_state_writer_writes_state_and_events(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    w = StrategyStateWriter(runs_dir=runs_dir, profile="alt", recent_events_max=3)
    w.set_health(ok=True, running=True, error="", last_loop_at="2026-01-01T00:00:00Z")
    w.set_effective(model_name="xgboost", horizon=50)
    w.set_targets({"SHFE.cu2602": 1}, {"strategy_run_id": "r1"})
    w.append_event({"type": "strategy_start"})

    p = strategy_state_path(runs_dir=runs_dir, profile="alt")
    assert p.exists()
    obj = p.read_text(encoding="utf-8")
    assert "schema_version" in obj
    assert "health" in obj
    assert "recent_events" in obj

