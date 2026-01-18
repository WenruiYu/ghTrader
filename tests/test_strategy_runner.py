from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from ghtrader.trading.strategy_runner import compute_target, gateway_targets_path, write_gateway_targets


def test_compute_target_thresholds() -> None:
    # probs = [down, flat, up]
    assert compute_target(np.array([0.1, 0.8, 0.1]), threshold_up=0.6, threshold_down=0.6, position_size=3) == 0
    assert compute_target(np.array([0.7, 0.2, 0.1]), threshold_up=0.6, threshold_down=0.6, position_size=3) == -3
    assert compute_target(np.array([0.1, 0.2, 0.7]), threshold_up=0.6, threshold_down=0.6, position_size=3) == 3


def test_write_gateway_targets(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    write_gateway_targets(
        runs_dir=runs_dir,
        account_profile="Alt-01",
        targets={"SHFE.cu2602": 1},
        meta={"model_name": "xgboost"},
    )
    p = gateway_targets_path(runs_dir=runs_dir, account_profile="alt_01")
    assert p.exists()
    obj = json.loads(p.read_text(encoding="utf-8"))
    assert obj["schema_version"] == 1
    assert obj["account_profile"] == "alt_01"
    assert obj["targets"]["SHFE.cu2602"] == 1
    assert obj["meta"]["model_name"] == "xgboost"

