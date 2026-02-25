from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from ghtrader.trading.strategy_runner import (
    _ZmqConnectionHealthFsm,
    compute_target,
    gateway_targets_path,
    write_gateway_targets,
)


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


def test_hot_path_fsm_enters_degraded_after_consecutive_failures() -> None:
    fsm = _ZmqConnectionHealthFsm(enter_degraded_after_failures=3, recover_after_successes=2)
    assert fsm.state == "starting"
    assert fsm.record_failure(reason="timeout", now_ts=1.0) is True
    assert fsm.state == "suspect"
    assert fsm.record_failure(reason="timeout", now_ts=2.0) is False
    assert fsm.state == "suspect"
    assert fsm.record_failure(reason="timeout", now_ts=3.0) is True
    assert fsm.state == "degraded"


def test_hot_path_fsm_requires_consecutive_successes_to_recover() -> None:
    fsm = _ZmqConnectionHealthFsm(enter_degraded_after_failures=2, recover_after_successes=2)
    fsm.record_failure(reason="timeout", now_ts=1.0)
    fsm.record_failure(reason="timeout", now_ts=2.0)
    assert fsm.state == "degraded"
    assert fsm.record_success(now_ts=3.0) is False
    assert fsm.state == "degraded"
    assert fsm.record_success(now_ts=4.0) is True
    assert fsm.state == "healthy"


def test_hot_path_fsm_success_resets_failure_counter() -> None:
    fsm = _ZmqConnectionHealthFsm(enter_degraded_after_failures=3, recover_after_successes=1)
    fsm.record_failure(reason="timeout", now_ts=1.0)
    assert fsm.consecutive_failures == 1
    fsm.record_success(now_ts=2.0)
    assert fsm.state == "healthy"
    assert fsm.consecutive_failures == 0


def test_hot_path_fsm_degraded_is_sticky_until_recovered() -> None:
    fsm = _ZmqConnectionHealthFsm(enter_degraded_after_failures=2, recover_after_successes=2)
    fsm.record_failure(reason="timeout", now_ts=1.0)
    fsm.record_failure(reason="timeout", now_ts=2.0)
    assert fsm.state == "degraded"
    fsm.record_success(now_ts=3.0)
    assert fsm.state == "degraded"
    # A new failure before recovery should keep degraded state.
    assert fsm.record_failure(reason="timeout", now_ts=4.0) is False
    assert fsm.state == "degraded"

