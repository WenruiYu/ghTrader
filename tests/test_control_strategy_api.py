from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_api_strategy_status_and_desired_and_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # No files yet.
    r0 = client.get("/api/strategy/status?account_profile=alt")
    assert r0.status_code == 200
    j0 = r0.json()
    assert j0["ok"] is True
    assert j0["account_profile"] == "alt"
    assert j0["exists"] is False

    # Set desired.
    r1 = client.post(
        "/api/strategy/desired",
        json={
            "account_profile": "alt",
            "desired": {
                "mode": "run",
                "symbols": ["SHFE.cu2602"],
                "model_name": "xgboost",
                "horizon": 50,
                "threshold_up": 0.6,
                "threshold_down": 0.6,
                "position_size": 1,
                "artifacts_dir": "artifacts",
                "poll_interval_sec": 0.5,
            },
        },
    )
    assert r1.status_code == 200
    assert r1.json()["ok"] is True

    r2 = client.get("/api/strategy/status?account_profile=alt")
    assert r2.status_code == 200
    j2 = r2.json()
    assert j2["exists"] is True
    assert isinstance(j2["desired"], dict)

    # Runs endpoint should work (may be empty).
    r3 = client.get("/api/strategy/runs?limit=5")
    assert r3.status_code == 200
    out = r3.json()
    assert "runs" in out

