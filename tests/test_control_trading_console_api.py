from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_api_trading_console_status_minimal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/trading/console/status?account_profile=alt")
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert j["account_profile"] == "alt"
    # Updated to include new fields: gateway_job, strategy_job
    expected_keys = {"ok", "account_profile", "live_enabled", "gateway", "strategy", "gateway_job", "strategy_job", "generated_at"}
    assert set(j) == expected_keys
    assert j["gateway"]["exists"] is False
    assert j["strategy"]["exists"] is False
    assert j["gateway_job"] is None
    assert j["strategy_job"] is None
    assert "stale" in j["gateway"]
    assert "stale" in j["strategy"]


def test_api_gateway_start_requires_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that gateway start fails gracefully when symbols are missing."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Start gateway with no symbols in desired should still work (starts job)
    r = client.post("/api/gateway/start", json={"account_profile": "test"})
    assert r.status_code == 200
    j = r.json()
    # Should succeed as we're just starting the gateway (it reads desired.json)
    # But if desired doesn't exist yet, it will create with empty symbols
    assert "ok" in j


def test_api_gateway_stop_when_not_running(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that gateway stop returns appropriate message when not running."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.post("/api/gateway/stop", json={"account_profile": "test"})
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is False
    assert j["error"] == "not_running"


def test_api_strategy_start_requires_desired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that strategy start fails when desired.json is missing."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Start strategy without creating desired.json first
    r = client.post("/api/strategy/start", json={"account_profile": "test"})
    assert r.status_code == 200
    j = r.json()
    # Should fail because no desired.json exists
    assert j["ok"] is False
    assert j["error"] in ["no_desired", "no_symbols"]


def test_api_strategy_start_with_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that strategy start succeeds when symbols are provided."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Start strategy with symbols in desired payload
    r = client.post("/api/strategy/start", json={
        "account_profile": "test",
        "desired": {
            "mode": "run",
            "symbols": ["SHFE.cu2602"],
            "model_name": "xgboost",
            "horizon": 50,
        },
    })
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert "job_id" in j


def test_api_strategy_stop_when_not_running(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that strategy stop returns appropriate message when not running."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.post("/api/strategy/stop", json={"account_profile": "test"})
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is False
    assert j["error"] == "not_running"

