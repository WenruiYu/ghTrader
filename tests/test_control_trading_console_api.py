from __future__ import annotations

import importlib
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
    assert set(j) == {"ok", "account_profile", "live_enabled", "gateway", "strategy", "generated_at"}
    assert j["gateway"]["exists"] is False
    assert j["strategy"]["exists"] is False

