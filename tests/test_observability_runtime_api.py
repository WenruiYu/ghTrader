from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _new_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return TestClient(mod.app)


def test_observability_runtime_endpoint_exposes_control_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _new_client(tmp_path, monkeypatch)

    # Prime middleware metrics with a real request.
    resp0 = client.get("/api/system")
    assert resp0.status_code == 200

    resp = client.get("/api/observability/runtime")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload.get("ok") is True
    domains = payload.get("domains") or {}
    assert isinstance(domains, dict)
    assert "control.api" in domains
    control_metrics = domains["control.api"]
    assert isinstance(control_metrics, dict)
    assert any(str(k).startswith("GET /api/system") for k in control_metrics.keys())

