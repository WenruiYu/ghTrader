from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _new_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return TestClient(mod.app)


def test_observability_slo_endpoint_ok_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import ghtrader.control.slo as slo

    monkeypatch.setattr(slo, "_questdb_status", lambda: {"ok": True})
    monkeypatch.setattr(slo, "_redis_status", lambda: {"enabled": False, "ok": True, "status": "disabled"})
    monkeypatch.setattr(slo, "_gpu_count", lambda: 8)

    client = _new_client(tmp_path, monkeypatch)
    resp = client.get("/api/observability/slo")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["overall"] in {"ok", "warn", "error"}
    assert "data_plane" in payload
    assert "training_plane" in payload
    assert "control_plane" in payload


def test_observability_slo_queue_threshold_warn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import ghtrader.control.slo as slo

    monkeypatch.setattr(slo, "_questdb_status", lambda: {"ok": True})
    monkeypatch.setattr(slo, "_redis_status", lambda: {"enabled": False, "ok": True, "status": "disabled"})
    monkeypatch.setattr(slo, "_gpu_count", lambda: 8)
    monkeypatch.setenv("GHTRADER_SLO_QUEUE_WARN", "2")
    monkeypatch.setenv("GHTRADER_SLO_QUEUE_CRIT", "10")

    client = _new_client(tmp_path, monkeypatch)

    class _Job:
        def __init__(self, status: str) -> None:
            self.status = status

    class _Store:
        def list_jobs(self, limit: int = 2000):  # type: ignore[no-untyped-def]
            _ = limit
            return [_Job("running"), _Job("queued"), _Job("queued")]

    client.app.state.job_store = _Store()
    resp = client.get("/api/observability/slo")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["control_plane"]["state"] == "warn"
