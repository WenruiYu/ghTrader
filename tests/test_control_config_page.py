from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _make_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return TestClient(mod.app)


def test_config_page_renders_and_keeps_token_query(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)
    r = client.get("/v/cu/config?token=abc123")
    assert r.status_code == 200
    body = r.text
    assert "Configuration" in body
    assert "/v/cu/config?token=abc123" in body
    assert "/v/au/config?token=abc123" in body
    assert "/api/config/schema" in body
    assert "/api/config/effective-preview" in body

