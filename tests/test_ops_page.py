from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_ops_page_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/ops")
    assert r.status_code == 200
    html = r.text
    assert "Happy Path Pipeline" in html
    assert "Sync cu to QuestDB" in html
    assert "main_l5" in html

    # Legacy Ops pages redirect to consolidated page.
    r2 = client.get("/ops/ingest", follow_redirects=False)
    assert r2.status_code in {302, 303, 307, 308}

