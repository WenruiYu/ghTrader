from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_ops_page_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy /ops entrypoints are removed after compatibility cutover."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # /ops should be removed
    r = client.get("/ops", follow_redirects=False)
    assert r.status_code == 404

    # Canonical data route remains healthy.
    r_data = client.get("/data")
    assert r_data.status_code == 200
    html = r_data.text
    assert "Main schedule" in html
    assert "Build Schedule" in html
    assert "Build main_l5" in html

    # Other /ops aliases are removed as well.
    r2 = client.get("/ops/ingest", follow_redirects=False)
    assert r2.status_code == 404

