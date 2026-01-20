from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_ops_page_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that /ops redirects to Data Hub with unified workflow.
    
    The Pipeline tab has been consolidated into the Contracts tab workflow.
    /ops now redirects to /data#contracts.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # /ops should redirect to /data#contracts
    r = client.get("/ops", follow_redirects=False)
    assert r.status_code in {302, 303, 307, 308}
    assert "/data" in r.headers.get("location", "")
    assert "#contracts" in r.headers.get("location", "")

    # Following the redirect should render the Data Hub with unified workflow
    r_data = client.get("/data")
    assert r_data.status_code == 200
    html = r_data.text
    # Check for unified 8-step workflow in Contracts tab
    assert "Workflow Steps" in html
    assert "Build Schedule" in html
    assert "main_l5" in html

    # Legacy Ops pages redirect to consolidated page.
    r2 = client.get("/ops/ingest", follow_redirects=False)
    assert r2.status_code in {302, 303, 307, 308}

