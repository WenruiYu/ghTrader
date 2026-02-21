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


@pytest.mark.parametrize("variety", ["cu", "au", "ag"])
@pytest.mark.parametrize("page", ["dashboard", "data", "models", "jobs", "trading"])
def test_variety_workspace_routes_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variety: str, page: str
) -> None:
    client = _make_client(tmp_path, monkeypatch)
    r = client.get(f"/v/{variety}/{page}")
    assert r.status_code == 200


def test_invalid_variety_route_returns_404(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    r = client.get("/v/ni/dashboard", follow_redirects=False)
    assert r.status_code == 404


@pytest.mark.parametrize(
    ("legacy_path", "target_prefix"),
    [
        ("/", "/v/cu/dashboard"),
        ("/data", "/v/cu/data"),
        ("/models", "/v/cu/models"),
        ("/jobs", "/v/cu/jobs"),
        ("/trading", "/v/cu/trading"),
        ("/ops", "/v/cu/data"),
    ],
)
def test_legacy_routes_redirect_to_default_variety(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy_path: str, target_prefix: str
) -> None:
    client = _make_client(tmp_path, monkeypatch)
    r = client.get(f"{legacy_path}?token=t", follow_redirects=False)
    assert r.status_code in {302, 303, 307, 308}
    location = str(r.headers.get("location") or "")
    assert location.startswith(target_prefix)
    assert "token=t" in location
