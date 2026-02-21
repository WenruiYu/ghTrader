from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


class _DummyRec:
    def __init__(self, job_id: str) -> None:
        self.id = str(job_id)


class _CaptureJobManager:
    def __init__(self) -> None:
        self.start_job_calls = 0

    def start_job(self, spec):  # type: ignore[no-untyped-def]
        _ = spec
        self.start_job_calls += 1
        return _DummyRec("new-job")

    def start_queued_job(self, job_id: str):  # type: ignore[no-untyped-def]
        _ = job_id
        return None


class _Store:
    def list_jobs(self, limit: int = 200):  # type: ignore[no-untyped-def]
        _ = limit
        return []

    def list_active_jobs(self):  # type: ignore[no-untyped-def]
        return []

    def list_unstarted_queued_jobs(self, limit: int = 2000):  # type: ignore[no-untyped-def]
        _ = limit
        return []


def _make_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    mod.app.state.job_manager = _CaptureJobManager()
    mod.app.state.job_store = _Store()
    return TestClient(mod.app)


def test_variety_pages_use_context_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)

    r_data = client.get("/v/au/data")
    assert r_data.status_code == 200
    assert 'name="variety" value="au"' in r_data.text
    assert 'name="derived_symbol" value="KQ.m@SHFE.au"' in r_data.text

    r_models = client.get("/v/ag/models")
    assert r_models.status_code == 200
    assert 'value="KQ.m@SHFE.ag"' in r_models.text


def test_main_schedule_submit_requires_variety(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    r = client.post("/data/build/main_schedule", data={"data_dir": "data"}, follow_redirects=False)
    assert r.status_code == 400


def test_variety_submit_redirect_keeps_var_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    r = client.post(
        "/data/build/main_schedule",
        data={"variety": "au", "data_dir": "data"},
        follow_redirects=False,
    )
    assert r.status_code in {302, 303}
    location = str(r.headers.get("location") or "")
    assert "/jobs/new-job" in location
    assert "var=au" in location
