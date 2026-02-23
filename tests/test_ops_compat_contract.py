from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


class _DummyRec:
    def __init__(self, job_id: str = "ops-compat-job") -> None:
        self.id = job_id


class _CaptureJobManager:
    def __init__(self) -> None:
        self.last_spec = None

    def start_job(self, spec):  # type: ignore[no-untyped-def]
        self.last_spec = spec
        return _DummyRec()


def _new_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    cap_jm = _CaptureJobManager()
    mod.app.state.job_manager = cap_jm
    return TestClient(mod.app), cap_jm


def test_api_ops_compat_removed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _new_client(tmp_path, monkeypatch)
    resp = client.get("/api/ops/compat")
    assert resp.status_code == 404


def test_ops_model_train_alias_removed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _new_client(tmp_path, monkeypatch)
    resp = client.post(
        "/ops/model/train",
        data={
            "model": "xgboost",
            "symbol": "KQ.m@SHFE.cu",
            "gpus": "1",
            "ddp": "false",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 404


def test_models_model_train_canonical_still_submits_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, cap_jm = _new_client(tmp_path, monkeypatch)

    resp = client.post(
        "/models/model/train",
        data={
            "model": "xgboost",
            "symbol": "KQ.m@SHFE.cu",
            "gpus": "1",
            "ddp": "false",
        },
        follow_redirects=False,
    )
    assert resp.status_code in {302, 303}
    assert cap_jm.last_spec is not None
    assert "train" in list(cap_jm.last_spec.argv)
