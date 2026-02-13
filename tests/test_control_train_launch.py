from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


class _DummyRec:
    def __init__(self, job_id: str = "job-test") -> None:
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

    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)
    cap_jm = _CaptureJobManager()
    app_mod.app.state.job_manager = cap_jm
    return TestClient(app_mod.app), cap_jm


def test_models_train_uses_torchrun_for_multi_gpu(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, cap_jm = _new_client(tmp_path, monkeypatch)

    import ghtrader.control.views as views_mod

    monkeypatch.setattr(views_mod.shutil, "which", lambda cmd: "/usr/bin/torchrun" if "torchrun" in str(cmd) else None)

    resp = client.post(
        "/models/model/train",
        data={
            "model": "deeplob",
            "symbol": "KQ.m@SHFE.cu",
            "gpus": "4",
            "ddp": "true",
        },
        follow_redirects=False,
    )

    assert resp.status_code in {302, 303}
    assert cap_jm.last_spec is not None
    argv = list(cap_jm.last_spec.argv)
    assert argv[0] == "/usr/bin/torchrun"
    assert "--nproc_per_node=4" in argv
    assert "--ddp" in argv


def test_models_train_falls_back_when_torchrun_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, cap_jm = _new_client(tmp_path, monkeypatch)

    import ghtrader.control.views as views_mod

    monkeypatch.setattr(views_mod.shutil, "which", lambda _cmd: None)

    resp = client.post(
        "/models/model/train",
        data={
            "model": "deeplob",
            "symbol": "KQ.m@SHFE.cu",
            "gpus": "8",
            "ddp": "true",
        },
        follow_redirects=False,
    )

    assert resp.status_code in {302, 303}
    assert cap_jm.last_spec is not None
    argv = list(cap_jm.last_spec.argv)
    assert argv[0] == sys.executable
    assert "--no-ddp" in argv
    gi = argv.index("--gpus")
    assert argv[gi + 1] == "1"
    assert "fallback-single-no-torchrun" in str(cap_jm.last_spec.title)


def test_sweep_is_gpu_muted_when_ddp_train_active(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, cap_jm = _new_client(tmp_path, monkeypatch)

    class _Job:
        def __init__(self) -> None:
            self.command = [
                "torchrun",
                "--standalone",
                "--nnodes=1",
                "--nproc_per_node=8",
                "-m",
                "ghtrader.cli",
                "train",
                "--model",
                "deeplob",
            ]
            self.title = "train deeplob KQ.m@SHFE.cu mode=torchrun-ddp-8 gpus=8"

    class _Store:
        def list_active_jobs(self):  # type: ignore[no-untyped-def]
            return [_Job()]

        def list_unstarted_queued_jobs(self, limit: int = 2000):  # type: ignore[no-untyped-def]
            _ = limit
            return []

    client.app.state.job_store = _Store()

    resp = client.post(
        "/models/model/sweep",
        data={
            "symbol": "KQ.m@SHFE.cu",
            "model": "deeplob",
            "n_trials": "2",
            "n_cpus": "4",
            "n_gpus": "2",
        },
        follow_redirects=False,
    )

    assert resp.status_code in {302, 303}
    assert cap_jm.last_spec is not None
    argv = list(cap_jm.last_spec.argv)
    gi = argv.index("--n-gpus")
    assert argv[gi + 1] == "0"
    assert "cpu_only_ddp_train_active" in str(cap_jm.last_spec.title)
