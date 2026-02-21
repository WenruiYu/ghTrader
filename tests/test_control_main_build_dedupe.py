from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ghtrader.control.jobs import python_module_argv


class _DummyRec:
    def __init__(self, job_id: str) -> None:
        self.id = job_id


class _CaptureJobManager:
    def __init__(self) -> None:
        self.start_job_calls = 0
        self.start_queued_calls: list[str] = []

    def start_job(self, spec):  # type: ignore[no-untyped-def]
        _ = spec
        self.start_job_calls += 1
        return _DummyRec("new-job")

    def start_queued_job(self, job_id: str):  # type: ignore[no-untyped-def]
        self.start_queued_calls.append(str(job_id))
        return _DummyRec(str(job_id))


class _Job:
    def __init__(self, *, job_id: str, command: list[str], status: str, pid: int | None) -> None:
        self.id = str(job_id)
        self.command = list(command)
        self.status = str(status)
        self.pid = pid
        self.title = ""


class _Store:
    def __init__(self, *, active: list[_Job] | None = None, queued: list[_Job] | None = None) -> None:
        self._active = list(active or [])
        self._queued = list(queued or [])

    def list_active_jobs(self):  # type: ignore[no-untyped-def]
        return list(self._active)

    def list_unstarted_queued_jobs(self, limit: int = 2000):  # type: ignore[no-untyped-def]
        _ = limit
        return list(self._queued)


def _new_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    cap_jm = _CaptureJobManager()
    mod.app.state.job_manager = cap_jm
    return TestClient(mod.app), cap_jm


def test_data_build_main_schedule_reuses_existing_running_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, cap_jm = _new_client(tmp_path, monkeypatch)

    existing = _Job(
        job_id="existing-running",
        command=python_module_argv("ghtrader.cli", "main-schedule", "--var", "cu", "--data-dir", "data"),
        status="running",
        pid=4321,
    )
    client.app.state.job_store = _Store(active=[existing], queued=[])

    resp = client.post(
        "/data/build/main_schedule",
        data={"variety": "cu", "data_dir": "data"},
        follow_redirects=False,
    )

    assert resp.status_code in {302, 303}
    assert "/jobs/existing-running" in str(resp.headers.get("location") or "")
    assert cap_jm.start_job_calls == 0
    assert cap_jm.start_queued_calls == []


def test_jobs_start_main_l5_reuses_existing_unstarted_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, cap_jm = _new_client(tmp_path, monkeypatch)

    existing = _Job(
        job_id="existing-queued",
        command=python_module_argv("ghtrader.cli", "main-l5", "--var", "cu", "--symbol", "KQ.m@SHFE.cu"),
        status="queued",
        pid=None,
    )
    client.app.state.job_store = _Store(active=[], queued=[existing])

    resp = client.post(
        "/jobs/start",
        data={
            "job_type": "main_l5",
            "symbol_or_var": "cu",
            "derived_symbol": "KQ.m@SHFE.cu",
            "data_dir": "data",
        },
        follow_redirects=False,
    )

    assert resp.status_code in {302, 303}
    assert "/jobs/existing-queued" in str(resp.headers.get("location") or "")
    assert cap_jm.start_job_calls == 0
    assert cap_jm.start_queued_calls == ["existing-queued"]
