from __future__ import annotations

import importlib
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _wait_for_job_done(client: TestClient, job_id: str, *, timeout_s: float = 10.0) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        data = client.get("/api/jobs").json()
        job = next(j for j in data["jobs"] if j["id"] == job_id)
        if job["status"] not in {"queued", "running"}:
            return job
        time.sleep(0.05)
    raise AssertionError("timeout waiting for job to finish")


def test_control_api_job_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Isolate control-plane storage
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    app = mod.app

    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True

    create = client.post(
        "/api/jobs",
        json={
            "title": "echo",
            "argv": [sys.executable, "-c", "print('api-hello')"],
            "cwd": str(tmp_path),
        },
    )
    assert create.status_code == 200
    job_id = create.json()["job"]["id"]

    job = _wait_for_job_done(client, job_id, timeout_s=10.0)
    assert job["status"] in {"succeeded", "failed"}

    log_text = client.get(f"/api/jobs/{job_id}/log").text
    assert "api-hello" in log_text


def _touch_tick_day(data_dir: Path, symbol: str, day: date) -> None:
    d = data_dir / "lake" / "ticks" / f"symbol={symbol}" / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "part-test.parquet").write_bytes(b"PAR1")


def _write_no_data_dates(data_dir: Path, symbol: str, days: list[date]) -> None:
    p = data_dir / "lake" / "ticks" / f"symbol={symbol}" / "_no_data_dates.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps([d.isoformat() for d in days], indent=2))


def test_control_api_ingest_status_endpoints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    app = mod.app
    client = TestClient(app)

    store = app.state.job_store
    data_dir = tmp_path / "data"

    sym1 = "SHFE.cu2501"
    _touch_tick_day(data_dir, sym1, date(2025, 1, 2))
    _touch_tick_day(data_dir, sym1, date(2025, 1, 3))
    _write_no_data_dates(data_dir, sym1, [date(2025, 1, 6)])

    sym2 = "SHFE.cu2502"
    _touch_tick_day(data_dir, sym2, date(2025, 1, 2))
    _touch_tick_day(data_dir, sym2, date(2025, 1, 3))
    _touch_tick_day(data_dir, sym2, date(2025, 1, 6))
    _touch_tick_day(data_dir, sym2, date(2025, 1, 7))

    job_id = "ingest123"
    log_path = tmp_path / "runs" / "control" / "logs" / f"job-{job_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "\n".join(
            [
                "2026-01-12T15:00:00.000000Z [info     ] tq_ingest.contract_download     symbol=SHFE.cu2502 start=2025-02-03 end=2025-02-05",
                "2026-01-12T15:00:01.000000Z [info     ] tq_ingest.chunk_download        symbol=SHFE.cu2502 chunk_start=2025-02-03 chunk_end=2025-02-05 chunk_idx=1",
            ]
        )
        + "\n"
    )

    cmd = [
        sys.executable,
        "-m",
        "ghtrader.cli",
        "download-contract-range",
        "--exchange",
        "SHFE",
        "--var",
        "cu",
        "--start-contract",
        "2501",
        "--end-contract",
        "2502",
        "--start-date",
        "2025-01-02",
        "--end-date",
        "2025-01-07",
        "--data-dir",
        str(data_dir),
        "--chunk-days",
        "5",
    ]
    store.create_job(job_id=job_id, title="download-contract-range cu 2501->2502", command=cmd, cwd=tmp_path, source="terminal", log_path=log_path)
    store.update_job(job_id, status="running", pid=123, started_at="2026-01-01T00:00:00Z")

    r = client.get(f"/api/jobs/{job_id}/ingest_status")
    assert r.status_code == 200
    s = r.json()
    assert s["kind"] == "download_contract_range"
    assert s["summary"]["days_expected_total"] == 8
    assert s["summary"]["days_done_total"] == 7
    assert s["job_status"] == "running"

    r2 = client.get("/api/ingest/status")
    assert r2.status_code == 200
    jobs = r2.json()["jobs"]
    assert any(j.get("job_id") == job_id for j in jobs)

