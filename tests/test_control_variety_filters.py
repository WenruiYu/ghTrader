from __future__ import annotations

import importlib
import json
import os
import time
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


def test_models_inventory_filters_by_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    artifacts_dir = tmp_path / "artifacts"

    p_au = artifacts_dir / "KQ.m@SHFE.au" / "xgboost" / "model_h50.json"
    p_cu = artifacts_dir / "KQ.m@SHFE.cu" / "xgboost" / "model_h50.json"
    p_au.parent.mkdir(parents=True, exist_ok=True)
    p_cu.parent.mkdir(parents=True, exist_ok=True)
    p_au.write_text("{}", encoding="utf-8")
    p_cu.write_text("{}", encoding="utf-8")

    r = client.get("/api/models/inventory?var=au")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    symbols = [str(m.get("symbol") or "") for m in data.get("models") or []]
    assert symbols
    assert all(".au" in s.lower() for s in symbols)


def test_models_benchmarks_filters_by_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    runs_dir = tmp_path / "runs"

    p_ag = runs_dir / "benchmarks" / "KQ.m@SHFE.ag" / "xgboost" / "ag-run.json"
    p_cu = runs_dir / "benchmarks" / "KQ.m@SHFE.cu" / "xgboost" / "cu-run.json"
    p_ag.parent.mkdir(parents=True, exist_ok=True)
    p_cu.parent.mkdir(parents=True, exist_ok=True)
    p_ag.write_text(
        json.dumps(
            {
                "run_id": "ag-run",
                "timestamp": "2026-01-14T00:00:00Z",
                "model_type": "xgboost",
                "symbol": "KQ.m@SHFE.ag",
                "horizon": 50,
                "offline": {"accuracy": 0.7},
                "latency": {"inference_p95_ms": 2.0},
            }
        ),
        encoding="utf-8",
    )
    p_cu.write_text(
        json.dumps(
            {
                "run_id": "cu-run",
                "timestamp": "2026-01-13T00:00:00Z",
                "model_type": "xgboost",
                "symbol": "KQ.m@SHFE.cu",
                "horizon": 50,
                "offline": {"accuracy": 0.6},
                "latency": {"inference_p95_ms": 3.0},
            }
        ),
        encoding="utf-8",
    )

    r = client.get("/api/models/benchmarks?limit=10&var=ag")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    rows = data.get("benchmarks") or []
    assert rows
    assert all(".ag" in str((x or {}).get("symbol") or "").lower() for x in rows)


def test_trading_console_status_filters_symbols_by_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)

    runs_dir = tmp_path / "runs"
    gw_root = runs_dir / "gateway" / "account=default"
    gw_root.mkdir(parents=True, exist_ok=True)

    (gw_root / "desired.json").write_text(
        json.dumps({"desired": {"mode": "paper", "symbols": ["SHFE.au2602", "SHFE.cu2602"]}}),
        encoding="utf-8",
    )
    (gw_root / "state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-01-14T00:00:02Z",
                "health": {"ok": True, "connected": True},
                "effective": {"mode": "paper", "symbols": ["SHFE.au2602", "SHFE.cu2602"]},
                "last_snapshot": {
                    "symbols": ["SHFE.au2602", "SHFE.cu2602"],
                    "positions": {
                        "SHFE.au2602": {"volume_long": 1, "volume_short": 0},
                        "SHFE.cu2602": {"volume_long": 2, "volume_short": 0},
                    },
                    "orders_alive": [
                        {"symbol": "SHFE.au2602", "direction": "BUY", "volume_left": 1},
                        {"symbol": "SHFE.cu2602", "direction": "BUY", "volume_left": 1},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    now = time.time()
    os.utime(gw_root / "state.json", (now, now))

    r = client.get("/api/trading/console/status?account_profile=default&var=au")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    gw = data.get("gateway") or {}
    state = gw.get("state") or {}
    eff = state.get("effective") or {}
    snap = state.get("last_snapshot") or {}
    symbols = eff.get("symbols") or []
    assert symbols == ["SHFE.au2602"]
    assert set((snap.get("positions") or {}).keys()) == {"SHFE.au2602"}
    orders = snap.get("orders_alive") or []
    assert all("au" in str((o or {}).get("symbol") or "").lower() for o in orders)


class _Job:
    def __init__(
        self,
        *,
        job_id: str,
        status: str,
        title: str,
        command: list[str],
        metadata: dict | None = None,
    ) -> None:
        self.id = job_id
        self.status = status
        self.title = title
        self.command = command
        self.metadata = metadata
        self.source = "test"
        self.held_locks = []
        self.waiting_locks = []
        self.pid = None
        self.exit_code = None
        self.created_at = "2026-01-01T00:00:00Z"
        self.started_at = ""
        self.finished_at = ""


class _Store:
    def __init__(self, jobs: list[_Job]) -> None:
        self._jobs = jobs

    def list_jobs(self, limit: int = 200):  # type: ignore[no-untyped-def]
        _ = limit
        return list(self._jobs)


def test_jobs_page_defaults_to_current_variety_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    client.app.state.job_store = _Store(
        [
            _Job(
                job_id="j-au",
                status="running",
                title="main-schedule au env->latest",
                command=["python", "-m", "ghtrader.cli", "main-schedule", "--var", "au"],
            ),
            _Job(
                job_id="j-cu",
                status="running",
                title="main-schedule cu env->latest",
                command=["python", "-m", "ghtrader.cli", "main-schedule", "--var", "cu"],
            ),
        ]
    )

    r = client.get("/v/au/jobs")
    assert r.status_code == 200
    assert "main-schedule au env-&gt;latest" in r.text
    assert "main-schedule cu env-&gt;latest" not in r.text


def test_jobs_page_prefers_structured_variety_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    client.app.state.job_store = _Store(
        [
            _Job(
                job_id="j-meta-au",
                status="running",
                title="meta-au job",
                command=["python", "-m", "ghtrader.cli", "main-schedule", "--var", "cu"],
                metadata={"variety": "au"},
            ),
            _Job(
                job_id="j-meta-cu",
                status="running",
                title="meta-cu job",
                command=["python", "-m", "ghtrader.cli", "main-schedule", "--var", "au"],
                metadata={"variety": "cu"},
            ),
        ]
    )

    r = client.get("/v/au/jobs")
    assert r.status_code == 200
    assert "meta-au job" in r.text
    assert "meta-cu job" not in r.text
