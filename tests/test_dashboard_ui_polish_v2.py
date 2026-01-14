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

    # Ensure QuestDB checks fail fast (connection refused).
    monkeypatch.setenv("GHTRADER_QUESTDB_HOST", "127.0.0.1")
    monkeypatch.setenv("GHTRADER_QUESTDB_PG_PORT", "1")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return TestClient(mod.app)


def test_models_and_trading_pages_render(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)

    r1 = client.get("/models")
    assert r1.status_code == 200
    assert "Models" in r1.text

    r2 = client.get("/trading")
    assert r2.status_code == 200
    assert "Trading Console" in r2.text


def test_api_ui_status_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)

    r = client.get("/api/ui/status")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert isinstance(data["questdb_ok"], bool)
    assert isinstance(data["gpu_status"], str)
    assert isinstance(data["running_count"], int)
    assert isinstance(data["queued_count"], int)


def test_api_dashboard_summary_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)

    r = client.get("/api/dashboard/summary")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert "pipeline" in data
    assert isinstance(data["data_symbols_v1"], int)
    assert isinstance(data["data_symbols_v2"], int)
    assert isinstance(data["data_symbols_union"], int)
    assert isinstance(data["model_count"], int)


def test_api_models_inventory_scans_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)

    artifacts_dir = tmp_path / "artifacts"
    p = artifacts_dir / "KQ.m@SHFE.cu" / "xgboost" / "model_h50.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{}", encoding="utf-8")

    r = client.get("/api/models/inventory")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    models = data["models"]
    assert any(m["symbol"] == "KQ.m@SHFE.cu" and m["model_type"] == "xgboost" and int(m["horizon"]) == 50 for m in models)


def test_api_models_benchmarks_lists_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)

    runs_dir = tmp_path / "runs"
    report_path = runs_dir / "benchmarks" / "KQ.m@SHFE.cu" / "xgboost" / "run123.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "run_id": "run123",
                "timestamp": "2026-01-14T00:00:00Z",
                "model_type": "xgboost",
                "symbol": "KQ.m@SHFE.cu",
                "horizon": 50,
                "offline": {"accuracy": 0.7, "f1_macro": 0.6, "log_loss": 0.9, "ece": 0.1},
                "latency": {"inference_p95_ms": 2.5},
            }
        ),
        encoding="utf-8",
    )

    r = client.get("/api/models/benchmarks?limit=5")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    benches = data["benchmarks"]
    assert any(b["run_id"] == "run123" and b["model_type"] == "xgboost" and b["symbol"] == "KQ.m@SHFE.cu" for b in benches)


def test_api_trading_status_schema_and_derivations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _make_client(tmp_path, monkeypatch)

    runs_dir = tmp_path / "runs"
    run_id = "20260114_000000"
    run_dir = runs_dir / "trading" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "run_config.json").write_text(
        json.dumps(
            {
                "created_at": "2026-01-14T00:00:00Z",
                "mode": "paper",
                "monitor_only": True,
                "executor": "targetpos",
                "model_name": "xgboost",
                "symbols_requested": ["KQ.m@SHFE.cu"],
                "symbols_resolved": ["SHFE.cu2602"],
                "limits": {"max_abs_position": 1, "max_order_size": 1, "max_ops_per_sec": 10},
            }
        ),
        encoding="utf-8",
    )

    snap = {
        "ts": "2026-01-14T00:00:01Z",
        "symbols": ["SHFE.cu2602"],
        "account": {"balance": 100.0, "float_profit": 5.0, "position_profit": 0.0},
        "positions": {"SHFE.cu2602": {"volume_long": 2, "volume_short": 1, "float_profit_long": 1.0, "float_profit_short": -0.5}},
        "orders_alive": [],
    }
    (run_dir / "snapshots.jsonl").write_text(json.dumps(snap) + "\n", encoding="utf-8")
    # Ensure "active" detection (mtime within 5 minutes)
    now = time.time()
    os.utime(run_dir / "snapshots.jsonl", (now, now))

    r = client.get("/api/trading/status")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["active"] is True
    assert data["run_id"] == run_id
    assert data["mode"] == "paper"
    assert data["model"] == "xgboost"
    assert data["account"]["equity"] == pytest.approx(105.0)
    assert data["pnl"] == pytest.approx(5.0)
    assert data["position"]["symbol"] == "SHFE.cu2602"
    assert int(data["position"]["volume"]) == 1


def test_base_template_has_ok_false_guard_and_ui_status_endpoint():
    repo_root = Path(__file__).resolve().parents[1]
    base = (repo_root / "src" / "ghtrader" / "control" / "templates" / "base.html").read_text(encoding="utf-8")
    assert "/api/ui/status" in base
    assert "body.ok === false" in base
    assert "initTabs" in base

