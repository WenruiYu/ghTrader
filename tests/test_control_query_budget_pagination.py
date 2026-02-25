from __future__ import annotations

import importlib
import json
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


def test_api_jobs_limit_and_offset_are_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)

    r1 = client.get("/api/jobs?limit=999999&offset=-42")
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["limit"] == 1000
    assert body1["offset"] == 0

    r2 = client.get("/api/jobs?limit=0&offset=999999")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["limit"] == 1
    assert body2["offset"] == 5000


def test_api_job_log_max_bytes_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    captured: dict[str, int] = {}

    def _fake_read_log_tail(job_id: str, max_bytes: int = 64000) -> str:
        _ = job_id
        captured["max_bytes"] = int(max_bytes)
        return "ok"

    monkeypatch.setattr(client.app.state.job_manager, "read_log_tail", _fake_read_log_tail, raising=True)

    r1 = client.get("/api/jobs/sample-job/log?max_bytes=99999999")
    assert r1.status_code == 200
    assert captured["max_bytes"] == 1_048_576

    r2 = client.get("/api/jobs/sample-job/log?max_bytes=1")
    assert r2.status_code == 200
    assert captured["max_bytes"] == 1024


def test_models_inventory_respects_max_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    artifacts_dir = tmp_path / "artifacts"
    for i in range(3):
        p = artifacts_dir / "KQ.m@SHFE.cu" / "xgboost" / f"model_h{50 + i}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{}", encoding="utf-8")

    r = client.get("/api/models/inventory?max_rows=1")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert len(body.get("models") or []) == 1


def test_data_report_limits_are_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    runs_dir = tmp_path / "runs"

    diagnose_dir = runs_dir / "control" / "reports" / "data_diagnose"
    diagnose_dir.mkdir(parents=True, exist_ok=True)
    for i in range(60):
        p = diagnose_dir / f"diagnose_exchange=SHFE_var=cu_{i:03d}.json"
        p.write_text(json.dumps({"ok": True, "i": i}), encoding="utf-8")

    r1 = client.get("/api/data/reports?kind=diagnose&exchange=SHFE&var=cu&limit=999")
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["ok"] is True
    assert body1["count"] == 50
    assert len(body1.get("reports") or []) == 50

    l5_start_dir = runs_dir / "control" / "reports" / "l5_start"
    l5_start_dir.mkdir(parents=True, exist_ok=True)
    for i in range(30):
        p = l5_start_dir / f"l5_start_exchange=SHFE_var=cu_{i:03d}.json"
        p.write_text(json.dumps({"ok": True, "i": i}), encoding="utf-8")

    r2 = client.get("/api/data/l5-start?exchange=SHFE&var=cu&limit=999")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["ok"] is True
    assert body2["count"] == 20
    assert len(body2.get("reports") or []) == 20


def test_main_l5_validate_report_limit_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    rep_dir = tmp_path / "runs" / "control" / "reports" / "main_l5_validate"
    rep_dir.mkdir(parents=True, exist_ok=True)
    for i in range(30):
        p = rep_dir / f"main_l5_validate_exchange=SHFE_var=cu_symbol=kq_m_shfe_cu_{i:03d}.json"
        p.write_text(json.dumps({"ok": True, "i": i}), encoding="utf-8")

    r = client.get("/api/data/main-l5-validate?exchange=SHFE&var=cu&symbol=KQ.m@SHFE.cu&limit=999")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["count"] == 20
    assert len(body.get("reports") or []) == 20


def test_gateway_list_and_strategy_runs_limits_are_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    runs_dir = tmp_path / "runs"

    gateway_root = runs_dir / "gateway"
    for prof in ("a", "b", "c"):
        (gateway_root / f"account={prof}").mkdir(parents=True, exist_ok=True)
    rg = client.get("/api/gateway/list?limit=1")
    assert rg.status_code == 200
    assert len(rg.json().get("profiles") or []) == 1

    strategy_root = runs_dir / "strategy"
    for run_id in ("20260101-000001", "20260101-000002", "20260101-000003"):
        d = strategy_root / run_id
        d.mkdir(parents=True, exist_ok=True)
        (d / "run_config.json").write_text(json.dumps({"account_profile": "default"}), encoding="utf-8")
    rs = client.get("/api/strategy/runs?limit=0")
    assert rs.status_code == 200
    assert len(rs.json().get("runs") or []) == 1


def test_questdb_query_limit_is_bounded_and_forwarded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)

    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.queries as qqueries

    captured: dict[str, int] = {}
    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())

    def _fake_query_sql_read_only(*, cfg, query: str, limit: int, connect_timeout_s: int):
        _ = (cfg, query, connect_timeout_s)
        captured["limit"] = int(limit)
        return ["x"], [[1]]

    monkeypatch.setattr(qqueries, "query_sql_read_only", _fake_query_sql_read_only)

    r1 = client.post("/api/questdb/query", json={"query": "select 1", "limit": 999999})
    assert r1.status_code == 200
    assert captured["limit"] == 500

    r2 = client.post("/api/questdb/query", json={"query": "select 1", "limit": 0})
    assert r2.status_code == 200
    assert captured["limit"] == 1


def test_config_history_limit_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(tmp_path, monkeypatch)
    import ghtrader.control.routes.core as core_routes

    captured: dict[str, int] = {}

    class _Resolver:
        def list_revisions(self, limit: int):
            captured["limit"] = int(limit)
            return []

    monkeypatch.setattr(core_routes, "get_config_resolver", lambda: _Resolver())

    r = client.get("/api/config/history?limit=999999")
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert captured["limit"] == 500
