from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_api_contracts_graceful_without_tqsdk_creds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.delenv("TQSDK_USER", raising=False)
    monkeypatch.delenv("TQSDK_PASSWORD", raising=False)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["contracts"] == []
    assert "tqsdk" in str(body.get("error") or "").lower()


def test_api_contracts_attaches_questdb_coverage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Provide a fake catalog so the endpoint doesn't require real TqSdk.
    import ghtrader.tqsdk_catalog as cat

    def fake_catalog(**kwargs):
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None}],
        }

    monkeypatch.setattr(cat, "get_contract_catalog", fake_catalog)

    # Fake QuestDB coverage so this test stays offline.
    import ghtrader.questdb_queries as qq

    def fake_cov(**kwargs):
        symbols = list(kwargs.get("symbols") or [])
        out = {}
        for s in symbols:
            out[str(s)] = {
                "first_tick_day": "2025-01-01",
                "last_tick_day": "2025-01-02",
                "tick_days": 2,
                "first_l5_day": "2025-01-01",
                "last_l5_day": "2025-01-02",
                "l5_days": 2,
            }
        return out

    monkeypatch.setattr(qq, "query_contract_coverage", fake_cov)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("active_ranges_manifest") is None
    assert body.get("questdb", {}).get("ok") is True
    assert body["contracts"][0]["questdb_coverage"]["first_tick_day"] == "2025-01-01"
    assert body["contracts"][0]["questdb_coverage"]["tick_days"] == 2
    assert body["contracts"][0]["db_status"] == "ok"


def test_api_contracts_includes_local_lake_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Provide a fake catalog that does NOT include the locally-downloaded symbol.
    import ghtrader.tqsdk_catalog as cat

    def fake_catalog(**kwargs):
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None}],
        }

    monkeypatch.setattr(cat, "get_contract_catalog", fake_catalog)

    # Avoid real QuestDB access in tests.
    import ghtrader.questdb_queries as qq

    monkeypatch.setattr(qq, "query_contract_coverage", lambda **kwargs: {})

    # Local symbol present on disk but not in the catalog.
    data_dir = tmp_path / "data"
    (data_dir / "lake_v2" / "ticks" / "symbol=SHFE.cu1601" / "date=2016-01-04").mkdir(parents=True, exist_ok=True)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    syms = [x.get("symbol") for x in body.get("contracts") or []]
    assert "SHFE.cu1601" in syms

    row = next(x for x in body["contracts"] if x.get("symbol") == "SHFE.cu1601")
    assert row.get("catalog_source") == "local_lake"


def test_api_contracts_enqueue_sync_and_update_start_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Avoid spawning subprocesses during unit tests.
    jm = mod.app.state.job_manager
    monkeypatch.setattr(jm, "start_job", jm.enqueue_job)

    r1 = client.post("/api/contracts/enqueue-sync-questdb", json={"symbols": ["SHFE.cu2001"]})
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["ok"] is True
    assert len(body1.get("started") or []) == 1

    r2 = client.post("/api/contracts/enqueue-update", json={"exchange": "SHFE", "var": "cu", "symbols": ["SHFE.cu2001"]})
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["ok"] is True
    assert len(body2.get("enqueued") or []) == 1

    jobs = client.get("/api/jobs").json()["jobs"]
    cmd_all = [" ".join(j.get("command") or []) for j in jobs]
    assert any("db serve-sync" in c or "db\" \"serve-sync" in c for c in cmd_all)
    assert any(" update " in f" {c} " for c in cmd_all)

