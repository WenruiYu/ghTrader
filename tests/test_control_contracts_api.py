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

    r = client.get("/api/contracts?exchange=SHFE&var=cu&lake_version=v1")
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
                "first_l5_day": "2025-01-01",
                "last_l5_day": "2025-01-02",
            }
        return out

    monkeypatch.setattr(qq, "query_contract_coverage", fake_cov)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu&lake_version=v1")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("active_ranges_manifest") is None
    assert body.get("questdb", {}).get("ok") is True
    assert body["contracts"][0]["questdb_coverage"]["first_tick_day"] == "2025-01-01"

