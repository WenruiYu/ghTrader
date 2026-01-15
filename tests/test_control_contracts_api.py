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

    # Initial load is cache-only (no network): without a cache, return a clear error promptly.
    r = client.get("/api/contracts?exchange=SHFE&var=cu&refresh=0")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["contracts"] == []
    assert "cache" in str(body.get("error") or "").lower()

    # Explicit refresh may hit TqSdk and should surface missing-credentials errors.
    r2 = client.get("/api/contracts?exchange=SHFE&var=cu&refresh=1")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["ok"] is False
    assert body2["contracts"] == []
    assert "tqsdk" in str(body2.get("error") or "").lower()


def test_api_contracts_returns_stale_catalog_when_cached_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If a forced catalog refresh fails but a cached catalog exists, /api/contracts should still
    return the cached contracts with catalog_ok=false for UI continuity.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    import ghtrader.tqsdk_catalog as cat

    def fake_catalog(**kwargs):
        return {
            "ok": False,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "error": "refresh_failed",
            "contracts": [{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None}],
        }

    monkeypatch.setattr(cat, "get_contract_catalog", fake_catalog)

    # Avoid real QuestDB access in tests.
    import ghtrader.questdb_queries as qq

    monkeypatch.setattr(qq, "query_contract_coverage", lambda **kwargs: {})

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu&refresh=1")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("catalog_ok") is False
    assert "refresh_failed" in str(body.get("catalog_error") or "")
    assert any(x.get("symbol") == "SHFE.cu2001" for x in body.get("contracts") or [])
    assert isinstance(body.get("timings_ms"), dict)


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
                "first_tick_ns": 1735689600000000000,
                "last_tick_ns": 1735776000000000000,
                "first_tick_ts": "2025-01-01T00:00:00+00:00",
                "last_tick_ts": "2025-01-02T00:00:00+00:00",
                "first_l5_day": "2025-01-01",
                "last_l5_day": "2025-01-02",
                "l5_days": 2,
                "first_l5_ns": 1735689600000000000,
                "last_l5_ns": 1735776000000000000,
                "first_l5_ts": "2025-01-01T00:00:00+00:00",
                "last_l5_ts": "2025-01-02T00:00:00+00:00",
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
    assert body["contracts"][0]["questdb_coverage"]["last_tick_ns"] == 1735776000000000000
    assert "T" in str(body["contracts"][0]["questdb_coverage"]["last_tick_ts"] or "")
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


def test_api_contracts_marks_active_stale_when_tick_lag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    A contract can be day-complete but still behind within the day.
    With a zero lag threshold, any non-zero lag should flip status to stale with stale_reason=tick_lag.
    """
    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("GHTRADER_CONTRACTS_LOCAL_TICK_LAG_STALE_MINUTES", "0")

    # Fake catalog (offline); mark as active.
    import ghtrader.tqsdk_catalog as cat

    def fake_catalog(**kwargs):
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [{"symbol": "SHFE.cu2604", "expired": False, "expire_datetime": None}],
        }

    monkeypatch.setattr(cat, "get_contract_catalog", fake_catalog)

    # Avoid real QuestDB access in tests.
    import ghtrader.questdb_queries as qq

    monkeypatch.setattr(qq, "query_contract_coverage", lambda **kwargs: {})

    # Create a local partition for today's trading day with a last tick ~1h ago.
    data_dir = tmp_path / "data"
    today = datetime.now(timezone.utc).date()
    trading_day = today
    while trading_day.weekday() >= 5:
        trading_day -= timedelta(days=1)
    last_ns = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1_000_000_000)
    import pandas as pd

    ddir = data_dir / "lake_v2" / "ticks" / "symbol=SHFE.cu2604" / f"date={trading_day.isoformat()}"
    ddir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"datetime": [last_ns - 1_000_000_000, last_ns]}).to_parquet(ddir / "part-test.parquet", index=False)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    body = client.get("/api/contracts?exchange=SHFE&var=cu").json()
    assert body["ok"] is True
    row = next(r for r in (body.get("contracts") or []) if r.get("symbol") == "SHFE.cu2604")
    assert row.get("local_last_tick_ts") is not None
    assert row.get("local_last_tick_age_sec") is not None
    assert row.get("tick_lag_stale_threshold_sec") == 0.0
    assert row.get("status") == "stale"
    assert row.get("stale_reason") == "tick_lag"

