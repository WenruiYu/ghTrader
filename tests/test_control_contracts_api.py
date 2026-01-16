from __future__ import annotations

import importlib
import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_api_contracts_without_snapshot_returns_building(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    PRD: Contracts API is always snapshot-backed. When no snapshot exists, return
    'contracts_snapshot_building' and enqueue a background build job.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.delenv("TQSDK_USER", raising=False)
    monkeypatch.delenv("TQSDK_PASSWORD", raising=False)

    # Keep the test hermetic: simulate empty/unavailable QuestDB index discovery.
    import ghtrader.questdb_index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "list_symbols_from_index", lambda **kwargs: [])

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Without a snapshot, return snapshot_building error and enqueue a job (PRD).
    r = client.get("/api/contracts?exchange=SHFE&var=cu&refresh=0")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body.get("error") == "contracts_snapshot_building"
    assert body.get("snapshot_job") is not None
    assert body.get("snapshot_job", {}).get("enqueued") is True

    # Explicit refresh should also return snapshot_building (job already enqueued or new).
    r2 = client.get("/api/contracts?exchange=SHFE&var=cu&refresh=1")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["ok"] is False
    assert body2.get("error") == "contracts_snapshot_building"


def test_api_contracts_serves_fresh_snapshot_without_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contracts API serves from snapshot without triggering network requests (PRD: snapshot-backed).
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Prepare a fresh snapshot file.
    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [{"symbol": "SHFE.cu2001", "expired": True}],
        "questdb": {"ok": False},
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    # If any code attempts holiday download, fail the test.
    import ghtrader.trading_calendar as tc

    monkeypatch.setattr(tc, "_fetch_holidays_raw", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("holiday download attempted")))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu&refresh=0")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("cache_stale") is False  # Fresh snapshot


def test_api_contracts_mode_cache_returns_snapshot_without_spawning_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [{"symbol": "SHFE.cu2001"}],
        "questdb": {"ok": False},
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    import ghtrader.control.jobs as jobs

    monkeypatch.setattr(jobs.JobManager, "start_job", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("start_job should not be called for fresh snapshot")))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?mode=cache&exchange=SHFE&var=cu&refresh=0")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("cache_stale") is False
    assert len(body.get("contracts") or []) == 1


def test_api_contracts_mode_cache_enqueues_job_on_missing_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    import ghtrader.control.jobs as jobs
    from ghtrader.control.db import JobRecord

    captured = {"argv": None}

    def fake_start_job(self, spec):  # noqa: ANN001
        captured["argv"] = list(spec.argv)
        return JobRecord(
            id="snapjob1",
            created_at="t",
            updated_at="t",
            status="running",
            title=str(spec.title),
            command=list(spec.argv),
            cwd=str(spec.cwd),
            source="dashboard",
            pid=123,
        )

    monkeypatch.setattr(jobs.JobManager, "start_job", fake_start_job)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?mode=cache&exchange=SHFE&var=cu&refresh=0")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body.get("error") == "contracts_snapshot_building"
    assert body.get("snapshot_job", {}).get("enqueued") is True
    assert body.get("snapshot_job", {}).get("job_id") == "snapjob1"
    assert captured["argv"] is not None
    assert "contracts-snapshot-build" in " ".join(captured["argv"] or [])
    assert "--questdb-full" in (captured["argv"] or [])
    i = (captured["argv"] or []).index("--questdb-full")
    assert (captured["argv"] or [])[i + 1] == "0"


def test_api_contracts_mode_cache_refresh_bootstraps_index_when_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Simulate an empty QuestDB index so the refresh path requests a bootstrap.
    import ghtrader.questdb_index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "list_symbols_from_index", lambda **kwargs: [])

    import ghtrader.control.jobs as jobs
    from ghtrader.control.db import JobRecord

    captured = {"argv": None}

    def fake_start_job(self, spec):  # noqa: ANN001
        captured["argv"] = list(spec.argv)
        return JobRecord(
            id="snapjob_boot",
            created_at="t",
            updated_at="t",
            status="running",
            title=str(spec.title),
            command=list(spec.argv),
            cwd=str(spec.cwd),
            source="dashboard",
            pid=123,
        )

    monkeypatch.setattr(jobs.JobManager, "start_job", fake_start_job)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?mode=cache&exchange=SHFE&var=cu&refresh=1")
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is False
    assert body.get("error") == "contracts_snapshot_building"

    assert captured["argv"] is not None
    argv = captured["argv"] or []
    assert "--questdb-full" in argv
    j = argv.index("--questdb-full")
    assert argv[j + 1] == "1"


def test_api_contracts_mode_cache_returns_stale_snapshot_and_enqueues_rebuild(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [{"symbol": "SHFE.cu2001"}],
        "questdb": {"ok": False},
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time() - 3600.0),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    import ghtrader.control.jobs as jobs
    from ghtrader.control.db import JobRecord

    def fake_start_job(self, spec):  # noqa: ANN001
        return JobRecord(
            id="snapjob2",
            created_at="t",
            updated_at="t",
            status="running",
            title=str(spec.title),
            command=list(spec.argv),
            cwd=str(spec.cwd),
            source="dashboard",
            pid=456,
        )

    monkeypatch.setattr(jobs.JobManager, "start_job", fake_start_job)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?mode=cache&exchange=SHFE&var=cu&refresh=0")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("cache_stale") is True
    assert body.get("snapshot_job", {}).get("enqueued") is True
    assert body.get("snapshot_job", {}).get("job_id") == "snapjob2"


def test_api_contracts_returns_snapshot_with_stale_catalog_info(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    PRD: When a snapshot was built with a stale catalog (catalog_ok=false), the API should
    pass through that info so the UI can display a warning.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Prepare a snapshot that was built with a stale catalog.
    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [{"symbol": "SHFE.cu2001", "expired": True}],
        "questdb": {"ok": False},
        "catalog_ok": False,
        "catalog_error": "refresh_failed",
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("catalog_ok") is False
    assert "refresh_failed" in str(body.get("catalog_error") or "")
    assert any(x.get("symbol") == "SHFE.cu2001" for x in body.get("contracts") or [])


def test_api_contracts_snapshot_contains_questdb_coverage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PRD: Contracts snapshot includes QuestDB coverage data per contract."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Prepare a snapshot with QuestDB coverage data (built by snapshot builder).
    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [
            {
                "symbol": "SHFE.cu2001",
                "expired": True,
                "questdb_coverage": {
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
                },
                "db_status": "ok",
            }
        ],
        "questdb": {"ok": True},
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/contracts?exchange=SHFE&var=cu")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body.get("questdb", {}).get("ok") is True
    assert body["contracts"][0]["questdb_coverage"]["first_tick_day"] == "2025-01-01"
    assert body["contracts"][0]["questdb_coverage"]["tick_days"] == 2
    assert body["contracts"][0]["questdb_coverage"]["last_tick_ns"] == 1735776000000000000
    assert "T" in str(body["contracts"][0]["questdb_coverage"]["last_tick_ts"] or "")
    assert body["contracts"][0]["db_status"] == "ok"


def test_api_contracts_snapshot_includes_questdb_only_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    PRD QuestDB-first: Snapshot includes symbols from QuestDB index even if TqSdk
    catalog doesn't include them (catalog_source="questdb_index").
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Prepare a snapshot that includes a QuestDB-only symbol.
    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [
            {"symbol": "SHFE.cu2001", "expired": True},
            {"symbol": "SHFE.cu1601", "expired": True, "catalog_source": "questdb_index"},
        ],
        "questdb": {"ok": True},
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

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
    assert row.get("catalog_source") == "questdb_index"


def test_api_contracts_enqueue_update_starts_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that enqueue-update starts update jobs."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Avoid spawning subprocesses during unit tests.
    jm = mod.app.state.job_manager
    monkeypatch.setattr(jm, "start_job", jm.enqueue_job)

    r = client.post("/api/contracts/enqueue-update", json={"exchange": "SHFE", "var": "cu", "symbols": ["SHFE.cu2001"]})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert len(body.get("enqueued") or []) == 1

    jobs = client.get("/api/jobs").json()["jobs"]
    cmd_all = [" ".join(j.get("command") or []) for j in jobs]
    assert any(" update " in f" {c} " for c in cmd_all)


def test_api_contracts_snapshot_marks_stale_with_tick_lag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    PRD: A contract can be day-complete but still behind within the day.
    The snapshot builder marks such contracts as stale with stale_reason=tick_lag.
    """
    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Set up test dates
    trading_day = datetime.now(timezone.utc).date()
    while trading_day.weekday() >= 5:
        trading_day -= timedelta(days=1)
    last_tick_ts = datetime.now(timezone.utc) - timedelta(hours=1)
    last_ns = int(last_tick_ts.timestamp() * 1_000_000_000)

    # Prepare a snapshot with a stale contract (tick lag).
    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [
            {
                "symbol": "SHFE.cu2604",
                "expired": False,
                "status": "stale",
                "stale_reason": "tick_lag",
                "db_last_tick_ts": last_tick_ts.isoformat(),
                "db_last_tick_age_sec": 3600.0,
                "tick_lag_stale_threshold_sec": 0.0,
            }
        ],
        "questdb": {"ok": True},
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    body = client.get("/api/contracts?exchange=SHFE&var=cu").json()
    assert body["ok"] is True
    row = next(r for r in (body.get("contracts") or []) if r.get("symbol") == "SHFE.cu2604")
    assert row.get("db_last_tick_ts") is not None
    assert row.get("db_last_tick_age_sec") is not None
    assert row.get("tick_lag_stale_threshold_sec") == 0.0
    assert row.get("status") == "stale"
    assert row.get("stale_reason") == "tick_lag"


def test_api_contracts_snapshot_marks_unindexed_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    PRD: Contracts with no QuestDB index rows should be marked as 'unindexed' with index_missing=True.
    The snapshot builder sets this status during build.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Prepare a snapshot with an unindexed contract.
    runs_dir = tmp_path / "runs"
    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap = {
        "ok": True,
        "exchange": "SHFE",
        "var": "cu",
        "lake_version": "v2",
        "contracts": [
            {"symbol": "SHFE.cu2501", "expired": True, "status": "complete", "index_missing": False},
            {"symbol": "SHFE.cu2502", "expired": True, "status": "unindexed", "index_missing": True},
        ],
        "questdb": {"ok": True},
        "completeness": {
            "summary": {
                "symbols_unindexed": 1,
                "bootstrap_recommended": True,
            }
        },
        "snapshot_cached_at": "t",
        "snapshot_cached_at_unix": float(time.time()),
    }
    snap_path.write_text(json.dumps(snap), encoding="utf-8")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    body = client.get("/api/contracts?exchange=SHFE&var=cu").json()
    assert body["ok"] is True

    # cu2501 should have normal status (indexed)
    cu2501 = next(r for r in (body.get("contracts") or []) if r.get("symbol") == "SHFE.cu2501")
    assert cu2501.get("index_missing") is not True  # Should be False or None

    # cu2502 should have unindexed status
    cu2502 = next(r for r in (body.get("contracts") or []) if r.get("symbol") == "SHFE.cu2502")
    assert cu2502.get("status") == "unindexed"
    assert cu2502.get("index_missing") is True

    # Completeness summary should indicate bootstrap is recommended
    completeness = body.get("completeness", {})
    summary = completeness.get("summary", {})
    assert summary.get("symbols_unindexed") == 1
    assert summary.get("bootstrap_recommended") is True


def test_contracts_snapshot_build_includes_completeness_summary_when_index_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Regression: when the QuestDB symbol-day index is empty, contracts-snapshot-build must still include
    completeness.summary (index_healthy/bootstrapping signals) so the UI doesn't treat the index as healthy.
    """
    # Keep the builder hermetic: no external network.
    import ghtrader.trading_calendar as tc

    monkeypatch.setattr(tc, "get_holidays", lambda **kwargs: set())

    # Fake a cached catalog for the requested scope.
    import ghtrader.tqsdk_catalog as cat

    monkeypatch.setattr(
        cat,
        "get_contract_catalog",
        lambda **kwargs: {
            "ok": True,
            "exchange": "SHFE",
            "var": "ag",
            "cached_at": "t",
            "cached_at_unix": 0.0,
            "source": "cache",
            "contracts": [
                {"symbol": "SHFE.ag2501", "expired": True, "expire_datetime": None, "open_date": None, "catalog_source": "cache"},
                {"symbol": "SHFE.ag2502", "expired": True, "expire_datetime": None, "open_date": None, "catalog_source": "cache"},
            ],
        },
    )

    # Simulate "index empty": coverage query returns no rows, but QuestDB itself is reachable.
    import ghtrader.questdb_index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "list_symbols_from_index", lambda **kwargs: [])
    monkeypatch.setattr(qix, "query_contract_coverage_from_index", lambda **kwargs: {})

    import ghtrader.questdb_queries as qq

    monkeypatch.setattr(qq, "query_contract_last_coverage", lambda **kwargs: {})

    import ghtrader.cli as cli

    runs_dir = tmp_path / "runs"
    data_dir = tmp_path / "data"
    # The click command is wrapped by @click.pass_context; call the underlying function directly.
    cli.contracts_snapshot_build.callback.__wrapped__(
        None,  # click context not used by the implementation
        exchange="SHFE",
        variety="ag",
        refresh_catalog=0,
        questdb_full=0,
        data_dir=str(data_dir),
        runs_dir=str(runs_dir),
    )

    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=ag.json"
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    completeness = snap.get("completeness", {})
    summary = completeness.get("summary", {})
    assert summary.get("symbols_indexed") == 0
    assert summary.get("symbols_unindexed") == 2
    assert summary.get("bootstrap_recommended") is True

    # Contracts should be explicitly marked unindexed (not "missing") when index rows are absent.
    rows = snap.get("contracts") or []
    ag2501 = next(r for r in rows if r.get("symbol") == "SHFE.ag2501")
    assert ag2501.get("status") == "unindexed"
    assert (ag2501.get("completeness") or {}).get("index_missing") is True


def test_api_data_enqueue_index_bootstrap_starts_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that /api/data/enqueue-index-bootstrap starts a bootstrap job."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    jm = mod.app.state.job_manager
    captured: dict[str, list[str] | None] = {"argv": None}
    orig_enqueue = jm.enqueue_job

    def fake_start_job(spec):  # noqa: ANN001
        captured["argv"] = list(spec.argv)
        return orig_enqueue(spec)

    monkeypatch.setattr(jm, "start_job", fake_start_job)

    r = client.post("/api/data/enqueue-index-bootstrap", json={"exchange": "SHFE", "var": "cu"})
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("deduped") is False
    assert captured["argv"] is not None
    assert "data" in (captured["argv"] or [])
    assert "index-bootstrap" in (captured["argv"] or [])
    assert "--exchange" in (captured["argv"] or [])
    assert "--var" in (captured["argv"] or [])

