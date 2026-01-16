from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_data_page_is_v2_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    sym_v1 = "SHFE.cu2501"
    sym_v2 = "SHFE.cu2502"

    # QuestDB-first coverage API uses the index; keep tests offline by faking it.
    import ghtrader.questdb_index as qix

    def fake_rows(**kwargs):
        assert str(kwargs.get("lake_version") or "") == "v2"
        return [{"symbol": sym_v2, "n_days": 1, "min_day": "2025-01-02", "max_day": "2025-01-02"}]

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "query_index_coverage_rows", fake_rows)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Default uses env (v2)
    r0 = client.get("/data")
    assert r0.status_code == 200
    assert "Data Hub" in r0.text
    assert "QuestDB" in r0.text
    # Coverage tables are lazy-loaded now; the page should still render and expose the Contracts workflow.
    assert "Contract Explorer" in r0.text

    # Query params are ignored; still v2-only.
    r1 = client.get("/data?lake_version=v1")
    assert r1.status_code == 200
    assert "Contract Explorer" in r1.text

    # Coverage API is v2-only: should include the v2 symbol and never include the v1 one.
    r2 = client.get("/api/data/coverage?kind=ticks&limit=500")
    assert r2.status_code == 200
    j = r2.json()
    assert j.get("ok") is True
    syms = [x.get("symbol") for x in (j.get("rows") or [])]
    assert sym_v2 in syms
    assert sym_v1 not in syms


def test_api_data_coverage_kinds_and_search(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    # Keep the test hermetic: features/labels coverage must fail cleanly when QuestDB is unreachable.
    monkeypatch.setenv("GHTRADER_QUESTDB_PG_PORT", "1")

    # QuestDB-first coverage API uses the index for ticks/main_l5; fake it so the test stays offline.
    import ghtrader.questdb_index as qix

    def fake_rows(**kwargs):
        assert str(kwargs.get("lake_version") or "") == "v2"
        tl = str(kwargs.get("ticks_lake") or "").strip()
        assert tl in {"raw", "main_l5"}
        q = str(kwargs.get("search") or "").lower().strip()
        if q and "does_not_exist" in q:
            return []
        if q and "cu2502" in q:
            return [{"symbol": "SHFE.cu2502", "n_days": 1, "min_day": "2025-01-02", "max_day": "2025-01-02"}]
        if q:
            return []
        return [{"symbol": "SHFE.cu2502", "n_days": 1, "min_day": "2025-01-02", "max_day": "2025-01-02"}]

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "query_index_coverage_rows", fake_rows)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    for kind in ["ticks", "main_l5"]:
        r = client.get(f"/api/data/coverage?kind={kind}&limit=50")
        assert r.status_code == 200
        j = r.json()
        assert j.get("ok") is True
        assert j.get("kind") == kind
        assert any(x.get("symbol") == "SHFE.cu2502" for x in (j.get("rows") or []))

    # features/labels are QuestDB-only and should fail cleanly when QuestDB is unavailable.
    for kind in ["features", "labels"]:
        r = client.get(f"/api/data/coverage?kind={kind}&limit=50")
        assert r.status_code == 200
        j = r.json()
        assert j.get("ok") is False
        assert j.get("kind") == kind
        assert j.get("rows") == []
        assert j.get("source") == "questdb_builds"

    # Search should filter results.
    r2 = client.get("/api/data/coverage?kind=ticks&limit=50&search=cu2502")
    assert r2.status_code == 200
    j2 = r2.json()
    assert any(x.get("symbol") == "SHFE.cu2502" for x in (j2.get("rows") or []))

    r3 = client.get("/api/data/coverage?kind=ticks&limit=50&search=does_not_exist")
    assert r3.status_code == 200
    j3 = r3.json()
    assert j3.get("rows") == []


def test_api_data_coverage_ticks_uses_questdb_index_no_fs_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perf gate: when QuestDB index path is available, /api/data/coverage(kind=ticks) must not scan filesystem.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    import ghtrader.questdb_index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(
        qix,
        "query_index_coverage_rows",
        lambda **kwargs: [{"symbol": "SHFE.cu2502", "n_days": 2, "min_day": "2025-01-01", "max_day": "2025-01-02"}],
    )

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/data/coverage?kind=ticks&limit=50")
    assert r.status_code == 200
    j = r.json()
    assert j.get("ok") is True
    assert j.get("source") == "questdb_index"
    assert any(x.get("symbol") == "SHFE.cu2502" for x in (j.get("rows") or []))


def test_api_data_enqueue_verify_starts_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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

    r = client.post("/api/data/enqueue-verify", json={"exchange": "SHFE", "var": "cu", "refresh_catalog": False})
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("deduped") is False
    assert captured["argv"] is not None
    assert "data" in (captured["argv"] or [])
    assert "verify" in (captured["argv"] or [])


def test_api_data_enqueue_fill_missing_init_mode_enqueues_download_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Init mode: when init_mode=true, /api/data/enqueue-fill-missing should enqueue one queued `download`
    job per contract symbol (deduped vs existing queued/running jobs).
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Keep test hermetic: fake the catalog and avoid network.
    import ghtrader.tqsdk_catalog as cat

    monkeypatch.setattr(
        cat,
        "get_contract_catalog",
        lambda **kwargs: {
            "ok": True,
            "exchange": "SHFE",
            "var": "ag",
            "source": "cache",
            "cached_at": "t",
            "cached_at_unix": 0.0,
            "contracts": [
                {"symbol": "SHFE.ag2501", "expired": True, "expire_datetime": None, "open_date": None, "catalog_source": "cache"},
                {"symbol": "SHFE.ag2502", "expired": True, "expire_datetime": None, "open_date": None, "catalog_source": "cache"},
            ],
        },
    )

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    jm = mod.app.state.job_manager
    captured: list[list[str]] = []
    orig_enqueue = jm.enqueue_job

    def fake_enqueue(spec):  # noqa: ANN001
        captured.append(list(spec.argv))
        return orig_enqueue(spec)

    monkeypatch.setattr(jm, "enqueue_job", fake_enqueue)

    r = client.post(
        "/api/data/enqueue-fill-missing",
        json={"exchange": "SHFE", "var": "ag", "init_mode": True, "refresh_catalog": False},
    )
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("mode") == "init"
    assert body.get("count") == 2
    assert len(captured) == 2
    assert all("download" in argv for argv in captured)
    for argv in captured:
        assert "--symbol" in argv
        assert "--start" in argv
        assert "--end" in argv


def test_data_hub_ui_has_workflow_stepper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PRD: Data Hub UI should display a workflow stepper card with steps 0-5."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Avoid QuestDB access in unit tests.
    import ghtrader.questdb_index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "query_index_coverage_rows", lambda **kwargs: [])

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/data")
    assert r.status_code == 200
    html = r.text

    # Check for workflow stepper elements
    assert "Workflow Steps" in html
    assert "workflow-stepper" in html
    assert "Refresh Snapshot" in html  # Step 0
    assert "Index Health" in html  # Step 1
    assert "Bootstrap Index" in html  # Step 1 action
    assert "Verify Completeness" in html  # Step 2
    assert "Fill Missing" in html  # Step 3
    assert "Update All" in html  # Step 4
    assert "Compute L5" in html  # Step 5

    # Check for unindexed status handling in JavaScript
    assert "unindexed" in html

