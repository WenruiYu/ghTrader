from __future__ import annotations

import importlib
import json
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
    import ghtrader.questdb.index as qix

    def fake_rows(**kwargs):
        assert str(kwargs.get("dataset_version") or "") == "v2"
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
    r1 = client.get("/data?dataset_version=v1")
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
    import ghtrader.questdb.index as qix

    def fake_rows(**kwargs):
        assert str(kwargs.get("dataset_version") or "") == "v2"
        tl = str(kwargs.get("ticks_kind") or "").strip()
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

    import ghtrader.questdb.index as qix

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


def test_api_data_health_enqueues_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    jm = mod.app.state.job_manager
    captured: dict[str, list[str] | None] = {"argv": None}
    orig_enqueue = jm.enqueue_job

    def fake_enqueue_job(spec):  # noqa: ANN001
        captured["argv"] = list(spec.argv)
        return orig_enqueue(spec)

    monkeypatch.setattr(jm, "enqueue_job", fake_enqueue_job)

    r = client.post(
        "/api/data/health",
        json={"exchange": "SHFE", "var": "cu", "thoroughness": "standard", "auto_repair": True, "dry_run": False},
    )
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("deduped") is False
    assert captured["argv"] is not None
    assert "data" in (captured["argv"] or [])
    assert "health" in (captured["argv"] or [])
    assert "--auto-repair" in (captured["argv"] or [])


def test_api_data_reports_lists_diagnose_and_repair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Health-first: /api/data/reports should list diagnose + repair artifacts by scope."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    runs_dir = tmp_path / "runs"
    diag_dir = runs_dir / "control" / "reports" / "data_diagnose"
    rep_dir = runs_dir / "control" / "reports" / "data_repair"
    diag_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    (diag_dir / "diagnose_exchange=SHFE_var=cu_abcd1234.json").write_text(
        json.dumps({"ok": True, "exchange": "SHFE", "var": "cu", "generated_at": "t", "summary": {}}), encoding="utf-8"
    )
    (rep_dir / "repair_deadbeef.plan.json").write_text(
        json.dumps({"run_id": "deadbeef", "exchange": "SHFE", "var": "cu", "dataset_version": "v2", "ticks_kind": "raw", "actions": []}),
        encoding="utf-8",
    )
    (rep_dir / "repair_deadbeef.result.json").write_text(
        json.dumps({"run_id": "deadbeef", "started_at": "t0", "finished_at": "t1", "ok": True, "actions": []}), encoding="utf-8"
    )

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r1 = client.get("/api/data/reports?kind=diagnose&exchange=SHFE&var=cu&limit=1")
    assert r1.status_code == 200
    j1 = r1.json()
    assert j1.get("ok") is True
    assert j1.get("kind") == "diagnose"
    assert j1.get("count") == 1
    assert (j1.get("reports") or [])[0].get("_path", "").endswith("diagnose_exchange=SHFE_var=cu_abcd1234.json")

    r2 = client.get("/api/data/reports?kind=repair&exchange=SHFE&var=cu&limit=1")
    assert r2.status_code == 200
    j2 = r2.json()
    assert j2.get("ok") is True
    assert j2.get("kind") == "repair"
    assert j2.get("count") == 1
    rep0 = (j2.get("reports") or [])[0]
    assert rep0.get("plan", {}).get("run_id") == "deadbeef"
    assert rep0.get("result", {}).get("ok") is True


def test_data_hub_ui_has_workflow_stepper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PRD: Data Hub UI should display a Health-first workflow stepper."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Avoid QuestDB access in unit tests.
    import ghtrader.questdb.index as qix

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
    assert "Health Check" in html  # Step 1
    assert "Health" in html  # Step 1 action
    assert "Repair" in html  # Step 1 action
    assert "Bootstrap Index" in html  # Optional action
    assert "Update All" in html  # Step 2
    assert "Compute L5" in html  # Step 3

    # Check for unindexed status handling in JavaScript
    assert "unindexed" in html

