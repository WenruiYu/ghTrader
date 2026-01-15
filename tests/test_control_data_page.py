from __future__ import annotations

import importlib
from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _touch_partition(root: Path, symbol: str, day: date) -> None:
    d = root / f"symbol={symbol}" / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    # scan_partitioned_store only needs the date directory, but keep a stub parquet for realism.
    (d / "part-test.parquet").write_bytes(b"PAR1")


def test_data_page_is_v2_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    data_dir = tmp_path / "data"
    sym_v1 = "SHFE.cu2501"
    sym_v2 = "SHFE.cu2502"

    # v1 path should be ignored by the dashboard (v2-only).
    _touch_partition(data_dir / "lake" / "ticks", sym_v1, date(2025, 1, 2))
    _touch_partition(data_dir / "lake_v2" / "ticks", sym_v2, date(2025, 1, 2))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Default uses env (v2)
    r0 = client.get("/data")
    assert r0.status_code == 200
    assert "Data Hub" in r0.text
    assert "lake_v2" in r0.text
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

    data_dir = tmp_path / "data"
    # Populate each kind with one symbol.
    _touch_partition(data_dir / "lake_v2" / "ticks", "SHFE.cu2502", date(2025, 1, 2))
    _touch_partition(data_dir / "lake_v2" / "main_l5" / "ticks", "SHFE.cu2502", date(2025, 1, 2))
    _touch_partition(data_dir / "features", "SHFE.cu2502", date(2025, 1, 2))
    _touch_partition(data_dir / "labels", "SHFE.cu2502", date(2025, 1, 2))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    for kind in ["ticks", "main_l5", "features", "labels"]:
        r = client.get(f"/api/data/coverage?kind={kind}&limit=50")
        assert r.status_code == 200
        j = r.json()
        assert j.get("ok") is True
        assert j.get("kind") == kind
        assert any(x.get("symbol") == "SHFE.cu2502" for x in (j.get("rows") or []))

    # Search should filter results.
    r2 = client.get("/api/data/coverage?kind=ticks&limit=50&search=cu2502")
    assert r2.status_code == 200
    j2 = r2.json()
    assert any(x.get("symbol") == "SHFE.cu2502" for x in (j2.get("rows") or []))

    r3 = client.get("/api/data/coverage?kind=ticks&limit=50&search=does_not_exist")
    assert r3.status_code == 200
    j3 = r3.json()
    assert j3.get("rows") == []

