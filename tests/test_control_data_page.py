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


def test_data_page_lake_version_switching(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("GHTRADER_LAKE_VERSION", "v2")

    data_dir = tmp_path / "data"
    sym_v1 = "SHFE.cu2501"
    sym_v2 = "SHFE.cu2502"

    _touch_partition(data_dir / "lake" / "ticks", sym_v1, date(2025, 1, 2))
    _touch_partition(data_dir / "lake_v2" / "ticks", sym_v2, date(2025, 1, 2))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Default uses env (v2)
    r0 = client.get("/data")
    assert r0.status_code == 200
    assert "Contracts (TqSdk + local coverage)" in r0.text
    assert sym_v2 in r0.text
    assert sym_v1 not in r0.text

    # Explicit v1
    r1 = client.get("/data?lake_version=v1")
    assert r1.status_code == 200
    assert sym_v1 in r1.text
    assert sym_v2 not in r1.text

    # Explicit v2
    r2 = client.get("/data?lake_version=v2")
    assert r2.status_code == 200
    assert sym_v2 in r2.text
    assert sym_v1 not in r2.text

    # Both
    r3 = client.get("/data?lake_version=both")
    assert r3.status_code == 200
    assert sym_v1 in r3.text
    assert sym_v2 in r3.text

