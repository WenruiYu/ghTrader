from __future__ import annotations

import importlib
import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_quality_summary_and_detail_endpoints(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    # Force QuestDB to be unreachable so the endpoint exercises fallback behavior.
    monkeypatch.setenv("GHTRADER_QUESTDB_PG_PORT", "1")

    # Create a minimal contracts snapshot (QuestDB-independent).
    runs_dir = tmp_path / "runs"
    snap_dir = runs_dir / "control" / "cache" / "contracts_snapshot"
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_path = snap_dir / "contracts_exchange=SHFE_var=cu.json"
    snap_path.write_text(
        json.dumps(
            {
                "ok": True,
                "exchange": "SHFE",
                "var": "cu",
                "dataset_version": "v2",
                "completeness": {"summary": {"symbols_total": 2, "symbols_complete": 1}},
                "contracts": [
                    {
                        "symbol": "SHFE.cu2602",
                        "expired": False,
                        "status": "complete",
                        "last_tick_age_sec": 30,
                        "questdb_coverage": {"last_tick_ts": "2026-01-01T00:00:00+00:00", "last_tick_day": "2026-01-01"},
                        "completeness": {"missing_days": 0, "expected_days": 10, "present_days": 10},
                    },
                    {
                        "symbol": "SHFE.cu2603",
                        "expired": False,
                        "status": "missing",
                        "last_tick_age_sec": 300,
                        "questdb_coverage": {"last_tick_ts": "2026-01-01T00:00:00+00:00", "last_tick_day": "2026-01-01"},
                        "completeness": {"missing_days": 2, "expected_days": 10, "present_days": 8},
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    app = mod.app
    client = TestClient(app)

    r = client.get("/api/data/quality-summary?exchange=SHFE&var=cu&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["exchange"] == "SHFE"
    assert body["var"] == "cu"
    assert body["count"] == 2
    syms = {row["symbol"] for row in body.get("rows") or []}
    assert "SHFE.cu2602" in syms
    assert "SHFE.cu2603" in syms
    assert body["sources"]["contracts_snapshot"] == str(snap_path)

    r2 = client.get("/api/data/quality-detail/SHFE.cu2602")
    assert r2.status_code == 200
    d = r2.json()
    assert d["ok"] is True
    assert d["symbol"] == "SHFE.cu2602"
    # With QuestDB offline, detail arrays are empty but endpoint remains ok=true
    assert isinstance(d.get("field_quality"), list)
    assert isinstance(d.get("tick_gaps"), list)

