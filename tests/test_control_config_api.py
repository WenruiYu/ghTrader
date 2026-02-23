from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _build_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return TestClient(mod.app)


def test_config_api_set_history_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _build_client(tmp_path, monkeypatch)

    base = client.get("/api/config/effective")
    assert base.status_code == 200
    base_body = base.json()
    assert base_body["ok"] is True
    base_revision = int(base_body["revision"])

    set_res = client.post(
        "/api/config/set",
        json={
            "reason": "api_test_set",
            "values": {
                "GHTRADER_MAIN_L5_TOTAL_WORKERS": "11",
                "GHTRADER_MAIN_L5_SEGMENT_WORKERS": "3",
            },
        },
    )
    assert set_res.status_code == 200
    set_body = set_res.json()
    assert set_body["ok"] is True
    assert set_body["actor"].startswith("api:")
    assert set_body["reason"] == "api_test_set"
    assert int(set_body["revision"]) > base_revision

    eff = client.get("/api/config/effective?prefix=GHTRADER_MAIN_L5_")
    assert eff.status_code == 200
    eff_body = eff.json()
    assert eff_body["values"]["GHTRADER_MAIN_L5_TOTAL_WORKERS"] == "11"
    assert eff_body["values"]["GHTRADER_MAIN_L5_SEGMENT_WORKERS"] == "3"

    hist = client.get("/api/config/history?limit=5")
    assert hist.status_code == 200
    hist_body = hist.json()
    assert hist_body["ok"] is True
    assert len(hist_body.get("items") or []) >= 1
    assert "revision" in hist_body["items"][0]

    rb = client.post(
        "/api/config/rollback",
        json={"revision": base_revision, "reason": "api_test_rollback"},
    )
    assert rb.status_code == 200
    rb_body = rb.json()
    assert rb_body["ok"] is True
    assert rb_body["actor"].startswith("api:")
    assert rb_body["reason"] == "api_test_rollback"
    assert int(rb_body["rollback_to"]) == base_revision

    eff2 = client.get("/api/config/effective?prefix=GHTRADER_MAIN_L5_")
    assert eff2.status_code == 200
    eff2_values = eff2.json()["values"]
    assert "GHTRADER_MAIN_L5_TOTAL_WORKERS" not in eff2_values
    assert "GHTRADER_MAIN_L5_SEGMENT_WORKERS" not in eff2_values


def test_config_api_migrate_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_PROGRESS_EVERY_N", "31")
    client = _build_client(tmp_path, monkeypatch)

    res = client.post("/api/config/migrate-env", json={"reason": "api_migrate_test"})
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert int(body["migrated_count"]) >= 1

    eff = client.get("/api/config/effective?prefix=GHTRADER_PROGRESS_EVERY_")
    assert eff.status_code == 200
    values = eff.json()["values"]
    assert values.get("GHTRADER_PROGRESS_EVERY_N") == "31"


def test_config_api_schema_and_effective_preview(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _build_client(tmp_path, monkeypatch)
    up = client.post(
        "/api/config/set",
        json={
            "reason": "schema_preview_seed",
            "values": {"GHTRADER_PROGRESS_EVERY_N": "33"},
        },
    )
    assert up.status_code == 200

    schema = client.get("/api/config/schema?prefix=GHTRADER_PROGRESS_")
    assert schema.status_code == 200
    rows = schema.json().get("items") or []
    by_key = {str(r.get("key")): r for r in rows}
    assert "GHTRADER_PROGRESS_EVERY_N" in by_key
    assert by_key["GHTRADER_PROGRESS_EVERY_N"]["value_type"] == "int"
    assert by_key["GHTRADER_PROGRESS_EVERY_N"]["editable"] is True

    preview = client.get("/api/config/effective-preview?prefix=GHTRADER_PROGRESS_")
    assert preview.status_code == 200
    pbody = preview.json()
    assert pbody["ok"] is True
    pitems = {str(x.get("key")): x for x in (pbody.get("items") or [])}
    assert pitems["GHTRADER_PROGRESS_EVERY_N"]["value"] == "33"
    assert pitems["GHTRADER_PROGRESS_EVERY_N"]["source"] == "config:GHTRADER_PROGRESS_EVERY_N"


def test_config_api_set_rejects_protected_or_invalid_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _build_client(tmp_path, monkeypatch)

    protected = client.post(
        "/api/config/set",
        json={
            "reason": "protected_key",
            "values": {"GHTRADER_DASHBOARD_TOKEN": "secret"},
        },
    )
    assert protected.status_code == 400
    pdetail = protected.json().get("detail") or {}
    issues = pdetail.get("issues") if isinstance(pdetail, dict) else None
    assert isinstance(issues, list) and issues
    assert any(str(i.get("key")) == "GHTRADER_DASHBOARD_TOKEN" for i in issues)

    invalid = client.post(
        "/api/config/set",
        json={
            "reason": "ratio_out_of_range",
            "values": {"GHTRADER_L5_VALIDATE_STRICT_RATIO": "1.2"},
        },
    )
    assert invalid.status_code == 400
    idetail = invalid.json().get("detail") or {}
    iissues = idetail.get("issues") if isinstance(idetail, dict) else None
    assert isinstance(iissues, list) and iissues
    assert any(str(i.get("key")) == "GHTRADER_L5_VALIDATE_STRICT_RATIO" for i in iissues)

