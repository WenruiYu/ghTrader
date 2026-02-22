from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _boot_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return TestClient(mod.app)


def test_quality_readiness_api_returns_layered_states(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _boot_client(tmp_path, monkeypatch)
    import ghtrader.data.main_l5_validation as validation
    import ghtrader.questdb.main_l5_validate as validate_q

    monkeypatch.setattr(
        validation,
        "read_latest_validation_report",
        lambda **kwargs: {
            "state": "warn",
            "overall_state": "warn",
            "engineering_state": "ok",
            "source_state": "warn",
            "policy_state": "ok",
            "reason_code": "source_missing_days_warn",
            "action_hint": "increase tolerance for known provider anomaly windows",
            "checked_days": 10,
            "source_missing_days_count": 1,
            "gap_count_gt_30s": 12,
            "ticks_outside_sessions_seconds_total": 0,
            "created_at": "2026-02-21T00:00:00+08:00",
            "schedule_hash": "abc",
        },
    )
    monkeypatch.setattr(validate_q, "fetch_main_l5_validate_overview", lambda **kwargs: {"days_total": 10, "state": "warn"})

    resp = client.get("/api/data/quality/readiness?exchange=SHFE&var=cu")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["overall_state"] == "warn"
    assert payload["engineering_state"] == "ok"
    assert payload["source_state"] == "warn"
    assert payload["policy_state"] == "ok"
    assert payload["reason_code"] == "source_missing_days_warn"
    assert payload["slo_metrics"]["provider_missing_day_rate"] == 0.1


def test_quality_anomalies_api_returns_report_issues(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _boot_client(tmp_path, monkeypatch)
    import ghtrader.data.main_l5_validation as validation

    monkeypatch.setattr(
        validation,
        "read_latest_validation_report",
        lambda **kwargs: {
            "issues": [
                {
                    "trading_day": "2026-01-01",
                    "missing_segments_total": 4,
                    "missing_half_seconds": 12,
                    "ticks_outside_sessions_seconds": 0,
                    "max_gap_s": 31,
                }
            ],
            "missing_days_sample": ["2026-01-02"],
            "source_missing_days_count": 1,
        },
    )
    resp = client.get("/api/data/quality/anomalies?exchange=SHFE&var=au")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["count"] == 1
    assert data["rows"][0]["max_gap_s"] == 31
    assert data["missing_days_sample"] == ["2026-01-02"]


def test_quality_profiles_api_returns_effective_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _boot_client(tmp_path, monkeypatch)
    import ghtrader.data.main_l5_validation as validation

    monkeypatch.setattr(
        validation,
        "resolve_validation_policy_preview",
        lambda **kwargs: {
            "gap_threshold_s": 8.0,
            "strict_ratio": 0.65,
            "policy_sources": {"gap_threshold_s": "env:GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S_CU"},
        },
    )

    resp = client.get("/api/data/quality/profiles?var=cu")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["profile"]["gap_threshold_s"] == 8.0
    assert "policy_sources" in body["profile"]
