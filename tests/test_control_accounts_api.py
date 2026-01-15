from __future__ import annotations

import importlib
import json
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _write_run(
    *,
    runs_dir: Path,
    run_id: str,
    account_profile: str,
    active: bool,
    mode: str = "live",
    monitor_only: bool = True,
    last_ts: str = "2026-01-01T00:00:00Z",
    balance: float = 100.0,
    equity: float = 110.0,
) -> None:
    root = runs_dir / "trading" / run_id
    root.mkdir(parents=True, exist_ok=True)
    (root / "run_config.json").write_text(
        json.dumps(
            {
                "created_at": "2026-01-01T00:00:00Z",
                "mode": mode,
                "monitor_only": bool(monitor_only),
                "account_profile": account_profile,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-01-01T00:00:00Z",
                "run_id": run_id,
                "last_snapshot": {
                    "schema_version": 2,
                    "ts": last_ts,
                    "account_meta": {"account_profile": account_profile, "broker_configured": True},
                    "account": {"balance": balance, "float_profit": 0.0, "equity": equity},
                    "positions": {},
                    "orders_alive": [],
                },
                "recent_events": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    # Control the active heuristic via mtime.
    sf = root / "state.json"
    now = time.time()
    ts = now if active else (now - 600.0)
    os.utime(sf, (ts, ts))


def test_api_trading_status_filters_by_account_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("GHTRADER_TQ_ACCOUNT_PROFILES", "alt")
    # Mark both profiles configured (no real secrets; tests never connect).
    monkeypatch.setenv("TQ_BROKER_ID", "B0")
    monkeypatch.setenv("TQ_ACCOUNT_ID", "A0")
    monkeypatch.setenv("TQ_ACCOUNT_PASSWORD", "P0")
    monkeypatch.setenv("TQ_BROKER_ID_ALT", "B1")
    monkeypatch.setenv("TQ_ACCOUNT_ID_ALT", "A1")
    monkeypatch.setenv("TQ_ACCOUNT_PASSWORD_ALT", "P1")

    runs_dir = tmp_path / "runs"
    _write_run(runs_dir=runs_dir, run_id="20260101_000001", account_profile="default", active=True)
    _write_run(runs_dir=runs_dir, run_id="20260101_000002", account_profile="alt", active=True)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/trading/status?account_profile=alt")
    assert r.status_code == 200
    data = r.json()
    assert data["active"] is True
    assert data["run_id"] == "20260101_000002"
    assert data["account_profile"] == "alt"

    r2 = client.get("/api/trading/status?account_profile=default")
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["active"] is True
    assert data2["run_id"] == "20260101_000001"
    assert data2["account_profile"] == "default"


def test_api_accounts_aggregates_profiles_runs_and_verify_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    runs_dir = tmp_path / "runs"
    accounts_env = runs_dir / "control" / "accounts.env"
    accounts_env.parent.mkdir(parents=True, exist_ok=True)
    accounts_env.write_text(
        "\n".join(
            [
                'GHTRADER_TQ_ACCOUNT_PROFILES="alt"',
                'TQ_BROKER_ID="B0"',
                'TQ_ACCOUNT_ID="A0"',
                'TQ_ACCOUNT_PASSWORD="P0"',
                'TQ_BROKER_ID_ALT="B1"',
                'TQ_ACCOUNT_ID_ALT="A1"',
                'TQ_ACCOUNT_PASSWORD_ALT="P1"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    _write_run(runs_dir=runs_dir, run_id="20260101_000010", account_profile="default", active=False, last_ts="2026-01-01T00:10:00Z")
    _write_run(runs_dir=runs_dir, run_id="20260101_000020", account_profile="alt", active=True, last_ts="2026-01-01T00:20:00Z")

    verify_dir = runs_dir / "control" / "cache" / "accounts"
    verify_dir.mkdir(parents=True, exist_ok=True)
    (verify_dir / "account=alt.json").write_text(json.dumps({"profile": "alt", "verified_at": "2026-01-02T00:00:00Z"}), encoding="utf-8")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/accounts")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    profs = {p["profile"]: p for p in data["profiles"]}
    assert "default" in profs
    assert "alt" in profs
    assert profs["default"]["configured"] is True
    assert profs["alt"]["configured"] is True
    assert profs["default"]["broker_id"] == "B0"
    assert profs["alt"]["broker_id"] == "B1"

    assert profs["alt"]["verify"] is not None
    assert profs["alt"]["active_run"]["run_id"] == "20260101_000020"
    assert profs["default"]["latest_run"]["run_id"] == "20260101_000010"

