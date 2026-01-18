from __future__ import annotations

import importlib
import json
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _write_gateway(
    *,
    runs_dir: Path,
    profile: str,
    desired_mode: str,
    state: dict | None,
    state_mtime: float | None,
) -> None:
    root = runs_dir / "gateway" / f"account={profile}"
    root.mkdir(parents=True, exist_ok=True)
    (root / "desired.json").write_text(json.dumps({"desired": {"mode": desired_mode}}, indent=2), encoding="utf-8")
    if state is not None:
        sf = root / "state.json"
        sf.write_text(json.dumps(state, indent=2), encoding="utf-8")
        if state_mtime is not None:
            os.utime(sf, (state_mtime, state_mtime))


def _write_strategy(*, runs_dir: Path, profile: str, desired_mode: str, state_mtime: float | None) -> None:
    root = runs_dir / "strategy" / f"account={profile}"
    root.mkdir(parents=True, exist_ok=True)
    (root / "desired.json").write_text(json.dumps({"desired": {"mode": desired_mode}}, indent=2), encoding="utf-8")
    if state_mtime is not None:
        sf = root / "state.json"
        sf.write_text("{}", encoding="utf-8")
        os.utime(sf, (state_mtime, state_mtime))


def test_api_trading_console_status_filters_by_account_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    now = time.time()
    _write_gateway(
        runs_dir=runs_dir,
        profile="default",
        desired_mode="paper",
        state={"last_snapshot": {"account": {"balance": 100.0, "equity": 110.0}}},
        state_mtime=now,
    )
    _write_gateway(
        runs_dir=runs_dir,
        profile="alt",
        desired_mode="paper",
        state={"last_snapshot": {"account": {"balance": 200.0, "equity": 210.0}}},
        state_mtime=now,
    )

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/trading/console/status?account_profile=alt")
    assert r.status_code == 200
    data = r.json()
    assert data["account_profile"] == "alt"
    assert data["gateway"]["exists"] is True
    assert data["gateway"]["state"]["last_snapshot"]["account"]["balance"] == pytest.approx(200.0)

    r2 = client.get("/api/trading/console/status?account_profile=default")
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["account_profile"] == "default"
    assert data2["gateway"]["exists"] is True
    assert data2["gateway"]["state"]["last_snapshot"]["account"]["balance"] == pytest.approx(100.0)


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

    now = time.time()
    _write_gateway(runs_dir=runs_dir, profile="default", desired_mode="idle", state=None, state_mtime=None)
    _write_gateway(runs_dir=runs_dir, profile="alt", desired_mode="paper", state={"health": {"ok": True}}, state_mtime=now)
    _write_strategy(runs_dir=runs_dir, profile="default", desired_mode="idle", state_mtime=None)
    _write_strategy(runs_dir=runs_dir, profile="alt", desired_mode="run", state_mtime=(now - 600.0))

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
    assert profs["default"]["gateway"]["exists"] is True
    assert profs["default"]["gateway"]["status"] == "desired_idle"
    assert profs["default"]["strategy"]["exists"] is True
    assert profs["default"]["strategy"]["status"] == "desired_idle"
    assert profs["alt"]["gateway"]["exists"] is True
    assert profs["alt"]["gateway"]["status"] == "running"
    assert profs["alt"]["strategy"]["exists"] is True
    assert profs["alt"]["strategy"]["status"] == "degraded"

