from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_api_gateway_status_and_desired_and_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # No files yet.
    r0 = client.get("/api/gateway/status?account_profile=alt")
    assert r0.status_code == 200
    j0 = r0.json()
    assert j0["ok"] is True
    assert j0["account_profile"] == "alt"
    assert j0["exists"] is False

    # Set desired.
    r1 = client.post(
        "/api/gateway/desired",
        json={
            "account_profile": "alt",
            "desired": {
                "mode": "sim",
                "symbols": ["SHFE.cu2602"],
                "executor": "targetpos",
                "sim_account": "tqsim",
                "max_abs_position": 2,
                "max_order_size": 1,
                "max_ops_per_sec": 10,
                "enforce_trading_time": True,
            },
        },
    )
    assert r1.status_code == 200
    assert r1.json()["ok"] is True

    r2 = client.get("/api/gateway/status?account_profile=alt")
    assert r2.status_code == 200
    j2 = r2.json()
    assert j2["exists"] is True
    assert isinstance(j2["desired"], dict)

    # List should show the profile.
    r3 = client.get("/api/gateway/list")
    assert r3.status_code == 200
    j3 = r3.json()
    assert j3["ok"] is True
    profs = {p["account_profile"] for p in (j3.get("profiles") or [])}
    assert "alt" in profs

    # Append a command.
    r4 = client.post("/api/gateway/command", json={"account_profile": "alt", "type": "cancel_all", "params": {}})
    assert r4.status_code == 200
    out = r4.json()
    assert out["ok"] is True
    assert out["account_profile"] == "alt"
    assert out.get("command_id")

    cmd_path = tmp_path / "runs" / "gateway" / "account=alt" / "commands.jsonl"
    assert cmd_path.exists()
    line = cmd_path.read_text(encoding="utf-8").strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["type"] == "cancel_all"
    assert obj["account_profile"] == "alt"

