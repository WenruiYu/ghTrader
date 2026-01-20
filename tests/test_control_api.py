from __future__ import annotations

import importlib
import json
import os
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _wait_for_job_done(client: TestClient, job_id: str, *, timeout_s: float = 10.0) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        data = client.get("/api/jobs").json()
        job = next(j for j in data["jobs"] if j["id"] == job_id)
        if job["status"] not in {"queued", "running"}:
            return job
        time.sleep(0.05)
    raise AssertionError("timeout waiting for job to finish")


def test_load_config_loads_accounts_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Ensure config.load_config() loads runs/control/accounts.env after .env.

    Note: load_config() normally skips auto-dotenv in pytest; we explicitly remove PYTEST_CURRENT_TEST
    and stub find_dotenv() to avoid reading any developer-local .env.
    """
    runs_dir = tmp_path / "runs"
    accounts_env = runs_dir / "control" / "accounts.env"
    accounts_env.parent.mkdir(parents=True, exist_ok=True)
    accounts_env.write_text(
        "\n".join(
            [
                'GHTRADER_TQ_ACCOUNT_PROFILES="tg_1"',
                'TQ_BROKER_ID_TG_1="T铜冠金源"',
                'TQ_ACCOUNT_ID_TG_1="12345678"',
                'TQ_ACCOUNT_PASSWORD_TG_1="pw"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GHTRADER_DISABLE_DOTENV", raising=False)
    monkeypatch.delenv("GHTRADER_TQ_ACCOUNT_PROFILES", raising=False)
    monkeypatch.delenv("TQ_BROKER_ID_TG_1", raising=False)
    monkeypatch.delenv("TQ_ACCOUNT_ID_TG_1", raising=False)
    monkeypatch.delenv("TQ_ACCOUNT_PASSWORD_TG_1", raising=False)

    mod = importlib.import_module("ghtrader.config")
    importlib.reload(mod)
    monkeypatch.setattr(mod, "find_dotenv", lambda: None)
    mod.load_config()

    assert os.environ.get("GHTRADER_TQ_ACCOUNT_PROFILES") == "tg_1"
    assert os.environ.get("TQ_BROKER_ID_TG_1") == "T铜冠金源"
    assert os.environ.get("TQ_ACCOUNT_ID_TG_1") == "12345678"
    assert os.environ.get("TQ_ACCOUNT_PASSWORD_TG_1") == "pw"

    from ghtrader.tq.runtime import list_account_profiles_from_env

    assert "tg_1" in list_account_profiles_from_env()


def test_control_api_accounts_crud_and_brokers_offline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    app = mod.app
    client = TestClient(app)

    # /api/brokers should use fallback in unit tests (no network)
    r = client.get("/api/brokers")
    assert r.status_code == 200
    b = r.json()
    assert b["ok"] is True
    assert b["source"] == "fallback"
    assert "T铜冠金源" in (b.get("brokers") or [])

    # Cached brokers should be preferred when present
    cache = tmp_path / "runs" / "control" / "cache" / "brokers.json"
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps({"brokers": ["X测试券商"]}, ensure_ascii=False), encoding="utf-8")
    r2 = client.get("/api/brokers")
    assert r2.status_code == 200
    b2 = r2.json()
    assert b2["ok"] is True
    assert b2["source"] == "cache"
    assert b2["brokers"] == ["X测试券商"]

    # Upsert a profile
    up = client.post(
        "/api/accounts/upsert",
        json={
            "profile": "tg_1",
            "broker_id": "T铜冠金源",
            "account_id": "12345678",
            "password": "pw",
        },
    )
    assert up.status_code == 200
    assert up.json()["ok"] is True
    assert up.json()["profile"] == "tg_1"

    from dotenv import dotenv_values

    accounts_env = tmp_path / "runs" / "control" / "accounts.env"
    vals = dotenv_values(accounts_env)
    assert vals.get("GHTRADER_TQ_ACCOUNT_PROFILES") == "tg_1"
    assert vals.get("TQ_BROKER_ID_TG_1") == "T铜冠金源"
    assert vals.get("TQ_ACCOUNT_ID_TG_1") == "12345678"
    assert vals.get("TQ_ACCOUNT_PASSWORD_TG_1") == "pw"

    # Verify cache should be returned if present
    verify_dir = tmp_path / "runs" / "control" / "cache" / "accounts"
    verify_dir.mkdir(parents=True, exist_ok=True)
    (verify_dir / "account=tg_1.json").write_text(json.dumps({"verified_at": "2026-01-01T00:00:00Z"}), encoding="utf-8")

    got = client.get("/api/accounts")
    assert got.status_code == 200
    data = got.json()
    assert data["ok"] is True
    rows = {r["profile"]: r for r in data.get("profiles") or []}
    assert "tg_1" in rows
    row = rows["tg_1"]
    assert row["configured"] is True
    assert row["broker_id"] == "T铜冠金源"
    assert row["account_id_masked"] == "12***78"
    assert row["verify"]["verified_at"] == "2026-01-01T00:00:00Z"

    # Delete should remove from accounts.env and delete verify cache
    dele = client.post("/api/accounts/delete", json={"profile": "tg_1"})
    assert dele.status_code == 200
    assert dele.json()["ok"] is True
    assert dele.json()["profile"] == "tg_1"
    assert not (verify_dir / "account=tg_1.json").exists()

    vals2 = dotenv_values(accounts_env)
    assert vals2.get("GHTRADER_TQ_ACCOUNT_PROFILES") in {None, ""}
    assert vals2.get("TQ_BROKER_ID_TG_1") in {None, ""}
    assert vals2.get("TQ_ACCOUNT_ID_TG_1") in {None, ""}
    assert vals2.get("TQ_ACCOUNT_PASSWORD_TG_1") in {None, ""}

    got2 = client.get("/api/accounts").json()
    rows2 = {r["profile"]: r for r in got2.get("profiles") or []}
    assert "tg_1" not in rows2


def test_control_api_job_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Isolate control-plane storage
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    app = mod.app

    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True

    create = client.post(
        "/api/jobs",
        json={
            "title": "echo",
            "argv": [sys.executable, "-c", "print('api-hello')"],
            "cwd": str(tmp_path),
        },
    )
    assert create.status_code == 200
    job_id = create.json()["job"]["id"]

    job = _wait_for_job_done(client, job_id, timeout_s=10.0)
    assert job["status"] in {"succeeded", "failed"}

    log_text = client.get(f"/api/jobs/{job_id}/log").text
    assert "api-hello" in log_text


