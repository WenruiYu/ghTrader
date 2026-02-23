from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from ghtrader.cli import main


def test_cli_config_set_and_effective(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("GHTRADER_CONFIG_ALLOW_LEGACY_ENV", "true")

    runner = CliRunner()
    set_res = runner.invoke(
        main,
        [
            "config",
            "set",
            "--key",
            "GHTRADER_PROGRESS_EVERY_N",
            "--value",
            "44",
            "--type",
            "int",
            "--json",
        ],
    )
    assert set_res.exit_code == 0, set_res.output
    set_body = json.loads(set_res.output)
    assert set_body["ok"] is True

    eff_res = runner.invoke(main, ["config", "effective", "--prefix", "GHTRADER_PROGRESS_", "--json"])
    assert eff_res.exit_code == 0, eff_res.output
    eff_body = json.loads(eff_res.output)
    assert eff_body["ok"] is True
    assert eff_body["values"]["GHTRADER_PROGRESS_EVERY_N"] == "44"


def test_cli_config_migrate_env_is_allowed_under_legacy_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("GHTRADER_MAIN_L5_TOTAL_WORKERS", "6")
    monkeypatch.delenv("GHTRADER_CONFIG_ALLOW_LEGACY_ENV", raising=False)

    runner = CliRunner()
    res = runner.invoke(main, ["config", "migrate-env", "--json"])
    assert res.exit_code == 0, res.output
    body = json.loads(res.output)
    assert body["ok"] is True
    assert "GHTRADER_MAIN_L5_TOTAL_WORKERS" in (body.get("changed_keys") or [])

