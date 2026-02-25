from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from ghtrader.config_service import enforce_no_legacy_env
from ghtrader.config_service.schema import coerce_value_for_key, key_is_ui_editable
from ghtrader.config_service.resolver import ConfigResolver


def test_config_resolver_precedence_config_over_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_MAIN_L5_TOTAL_WORKERS", "7")
    resolver = ConfigResolver(runs_dir=runs_dir)

    raw_1, src_1 = resolver.get_raw_with_source("GHTRADER_MAIN_L5_TOTAL_WORKERS", None)
    assert raw_1 == "7"
    assert src_1 == "env:GHTRADER_MAIN_L5_TOTAL_WORKERS"

    rev = resolver.set_values(
        values={"GHTRADER_MAIN_L5_TOTAL_WORKERS": 13},
        actor="test",
        reason="precedence",
    )
    assert rev.revision >= 2

    raw_2, src_2 = resolver.get_raw_with_source("GHTRADER_MAIN_L5_TOTAL_WORKERS", None)
    assert raw_2 == "13"
    assert src_2 == "config:GHTRADER_MAIN_L5_TOTAL_WORKERS"
    assert resolver.get_int("GHTRADER_MAIN_L5_TOTAL_WORKERS", 0) == 13


def test_config_revisions_and_rollback(tmp_path: Path):
    resolver = ConfigResolver(runs_dir=tmp_path / "runs")
    rev_a = resolver.set_values(
        values={"GHTRADER_MAIN_L5_SEGMENT_WORKERS": 3},
        actor="test",
        reason="set_a",
    )
    rev_b = resolver.set_values(
        values={"GHTRADER_MAIN_L5_SEGMENT_WORKERS": 5},
        actor="test",
        reason="set_b",
    )
    assert rev_b.revision > rev_a.revision
    assert resolver.get_int("GHTRADER_MAIN_L5_SEGMENT_WORKERS", 0) == 5

    rev_c = resolver.rollback(revision=rev_a.revision, actor="test", reason="rollback")
    assert rev_c.revision > rev_b.revision
    assert resolver.get_int("GHTRADER_MAIN_L5_SEGMENT_WORKERS", 0) == 3


def test_snapshot_hash_changes_with_value(tmp_path: Path):
    resolver = ConfigResolver(runs_dir=tmp_path / "runs")
    hash_0 = resolver.snapshot_hash
    resolver.set_values(values={"GHTRADER_PROGRESS_EVERY_N": 25}, actor="test", reason="h1")
    hash_1 = resolver.snapshot_hash
    resolver.set_values(values={"GHTRADER_PROGRESS_EVERY_N": 30}, actor="test", reason="h2")
    hash_2 = resolver.snapshot_hash
    assert hash_0 != hash_1
    assert hash_1 != hash_2


def test_legacy_env_gate_detects_managed_keys(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("GHTRADER_CONFIG_ALLOW_LEGACY_ENV", raising=False)
    monkeypatch.setenv("GHTRADER_MAIN_L5_TOTAL_WORKERS", "9")

    found = enforce_no_legacy_env(
        strict=False,
        allow_in_tests=False,
        argv=["ghtrader", "data", "main-l5"],
    )
    assert "GHTRADER_MAIN_L5_TOTAL_WORKERS" in found

    with pytest.raises(RuntimeError):
        enforce_no_legacy_env(
            strict=True,
            allow_in_tests=False,
            argv=["ghtrader", "data", "main-l5"],
        )

    # Migration command is explicitly exempted to allow one-shot import.
    assert (
        enforce_no_legacy_env(
            strict=True,
            allow_in_tests=False,
            argv=["ghtrader", "config", "migrate-env"],
        )
        == []
    )


def test_ui_editable_and_value_validation_boundaries():
    assert key_is_ui_editable("GHTRADER_PROGRESS_EVERY_N") is True
    assert key_is_ui_editable("GHTRADER_DASHBOARD_TOKEN") is False

    assert coerce_value_for_key("GHTRADER_PROGRESS_EVERY_N", "20") == 20
    with pytest.raises(ValueError):
        coerce_value_for_key("GHTRADER_L5_VALIDATE_STRICT_RATIO", "1.3")


def test_config_store_rejects_env_only_keys(tmp_path: Path):
    resolver = ConfigResolver(runs_dir=tmp_path / "runs")
    with pytest.raises(ValueError, match="config_key_not_writable:GHTRADER_DASHBOARD_TOKEN:env_only"):
        resolver.set_values(
            values={"GHTRADER_DASHBOARD_TOKEN": "secret"},
            actor="test",
            reason="should_fail",
        )


def test_config_store_rejects_unmanaged_keys(tmp_path: Path):
    resolver = ConfigResolver(runs_dir=tmp_path / "runs")
    with pytest.raises(ValueError, match="config_key_not_writable:NOT_MANAGED_KEY:unmanaged"):
        resolver.set_values(
            values={"NOT_MANAGED_KEY": "x"},
            actor="test",
            reason="should_fail",
        )


def test_config_store_auto_sanitizes_legacy_illegal_keys(tmp_path: Path):
    runs_dir = tmp_path / "runs"
    resolver = ConfigResolver(runs_dir=runs_dir)
    rev = resolver.set_values(
        values={"GHTRADER_PROGRESS_EVERY_N": 19},
        actor="test",
        reason="seed",
    )
    db_path = runs_dir / "control" / "config.db"

    with sqlite3.connect(str(db_path)) as con:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT id, snapshot_json FROM config_revisions WHERE id=?",
            (int(rev.revision),),
        ).fetchone()
        assert row is not None
        snap = json.loads(str(row["snapshot_json"]))
        snap["GHTRADER_DASHBOARD_TOKEN"] = "legacy_secret"
        snap["NOT_MANAGED_KEY"] = "legacy_value"
        con.execute(
            "UPDATE config_revisions SET snapshot_json=?, snapshot_hash=? WHERE id=?",
            (json.dumps(snap, ensure_ascii=False, sort_keys=True), "legacy", int(rev.revision)),
        )
        con.execute(
            "INSERT OR REPLACE INTO config_current(key, value, updated_at, revision_id) VALUES (?, ?, ?, ?)",
            ("GHTRADER_DASHBOARD_TOKEN", "legacy_secret", "legacy", int(rev.revision)),
        )
        con.execute(
            "INSERT OR REPLACE INTO config_current(key, value, updated_at, revision_id) VALUES (?, ?, ?, ?)",
            ("NOT_MANAGED_KEY", "legacy_value", "legacy", int(rev.revision)),
        )
        con.commit()

    resolver2 = ConfigResolver(runs_dir=runs_dir)
    snap2 = resolver2.snapshot()
    assert "GHTRADER_DASHBOARD_TOKEN" not in snap2
    assert "NOT_MANAGED_KEY" not in snap2
    assert resolver2.revision > int(rev.revision)

    latest = resolver2.list_revisions(limit=1)[0]
    assert latest.reason == "sanitize_illegal_keys"

