from __future__ import annotations

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

