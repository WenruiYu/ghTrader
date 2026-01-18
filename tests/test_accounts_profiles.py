from __future__ import annotations

import pytest

from ghtrader.tq.runtime import (
    canonical_account_profile,
    is_trade_account_configured,
    list_account_profiles_from_env,
    load_trade_account_config_from_env,
)


def test_canonical_account_profile_sanitizes() -> None:
    assert canonical_account_profile("") == "default"
    assert canonical_account_profile("DEFAULT") == "default"
    assert canonical_account_profile("Main") == "main"
    assert canonical_account_profile("main-2") == "main_2"
    assert canonical_account_profile("  main__X  ") == "main_x"


def test_list_account_profiles_from_env_includes_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_TQ_ACCOUNT_PROFILES", "main, alt,Main-2,,")
    assert list_account_profiles_from_env() == ["default", "main", "alt", "main_2"]


def test_load_trade_account_config_from_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TQ_BROKER_ID", "BROKER0")
    monkeypatch.setenv("TQ_ACCOUNT_ID", "ACC0")
    monkeypatch.setenv("TQ_ACCOUNT_PASSWORD", "PWD0")
    cfg = load_trade_account_config_from_env(profile="default")
    assert cfg.broker_id == "BROKER0"
    assert cfg.account_id == "ACC0"
    assert cfg.password == "PWD0"
    assert is_trade_account_configured(profile="default") is True


def test_load_trade_account_config_from_env_profile_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_TQ_ACCOUNT_PROFILES", "main")
    monkeypatch.setenv("TQ_BROKER_ID_MAIN", "BROKER1")
    monkeypatch.setenv("TQ_ACCOUNT_ID_MAIN", "ACC1")
    monkeypatch.setenv("TQ_ACCOUNT_PASSWORD_MAIN", "PWD1")

    cfg = load_trade_account_config_from_env(profile="main")
    assert cfg.broker_id == "BROKER1"
    assert cfg.account_id == "ACC1"
    assert cfg.password == "PWD1"
    assert is_trade_account_configured(profile="main") is True
