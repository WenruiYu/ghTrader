from __future__ import annotations

from click.testing import CliRunner

import ghtrader.cli as cli


def test_strategy_run_acquires_strategy_lock_only(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def _fake_acquire_locks(lock_keys, **_kwargs):  # type: ignore[no-untyped-def]
        captured["lock_keys"] = list(lock_keys)

    def _fake_run_strategy_runner(cfg):  # type: ignore[no-untyped-def]
        captured["cfg"] = cfg

    import ghtrader.trading.strategy_runner as strategy_runner

    monkeypatch.setattr(cli, "_acquire_locks", _fake_acquire_locks)
    monkeypatch.setattr(strategy_runner, "run_strategy_runner", _fake_run_strategy_runner)

    runner = CliRunner()
    res = runner.invoke(
        cli.main,
        [
            "strategy",
            "run",
            "--account",
            "default",
            "--symbols",
            "SHFE.cu2602",
            "--artifacts-dir",
            str(tmp_path / "artifacts"),
            "--runs-dir",
            str(tmp_path / "runs"),
        ],
    )
    assert res.exit_code == 0, res.output
    assert captured.get("lock_keys") == ["trade:strategy:account=default"]


def test_gateway_run_keeps_account_lock(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_acquire_locks(lock_keys, **_kwargs):  # type: ignore[no-untyped-def]
        captured["lock_keys"] = list(lock_keys)

    def _fake_run_gateway(**_kwargs):  # type: ignore[no-untyped-def]
        return None

    import ghtrader.tq.gateway as gateway

    monkeypatch.setattr(cli, "_acquire_locks", _fake_acquire_locks)
    monkeypatch.setattr(gateway, "run_gateway", _fake_run_gateway)

    runner = CliRunner()
    res = runner.invoke(cli.main, ["gateway", "run", "--account", "default"])
    assert res.exit_code == 0, res.output
    assert captured.get("lock_keys") == ["trade:account=default"]
