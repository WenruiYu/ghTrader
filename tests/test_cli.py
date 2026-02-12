from __future__ import annotations

from click.testing import CliRunner

from ghtrader.cli import main


def test_cli_help():
    runner = CliRunner()
    res = runner.invoke(main, ["--help"])
    assert res.exit_code == 0
    assert "ghTrader" in res.output


def test_cli_contracts_snapshot_build_removed() -> None:
    """
    Legacy contracts snapshot placeholder command is removed from active CLI surface.
    """
    runner = CliRunner()
    res = runner.invoke(main, ["contracts-snapshot-build"])
    assert res.exit_code != 0
    assert "No such command 'contracts-snapshot-build'" in res.output


