from __future__ import annotations

from click.testing import CliRunner

from ghtrader.cli import main


def test_cli_help():
    runner = CliRunner()
    res = runner.invoke(main, ["--help"])
    assert res.exit_code == 0
    assert "ghTrader" in res.output

