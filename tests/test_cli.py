from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from ghtrader.cli import main


def test_cli_help():
    runner = CliRunner()
    res = runner.invoke(main, ["--help"])
    assert res.exit_code == 0
    assert "ghTrader" in res.output


def test_cli_contracts_snapshot_build_default_is_last_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Default contracts snapshot build must not run full QuestDB bounds/day-count scans.
    """
    # Fake catalog so we don't require TqSdk or caches.
    import ghtrader.tq.catalog as cat

    monkeypatch.setattr(
        cat,
        "get_contract_catalog",
        lambda **kwargs: {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None, "open_date": None, "catalog_source": "cache"}],
        },
    )

    # Ensure the full (ticks-table) coverage query is NOT called.
    import ghtrader.questdb.queries as qq

    monkeypatch.setattr(qq, "query_contract_coverage", lambda **kwargs: (_ for _ in ()).throw(AssertionError("full coverage query called")))

    # Provide a fast last-only coverage result.
    def fake_last_cov(**kwargs):
        return {
            "SHFE.cu2001": {
                "first_tick_day": None,
                "last_tick_day": "2026-01-15",
                "tick_days": None,
                "first_tick_ns": None,
                "last_tick_ns": 1,
                "first_tick_ts": None,
                "last_tick_ts": "2026-01-15T00:00:00+00:00",
                "first_l5_day": None,
                "last_l5_day": None,
                "l5_days": None,
                "first_l5_ns": None,
                "last_l5_ns": None,
                "first_l5_ts": None,
                "last_l5_ts": None,
            }
        }

    monkeypatch.setattr(qq, "query_contract_last_coverage", fake_last_cov)

    runner = CliRunner()
    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    data_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    res = runner.invoke(
        main,
        [
            "contracts-snapshot-build",
            "--exchange",
            "SHFE",
            "--var",
            "cu",
            "--refresh-catalog",
            "0",
            "--questdb-full",
            "0",
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        ],
    )
    assert res.exit_code == 0, res.output

    snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / "contracts_exchange=SHFE_var=cu.json"
    assert snap_path.exists()
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    assert snap["ok"] is True
    assert snap["exchange"] == "SHFE"
    assert snap["var"] == "cu"
    assert snap.get("completeness", {}).get("mode") == "deferred"


