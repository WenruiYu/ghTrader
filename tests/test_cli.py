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

    # Ensure snapshot build does NOT call local/parquet status computation.
    import ghtrader.data.contract_status as cs

    monkeypatch.setattr(cs, "compute_contract_statuses", lambda **kwargs: (_ for _ in ()).throw(AssertionError("compute_contract_statuses called")))

    # Ensure snapshot build does NOT attempt holiday downloads (cache-only trading-day logic).
    import ghtrader.data.trading_calendar as tc

    monkeypatch.setattr(tc, "_fetch_holidays_raw", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("holiday download attempted")))

    # Ensure snapshot build does NOT attempt QuestDB full scans by default.
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "bootstrap_symbol_day_index_from_ticks", lambda **kwargs: (_ for _ in ()).throw(AssertionError("index bootstrap called")))

    # Keep unit test offline/fast by forcing index access to fail fast (should fall back to last-only coverage).
    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("questdb unavailable")))
    monkeypatch.setattr(qix, "query_contract_coverage_from_index", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("questdb unavailable")))
    monkeypatch.setattr(qix, "list_symbols_from_index", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("questdb unavailable")))

    # Avoid probe cache reads.
    import ghtrader.tq.l5_probe as lp

    monkeypatch.setattr(lp, "load_probe_result", lambda **kwargs: None)

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
    row = snap["contracts"][0]
    assert row["questdb_coverage"]["last_tick_day"] == "2026-01-15"


# ---------------------------------------------------------------------------
# infer_contract_date_range tests
# ---------------------------------------------------------------------------


def test_infer_contract_date_range_shfe_cu():
    """Test date inference for SHFE copper contracts."""
    from datetime import date

    from ghtrader.data.contract_status import infer_contract_date_range

    # SHFE.cu2602 -> Feb 2026 expiry
    start, end = infer_contract_date_range("SHFE.cu2602")
    assert start == date(2025, 2, 1)
    assert end == date(2026, 2, 15)

    # SHFE.cu2412 -> Dec 2024 expiry
    start, end = infer_contract_date_range("SHFE.cu2412")
    assert start == date(2023, 12, 1)
    assert end == date(2024, 12, 15)

    # cu2501 (no exchange prefix)
    start, end = infer_contract_date_range("cu2501")
    assert start == date(2024, 1, 1)
    assert end == date(2025, 1, 15)


def test_infer_contract_date_range_invalid():
    """Test date inference returns None for invalid symbols."""
    from ghtrader.data.contract_status import infer_contract_date_range

    # Invalid formats
    assert infer_contract_date_range("") == (None, None)
    assert infer_contract_date_range("SHFE.cu") == (None, None)
    assert infer_contract_date_range("SHFE.cu26") == (None, None)
    assert infer_contract_date_range("SHFE.cu260") == (None, None)
    assert infer_contract_date_range("SHFE.cu26023") == (None, None)
    assert infer_contract_date_range("invalid") == (None, None)

    # Invalid month
    assert infer_contract_date_range("SHFE.cu2613") == (None, None)
    assert infer_contract_date_range("SHFE.cu2600") == (None, None)


# ---------------------------------------------------------------------------
# data completeness edge case tests
# ---------------------------------------------------------------------------


def test_compute_day_level_completeness_keeps_inferred_window_for_explicit_symbol(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    When explicit symbols are provided and catalog/QuestDB bounds are missing, date inference should still
    produce a window and the symbol should appear in the per-symbol contracts rows (not skipped).
    """
    from ghtrader.data.completeness import compute_day_level_completeness

    # Mock catalog to return empty
    import ghtrader.tq.catalog as cat

    monkeypatch.setattr(
        cat,
        "get_contract_catalog",
        lambda **kwargs: {"ok": True, "contracts": []},
    )

    # Mock QuestDB to return no coverage
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "query_contract_coverage_from_index", lambda **kwargs: {})
    monkeypatch.setattr(qix, "fetch_present_days_by_symbol", lambda **kwargs: {})
    monkeypatch.setattr(qix, "fetch_no_data_days_by_symbol", lambda **kwargs: {})

    # Mock trading calendar
    import ghtrader.data.trading_calendar as tc

    monkeypatch.setattr(tc, "get_trading_calendar", lambda **kwargs: [])
    monkeypatch.setattr(tc, "get_trading_days", lambda **kwargs: [])

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    data_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    # Test with a symbol that has no catalog data and no QuestDB data
    # The date inference should kick in for SHFE contract format
    result = compute_day_level_completeness(
        exchange="SHFE",
        variety="cu",
        symbols=["SHFE.cu2602"],
        contracts=None,
        start_override=None,
        end_override=None,
        refresh_catalog=False,
        allow_download_calendar=False,
        data_dir=data_dir,
        runs_dir=runs_dir,
    )

    assert result["ok"] is True
    assert result.get("symbols") == ["SHFE.cu2602"]
    rows = result.get("contracts") or []
    assert len(rows) == 1
    assert rows[0].get("symbol") == "SHFE.cu2602"


def test_compute_day_level_completeness_explicit_symbols_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Only explicitly provided symbols should be processed when symbols are given."""
    from ghtrader.data.completeness import compute_day_level_completeness

    # Mock catalog with multiple contracts
    import ghtrader.tq.catalog as cat

    monkeypatch.setattr(
        cat,
        "get_contract_catalog",
        lambda **kwargs: {
            "ok": True,
            "contracts": [
                {"symbol": "SHFE.cu2601", "expired": False, "expire_datetime": None, "open_date": "2025-01-01"},
                {"symbol": "SHFE.cu2602", "expired": False, "expire_datetime": None, "open_date": "2025-02-01"},
                {"symbol": "SHFE.cu2603", "expired": False, "expire_datetime": None, "open_date": "2025-03-01"},
            ],
        },
    )

    # Mock QuestDB
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "query_contract_coverage_from_index", lambda **kwargs: {})
    monkeypatch.setattr(qix, "fetch_present_days_by_symbol", lambda **kwargs: {})
    monkeypatch.setattr(qix, "fetch_no_data_days_by_symbol", lambda **kwargs: {})

    import ghtrader.data.trading_calendar as tc

    monkeypatch.setattr(tc, "get_trading_calendar", lambda **kwargs: [])
    monkeypatch.setattr(tc, "get_trading_days", lambda **kwargs: [])

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    data_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    # When explicit symbols are provided, only those should be processed
    result = compute_day_level_completeness(
        exchange="SHFE",
        variety="cu",
        symbols=["SHFE.cu2602"],  # Only one symbol
        contracts=None,
        start_override=None,
        end_override=None,
        refresh_catalog=False,
        allow_download_calendar=False,
        data_dir=data_dir,
        runs_dir=runs_dir,
    )

    # Should only have the one explicitly requested symbol
    assert result["symbols"] == ["SHFE.cu2602"]


def test_compute_day_level_completeness_marks_index_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    PRD: If QuestDB index has no rows for a symbol, verify must not treat all days as missing.

    Instead, the symbol should be included with index_missing=True and missing_days=None.
    """
    from datetime import date

    from ghtrader.data.completeness import compute_day_level_completeness

    # Provide catalog metadata so expected date window is deterministic (no date.today() clamping).
    import ghtrader.tq.catalog as cat

    monkeypatch.setattr(
        cat,
        "get_contract_catalog",
        lambda **kwargs: {
            "ok": True,
            "contracts": [
                {
                    "symbol": "SHFE.cu2501",
                    "expired": True,
                    "expire_datetime": "2025-01-10T00:00:00",
                    "open_date": "2025-01-01",
                }
            ],
        },
    )

    # QuestDB index returns no coverage for this symbol -> index_missing
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **kwargs: None)
    monkeypatch.setattr(qix, "query_contract_coverage_from_index", lambda **kwargs: {})
    monkeypatch.setattr(qix, "fetch_present_days_by_symbol", lambda **kwargs: {})
    monkeypatch.setattr(qix, "fetch_no_data_days_by_symbol", lambda **kwargs: {})

    # Trading days: provide a non-empty set so a naive impl would mark all as missing.
    import ghtrader.data.trading_calendar as tc

    monkeypatch.setattr(tc, "get_trading_calendar", lambda **kwargs: None)
    monkeypatch.setattr(tc, "get_trading_days", lambda *args, **kwargs: [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 10)])

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    data_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    result = compute_day_level_completeness(
        exchange="SHFE",
        variety="cu",
        symbols=["SHFE.cu2501"],
        contracts=None,
        start_override=None,
        end_override=None,
        refresh_catalog=False,
        allow_download_calendar=False,
        data_dir=data_dir,
        runs_dir=runs_dir,
    )

    assert result["ok"] is True
    assert result.get("symbols") == ["SHFE.cu2501"]

    contracts = result.get("contracts") or []
    assert len(contracts) == 1
    row = contracts[0]
    assert row.get("symbol") == "SHFE.cu2501"
    assert row.get("expected_days") == 3
    assert row.get("index_missing") is True
    assert row.get("present_days") is None
    assert row.get("missing_days") is None

    # Index-missing is represented on the per-symbol row (not as a skipped symbol).
    skipped = result.get("skipped_symbols") or []
    assert not any(s.get("symbol") == "SHFE.cu2501" and s.get("reason") == "index_missing" for s in skipped)

