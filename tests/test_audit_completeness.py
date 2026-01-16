from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest


def test_audit_completeness_reports_missing_days(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ghtrader.audit import audit_completeness

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"

    sym = "SHFE.cu2001"

    def fake_catalog(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [{"symbol": sym, "expired": True, "expire_datetime": "2020-01-10T00:00:00Z", "open_date": "2020-01-06"}],
        }

    monkeypatch.setattr("ghtrader.tqsdk_catalog.get_contract_catalog", fake_catalog)

    # Expected trading days (patch calendar helper for hermetic unit test).
    monkeypatch.setattr(
        "ghtrader.trading_calendar.get_trading_days",
        lambda **_kwargs: [date(2020, 1, 6), date(2020, 1, 7), date(2020, 1, 8), date(2020, 1, 9), date(2020, 1, 10)],
    )

    # QuestDB index coverage (present days).
    def fake_cov(**kwargs: Any) -> dict[str, Any]:
        _ = kwargs
        return {sym: {"present_dates": {"2020-01-06", "2020-01-07", "2020-01-10"}}}

    monkeypatch.setattr("ghtrader.questdb_index.query_contract_coverage_from_index", fake_cov)
    monkeypatch.setattr("ghtrader.questdb_index.list_no_data_trading_days", lambda **_kwargs: [date(2020, 1, 8)])

    findings = audit_completeness(
        data_dir=data_dir,
        runs_dir=runs_dir,
        lake_version="v2",
        symbols=[sym],
        exchange="SHFE",
        var="cu",
        refresh_catalog=False,
    )

    codes = {f.code for f in findings}
    assert "missing_days" in codes

    miss = next(f for f in findings if f.code == "missing_days")
    assert miss.extra and "missing_days" in miss.extra
    assert "2020-01-09" in (miss.extra.get("missing_days") or [])

