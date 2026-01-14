from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest


def _touch_tick_day(data_dir: Path, symbol: str, day: date) -> None:
    d = data_dir / "lake_v2" / "ticks" / f"symbol={symbol}" / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "part-test.parquet").write_bytes(b"PAR1")


def test_audit_completeness_reports_missing_days(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ghtrader.audit import audit_completeness

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"

    sym = "SHFE.cu2001"
    _touch_tick_day(data_dir, sym, date(2020, 1, 6))
    _touch_tick_day(data_dir, sym, date(2020, 1, 7))
    _touch_tick_day(data_dir, sym, date(2020, 1, 10))
    p_no = data_dir / "lake_v2" / "ticks" / f"symbol={sym}" / "_no_data_dates.json"
    p_no.parent.mkdir(parents=True, exist_ok=True)
    p_no.write_text(json.dumps(["2020-01-08"], indent=2))

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

    def fake_cov(**kwargs: Any) -> dict[str, Any]:
        return {sym: {"first_tick_day": "2020-01-06", "last_tick_day": "2020-01-10", "tick_days": 5}}

    monkeypatch.setattr("ghtrader.questdb_queries.query_contract_coverage", fake_cov)

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
    assert "completeness_missing_trading_days" in codes

    miss = next(f for f in findings if f.code == "completeness_missing_trading_days")
    assert miss.extra and "missing_sample" in miss.extra
    assert "2020-01-09" in (miss.extra.get("missing_sample") or [])

