from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest


def test_run_update_selects_active_and_recent_expired(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Unit test: update.run_update selects candidates (active + recently expired) and
    calls tq_ingest.download_historical_ticks with computed expected ranges.
    """
    from ghtrader.data import update as upd

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).date()
    recent_expire = (today - timedelta(days=1)).isoformat() + "T00:00:00Z"
    old_expire = (today - timedelta(days=120)).isoformat() + "T00:00:00Z"

    # Avoid any holiday-list network access in this unit test.
    monkeypatch.setattr(upd, "get_trading_calendar", lambda **kwargs: [])

    # Fake catalog: 1 active, 1 recently expired, 1 old expired (should be excluded)
    def fake_catalog(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [
                {"symbol": "SHFE.cu2602", "expired": False, "expire_datetime": None, "open_date": "2025-02-17"},
                {"symbol": "SHFE.cu2501", "expired": True, "expire_datetime": recent_expire, "open_date": "2025-01-02"},
                {"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": old_expire, "open_date": "2020-01-02"},
            ],
        }

    monkeypatch.setattr("ghtrader.tq.catalog.get_contract_catalog", fake_catalog)

    # Capture the candidate list passed into compute_contract_statuses
    seen_candidates: list[str] = []

    def fake_compute_contract_statuses(**kwargs: Any) -> dict[str, Any]:
        contracts = list(kwargs.get("contracts") or [])
        seen_candidates.extend([str(c.get("symbol") or "") for c in contracts])
        # Return "needs action" rows for the two expected candidates.
        return {
            "ok": True,
            "contracts": [
                {"symbol": "SHFE.cu2602", "status": "stale", "expected_first": "2025-02-17", "expected_last": today.isoformat()},
                {"symbol": "SHFE.cu2501", "status": "incomplete", "expected_first": "2025-01-02", "expected_last": today.isoformat()},
            ],
        }

    monkeypatch.setattr("ghtrader.data.contract_status.compute_contract_statuses", fake_compute_contract_statuses)

    calls: list[tuple[str, str, str]] = []

    def fake_download_historical_ticks(*, symbol: str, start_date: date, end_date: date, data_dir: Path, chunk_days: int, **_kw: Any) -> None:
        calls.append((symbol, start_date.isoformat(), end_date.isoformat()))

    monkeypatch.setattr("ghtrader.tq.ingest.download_historical_ticks", fake_download_historical_ticks)

    out_path, report = upd.run_update(exchange="SHFE", var="cu", data_dir=data_dir, runs_dir=runs_dir, recent_expired_trading_days=10)

    assert out_path.exists()
    assert report["ok"] is True
    assert set(seen_candidates) == {"SHFE.cu2602", "SHFE.cu2501"}
    assert set(c[0] for c in calls) == {"SHFE.cu2602", "SHFE.cu2501"}


def test_run_update_skips_when_no_action(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from ghtrader.data import update as upd

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    # Avoid any holiday-list network access in this unit test.
    monkeypatch.setattr(upd, "get_trading_calendar", lambda **kwargs: [])

    def fake_catalog(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [
                {"symbol": "SHFE.cu2602", "expired": False, "expire_datetime": None, "open_date": "2025-02-17"},
            ],
        }

    monkeypatch.setattr("ghtrader.tq.catalog.get_contract_catalog", fake_catalog)
    monkeypatch.setattr(upd, "ensure_index_tables", lambda **_kw: None)
    monkeypatch.setattr(upd, "query_contract_coverage_from_index", lambda **_kw: {})
    monkeypatch.setattr(upd, "make_questdb_query_config_from_env", lambda: None)

    def fake_compute_contract_statuses(**kwargs: Any) -> dict[str, Any]:
        return {"ok": True, "contracts": [{"symbol": "SHFE.cu2602", "status": "complete"}]}

    monkeypatch.setattr("ghtrader.data.contract_status.compute_contract_statuses", fake_compute_contract_statuses)

    calls: list[str] = []

    def fake_download_historical_ticks(*, symbol: str, **_kw: Any) -> None:
        calls.append(symbol)

    monkeypatch.setattr("ghtrader.tq.ingest.download_historical_ticks", fake_download_historical_ticks)

    out_path, report = upd.run_update(exchange="SHFE", var="cu", data_dir=data_dir, runs_dir=runs_dir, refresh_catalog=False)

    assert out_path.exists()
    assert report["ok"] is True
    assert report["needs_action"] == 0
    assert calls == []


def test_run_update_catalog_ttl_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from ghtrader.data import update as upd

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(upd, "get_trading_calendar", lambda **kwargs: [])

    calls: list[dict[str, Any]] = []

    def fake_catalog(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        return {
            "ok": True,
            "exchange": "SHFE",
            "var": "cu",
            "cached_at": "t",
            "source": "cache",
            "contracts": [],
        }

    monkeypatch.setattr("ghtrader.tq.catalog.get_contract_catalog", fake_catalog)
    monkeypatch.setattr(upd, "ensure_index_tables", lambda **_kw: None)
    monkeypatch.setattr(upd, "query_contract_coverage_from_index", lambda **_kw: {})
    monkeypatch.setattr(upd, "make_questdb_query_config_from_env", lambda: None)
    monkeypatch.setattr("ghtrader.data.contract_status.compute_contract_statuses", lambda **_kw: {"ok": True, "contracts": []})

    upd.run_update(
        exchange="SHFE",
        var="cu",
        data_dir=data_dir,
        runs_dir=runs_dir,
        refresh_catalog=True,
        catalog_ttl_seconds=3600,
    )

    assert calls
    assert calls[0].get("offline") is True
    assert calls[-1].get("refresh") is False

