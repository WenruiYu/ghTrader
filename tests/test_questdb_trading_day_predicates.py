from __future__ import annotations

from datetime import date
from typing import Any


class _CaptureCursor:
    def __init__(
        self,
        *,
        rows: list[tuple[Any, ...]] | None = None,
        row: tuple[Any, ...] | None = None,
        sink: dict[str, Any],
    ) -> None:
        self._rows = list(rows or [])
        self._row = row
        self._sink = sink

    def execute(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> None:
        self._sink["sql"] = str(sql)
        self._sink["params"] = list(params or [])

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def __enter__(self) -> "_CaptureCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _CaptureConn:
    def __init__(self, cursor: _CaptureCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _CaptureCursor:
        return self._cursor

    def __enter__(self) -> "_CaptureConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_main_schedule_fetch_schedule_uses_direct_trading_day_predicates(monkeypatch) -> None:
    from ghtrader.questdb import main_schedule as mod

    captured: dict[str, Any] = {}
    cur = _CaptureCursor(rows=[], row=None, sink=captured)
    monkeypatch.setattr(mod, "connect_pg", lambda cfg, connect_timeout_s=2: _CaptureConn(cur))

    out = mod.fetch_schedule(
        cfg=object(),
        exchange="SHFE",
        variety="cu",
        start_day=date(2025, 1, 1),
        end_day=date(2025, 1, 31),
    )

    assert out.empty
    sql = str(captured.get("sql") or "")
    assert "SELECT trading_day AS trading_day" in sql
    assert "trading_day >= %s" in sql
    assert "trading_day <= %s" in sql
    assert "cast(trading_day as string)" not in sql


def test_main_schedule_resolve_main_contract_uses_direct_le_predicate(monkeypatch) -> None:
    from ghtrader.questdb import main_schedule as mod

    captured: dict[str, Any] = {}
    cur = _CaptureCursor(rows=[], row=("SHFE.cu2501", 0, "hash-1"), sink=captured)
    monkeypatch.setattr(mod, "connect_pg", lambda cfg, connect_timeout_s=2: _CaptureConn(cur))

    out = mod.resolve_main_contract(
        cfg=object(),
        exchange="SHFE",
        variety="cu",
        trading_day=date(2025, 1, 3),
    )

    assert out == ("SHFE.cu2501", 0, "hash-1")
    sql = str(captured.get("sql") or "")
    assert "trading_day <= %s" in sql
    assert "cast(trading_day as string)" not in sql


def test_main_l5_validate_fetch_latest_summary_orders_by_direct_trading_day(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validate as mod

    captured: dict[str, Any] = {}
    cur = _CaptureCursor(rows=[], row=None, sink=captured)
    monkeypatch.setattr(mod, "connect_pg", lambda cfg, connect_timeout_s=2: _CaptureConn(cur))

    out = mod.fetch_latest_main_l5_validate_summary(cfg=object(), symbol="KQ.m@SHFE.cu", limit=5)

    assert out == []
    sql = str(captured.get("sql") or "")
    assert "ORDER BY trading_day DESC LIMIT %s" in sql
    assert "cast(trading_day as string)" not in sql


def test_main_l5_validate_gap_list_uses_direct_trading_day_predicates(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validate as mod

    captured: dict[str, Any] = {}
    cur = _CaptureCursor(rows=[], row=None, sink=captured)
    monkeypatch.setattr(mod, "connect_pg", lambda cfg, connect_timeout_s=2: _CaptureConn(cur))

    out = mod.list_main_l5_validate_gaps(
        cfg=object(),
        symbol="KQ.m@SHFE.cu",
        trading_day=date(2025, 1, 3),
        start_day=date(2025, 1, 1),
        end_day=date(2025, 1, 31),
        limit=10,
    )

    assert out == []
    sql = str(captured.get("sql") or "")
    assert "trading_day = %s" in sql
    assert "trading_day >= %s" in sql
    assert "trading_day <= %s" in sql
    assert "cast(trading_day as string)" not in sql


def test_field_quality_list_symbol_trading_days_uses_direct_predicates(monkeypatch) -> None:
    from ghtrader.data import field_quality as mod

    captured: dict[str, Any] = {}
    cur = _CaptureCursor(rows=[("2025-01-02",), ("2025-01-03",)], row=None, sink=captured)
    monkeypatch.setattr("ghtrader.questdb.client.connect_pg", lambda cfg, connect_timeout_s=2: _CaptureConn(cur))

    out = mod.list_symbol_trading_days(
        cfg=object(),
        symbol="KQ.m@SHFE.cu",
        start_day=date(2025, 1, 1),
        end_day=date(2025, 1, 31),
    )

    assert out == [date(2025, 1, 2), date(2025, 1, 3)]
    sql = str(captured.get("sql") or "")
    assert "SELECT DISTINCT trading_day" in sql
    assert "trading_day >= %s" in sql
    assert "trading_day <= %s" in sql
    assert "cast(trading_day as string)" not in sql


def test_gap_detection_orders_by_trading_day_without_cast(monkeypatch) -> None:
    from ghtrader.data import gap_detection as mod

    captured: dict[str, Any] = {}
    rows = [
        ("2025-01-03", 10, 1, 2, 50, 1, 0, 50, None),
    ]
    cur = _CaptureCursor(rows=rows, row=None, sink=captured)
    monkeypatch.setattr(mod, "connect_pg", lambda cfg, connect_timeout_s=2: _CaptureConn(cur))

    out = mod.list_tick_gap_ledger(symbol="KQ.m@SHFE.cu", cfg=object(), limit=5)

    assert len(out) == 1
    sql = str(captured.get("sql") or "")
    assert "ORDER BY trading_day DESC" in sql
    assert "cast(trading_day as string)" not in sql
