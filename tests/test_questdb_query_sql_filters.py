from __future__ import annotations

from datetime import date
from typing import Any

import ghtrader.questdb.queries as qq
from ghtrader.questdb.client import QuestDBQueryConfig


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = list(rows or [])
        self.executed: list[tuple[str, list[Any]]] = []
        self.description = []

    def execute(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> None:
        self.executed.append((str(sql), list(params or [])))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        return list(self.rows)[: int(size)]

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def _cfg() -> QuestDBQueryConfig:
    return QuestDBQueryConfig(host="localhost", pg_port=8812, pg_user="user", pg_password="pass", pg_dbname="qdb")


def test_fetch_ticks_for_day_uses_direct_trading_day_predicate(monkeypatch) -> None:
    cur = _FakeCursor(rows=[])
    monkeypatch.setattr(qq, "_connect", lambda cfg, connect_timeout_s=2: _FakeConn(cur))

    qq.fetch_ticks_for_day(
        cfg=_cfg(),
        symbol="SHFE.cu2501",
        trading_day=date(2025, 1, 2),
        table="ghtrader_ticks_main_l5_v2",
        columns=["datetime_ns"],
        dataset_version="v2",
        ticks_kind="main_l5",
    )

    sql = cur.executed[0][0]
    assert "trading_day = %s" in sql
    assert "cast(trading_day as string) = %s" not in sql


def test_list_trading_days_for_symbol_uses_direct_day_range_predicates(monkeypatch) -> None:
    cur = _FakeCursor(rows=[("2025-01-02",), ("2025-01-03",)])
    monkeypatch.setattr(qq, "_connect", lambda cfg, connect_timeout_s=2: _FakeConn(cur))
    monkeypatch.setattr(qq, "_get_redis_client", lambda: None)

    out = qq.list_trading_days_for_symbol(
        cfg=_cfg(),
        table="ghtrader_ticks_main_l5_v2",
        symbol="SHFE.cu2501",
        start_day=date(2025, 1, 1),
        end_day=date(2025, 1, 31),
        dataset_version="v2",
        ticks_kind="main_l5",
        l5_only=False,
    )

    assert out == [date(2025, 1, 2), date(2025, 1, 3)]
    sql = cur.executed[0][0]
    assert "SELECT DISTINCT trading_day" in sql
    assert "trading_day >= %s" in sql
    assert "trading_day <= %s" in sql
    assert "cast(trading_day as string) >=" not in sql
    assert "cast(trading_day as string) <=" not in sql


def test_query_symbol_recent_last_uses_direct_day_in_predicate(monkeypatch) -> None:
    monkeypatch.setenv("GHTRADER_QDB_USE_MAIN_L5_DAILY_AGG", "0")
    cur = _FakeCursor(rows=[("SHFE.cu2501", "2025-01-03", 123)])
    monkeypatch.setattr(qq, "_connect", lambda cfg, connect_timeout_s=2: _FakeConn(cur))

    out = qq.query_symbol_recent_last(
        cfg=_cfg(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["SHFE.cu2501"],
        dataset_version="v2",
        ticks_kind="main_l5",
        trading_days=["2025-01-02", "2025-01-03"],
    )

    sql = cur.executed[0][0]
    assert "trading_day IN (" in sql
    assert "cast(trading_day as string) IN (" not in sql
    assert out["SHFE.cu2501"]["last_day"] == "2025-01-03"


def test_query_symbol_recent_last_filters_blank_days_in_sql_params(monkeypatch) -> None:
    monkeypatch.setenv("GHTRADER_QDB_USE_MAIN_L5_DAILY_AGG", "0")
    cur = _FakeCursor(rows=[("SHFE.cu2501", "2025-01-03", 123)])
    monkeypatch.setattr(qq, "_connect", lambda cfg, connect_timeout_s=2: _FakeConn(cur))

    out = qq.query_symbol_recent_last(
        cfg=_cfg(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["SHFE.cu2501"],
        dataset_version="v2",
        ticks_kind="main_l5",
        trading_days=["", "2025-01-03", "  "],
    )

    sql, params = cur.executed[0]
    assert "trading_day IN (%s)" in sql
    assert params == ["SHFE.cu2501", "main_l5", "v2", "2025-01-03"]
    assert out["SHFE.cu2501"]["last_day"] == "2025-01-03"


def test_query_symbol_recent_last_short_circuits_when_days_empty_after_normalization(monkeypatch) -> None:
    monkeypatch.setenv("GHTRADER_QDB_USE_MAIN_L5_DAILY_AGG", "0")
    cur = _FakeCursor(rows=[("SHFE.cu2501", "2025-01-03", 123)])
    monkeypatch.setattr(qq, "_connect", lambda cfg, connect_timeout_s=2: _FakeConn(cur))

    out = qq.query_symbol_recent_last(
        cfg=_cfg(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["SHFE.cu2501"],
        dataset_version="v2",
        ticks_kind="main_l5",
        trading_days=["", "   "],
    )

    assert out == {}
    assert cur.executed == []
