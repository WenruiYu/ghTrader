from __future__ import annotations

from datetime import date
from typing import Any


class _FakeCursor:
    def __init__(
        self,
        *,
        rows: list[tuple[Any, ...]] | None = None,
        fetchone_values: list[tuple[Any, ...]] | None = None,
        raise_on_agg_query: bool = False,
    ) -> None:
        self.rows = list(rows or [])
        self.fetchone_values = list(fetchone_values or [])
        self.raise_on_agg_query = bool(raise_on_agg_query)
        self.executed: list[tuple[str, list[Any] | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        s = str(sql)
        p = list(params) if params is not None else None
        self.executed.append((s, p))
        if self.raise_on_agg_query and "ghtrader_main_l5_daily_agg_v2" in s and "count(DISTINCT symbol)" in s:
            raise RuntimeError("table does not exist")

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        self.executemany_calls.append((str(sql), list(rows)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def fetchone(self) -> tuple[Any, ...]:
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return (0,)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur
        self.autocommit = False
        self.rollback_called = False

    def cursor(self) -> _FakeCursor:
        return self._cur

    def rollback(self) -> None:
        self.rollback_called = True

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_ensure_main_l5_daily_agg_table_emits_expected_ddl(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    cur = _FakeCursor()
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _FakeConn(cur))

    mod.ensure_main_l5_daily_agg_table(cfg=object())

    sql_text = "\n".join(s for s, _ in cur.executed)
    assert "CREATE TABLE IF NOT EXISTS ghtrader_main_l5_daily_agg_v2" in sql_text
    assert "DEDUP UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)" in sql_text
    assert "ALTER TABLE ghtrader_main_l5_daily_agg_v2 ADD COLUMN rows_total LONG" in sql_text


def test_rebuild_main_l5_daily_agg_upserts_grouped_rows(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    src_rows = [
        ("KQ.m@SHFE.cu", "2026-02-20", 120, 1000, 5000),
        ("KQ.m@SHFE.al", "2026-02-20", 80, 2000, 6000),
    ]
    read_cur = _FakeCursor(rows=src_rows)
    write_cur = _FakeCursor()
    conns = [_FakeConn(read_cur), _FakeConn(write_cur)]

    monkeypatch.setattr(mod, "ensure_main_l5_daily_agg_table", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: conns.pop(0))

    out = mod.rebuild_main_l5_daily_agg(
        cfg=object(),
        start_day=date(2026, 2, 1),
        end_day=date(2026, 2, 28),
        symbol="KQ.m@SHFE.cu",
    )

    assert out["ok"] is True
    assert out["source_groups"] == 2
    assert out["upserted_rows"] == 2
    assert out["distinct_symbols"] == 2
    assert out["invalid_rows"] == 0

    assert len(read_cur.executed) == 1
    read_sql, read_params = read_cur.executed[0]
    assert "FROM ghtrader_ticks_main_l5_v2" in read_sql
    assert "trading_day >= %s" in read_sql
    assert "trading_day <= %s" in read_sql
    assert "symbol=%s" in read_sql
    assert read_params == ["main_l5", "v2", "KQ.m@SHFE.cu", "2026-02-01", "2026-02-28"]

    assert len(write_cur.executemany_calls) == 1
    insert_sql, rows = write_cur.executemany_calls[0]
    assert "INSERT INTO ghtrader_main_l5_daily_agg_v2" in insert_sql
    assert len(rows) == 2
    assert rows[0][1] == "KQ.m@SHFE.cu"
    assert rows[0][2] == "2026-02-20"
    assert rows[0][5] == 120


def test_query_main_l5_symbol_count_prefers_agg_when_available(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    cur = _FakeCursor(fetchone_values=[(7,)])
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _FakeConn(cur))

    out = mod.query_main_l5_symbol_count_for_variety(cfg=object(), variety="cu")

    assert out == 7
    assert len(cur.executed) == 1
    assert "ghtrader_main_l5_daily_agg_v2" in cur.executed[0][0]


def test_query_main_l5_day_bounds_from_agg_returns_expected_shape(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    cur = _FakeCursor(
        rows=[
            ("KQ.m@SHFE.cu", "2026-02-01", "2026-02-05", 5, 1000, 9000),
        ]
    )
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _FakeConn(cur))

    out = mod.query_main_l5_day_bounds_from_agg(cfg=object(), symbols=["KQ.m@SHFE.cu"])

    assert "KQ.m@SHFE.cu" in out
    row = out["KQ.m@SHFE.cu"]
    assert row["first_day"] == "2026-02-01"
    assert row["last_day"] == "2026-02-05"
    assert row["n_days"] == 5
    assert row["first_ns"] == 1000
    assert row["last_ns"] == 9000
    assert isinstance(row["first_ts"], str) or row["first_ts"] is None
    assert isinstance(row["last_ts"], str) or row["last_ts"] is None


def test_query_main_l5_latest_from_agg_returns_expected_shape(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    cur = _FakeCursor(rows=[("KQ.m@SHFE.cu", "2026-02-05", 9000)])
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _FakeConn(cur))

    out = mod.query_main_l5_latest_from_agg(cfg=object(), symbols=["KQ.m@SHFE.cu"])

    assert out["KQ.m@SHFE.cu"]["last_day"] == "2026-02-05"
    assert out["KQ.m@SHFE.cu"]["last_ns"] == 9000
    assert isinstance(out["KQ.m@SHFE.cu"]["last_ts"], str) or out["KQ.m@SHFE.cu"]["last_ts"] is None


def test_query_main_l5_recent_last_from_agg_returns_expected_shape(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    cur = _FakeCursor(rows=[("KQ.m@SHFE.cu", "2026-02-05", 9000)])
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _FakeConn(cur))

    out = mod.query_main_l5_recent_last_from_agg(
        cfg=object(),
        symbols=["KQ.m@SHFE.cu"],
        trading_days=["2026-02-04", "2026-02-05"],
    )

    assert out["KQ.m@SHFE.cu"]["last_day"] == "2026-02-05"
    assert out["KQ.m@SHFE.cu"]["last_ns"] == 9000
    assert isinstance(out["KQ.m@SHFE.cu"]["last_ts"], str) or out["KQ.m@SHFE.cu"]["last_ts"] is None


def test_query_main_l5_symbol_count_falls_back_to_raw_table(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_daily_agg as mod

    cur = _FakeCursor(fetchone_values=[(5,)], raise_on_agg_query=True)
    conn = _FakeConn(cur)
    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: conn)

    out = mod.query_main_l5_symbol_count_for_variety(cfg=object(), variety="cu")

    assert out == 5
    assert conn.rollback_called is True
    assert len(cur.executed) == 2
    assert "ghtrader_main_l5_daily_agg_v2" in cur.executed[0][0]
    assert "ghtrader_ticks_main_l5_v2" in cur.executed[1][0]
