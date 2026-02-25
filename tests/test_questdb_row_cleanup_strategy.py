from __future__ import annotations

from datetime import date

import pytest


def test_replace_table_delete_where_uses_fixed_replace_table_flow(monkeypatch) -> None:
    from ghtrader.questdb import row_cleanup as mod

    executed: list[str] = []
    ensured: list[str] = []

    class _Cursor:
        def __init__(self) -> None:
            self._row = (0,)

        def execute(self, sql, params=None) -> None:
            q = " ".join(str(sql).strip().split())
            executed.append(q)
            ql = q.lower()
            if ql.startswith("select count() from sample_table where symbol=%s"):
                self._row = (6,)
                return
            if ql.startswith("delete from sample_table"):
                raise AssertionError("DELETE must not be used in fixed replace_table strategy")

        def fetchone(self):
            return self._row

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Conn:
        def __init__(self) -> None:
            self.autocommit = False
            self._cur = _Cursor()

        def cursor(self):
            return self._cur

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _Conn())

    deleted = mod.replace_table_delete_where(
        cfg=object(),
        table="sample_table",
        columns=["symbol", "ts"],
        delete_where_sql="symbol=%s",
        delete_params=["KQ.m@SHFE.cu"],
        ensure_table=lambda t: ensured.append(str(t)),
        connect_timeout_s=2,
    )

    assert deleted == 6
    assert ensured and ensured[0] == "sample_table"
    assert any(t.startswith("sample_table_clean_") for t in ensured)
    assert any("INSERT INTO sample_table_clean_" in q for q in executed)
    assert any("WHERE NOT (symbol=%s)" in q for q in executed)
    assert any("DROP TABLE sample_table" in q for q in executed)
    assert any("RENAME TABLE sample_table_clean_" in q for q in executed)


def test_replace_table_delete_where_is_fail_fast(monkeypatch) -> None:
    from ghtrader.questdb import row_cleanup as mod

    class _Cursor:
        def __init__(self) -> None:
            self._row = (1,)

        def execute(self, sql, params=None) -> None:
            q = " ".join(str(sql).strip().split()).lower()
            if q.startswith("select count() from sample_table where symbol=%s"):
                self._row = (1,)
                return
            if q.startswith("insert into sample_table_clean_"):
                raise RuntimeError("rewrite failed")

        def fetchone(self):
            return self._row

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Conn:
        def __init__(self) -> None:
            self.autocommit = False
            self._cur = _Cursor()

        def cursor(self):
            return self._cur

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: _Conn())

    with pytest.raises(RuntimeError, match="rewrite failed"):
        mod.replace_table_delete_where(
            cfg=object(),
            table="sample_table",
            columns=["symbol", "ts"],
            delete_where_sql="symbol=%s",
            delete_params=["KQ.m@SHFE.cu"],
            ensure_table=lambda _t: None,
            connect_timeout_s=2,
        )


def test_main_schedule_clear_delegates_to_row_cleanup(monkeypatch) -> None:
    from ghtrader.questdb import main_schedule as mod
    from ghtrader.questdb import row_cleanup as cleanup

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(cleanup, "replace_table_delete_where", lambda **kwargs: calls.append(dict(kwargs)) or 5)

    deleted = mod.clear_main_schedule_rows(cfg=object(), exchange="SHFE", variety="cu")

    assert deleted == 5
    assert len(calls) == 1
    call = calls[0]
    assert call["table"] == "ghtrader_main_schedule_v2"
    assert call["delete_where_sql"] == "exchange=%s AND variety=%s"
    assert call["delete_params"] == ["SHFE", "cu"]


def test_main_schedule_trim_delegates_to_row_cleanup(monkeypatch) -> None:
    from ghtrader.questdb import main_schedule as mod
    from ghtrader.questdb import row_cleanup as cleanup

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(cleanup, "replace_table_delete_where", lambda **kwargs: calls.append(dict(kwargs)) or 3)

    deleted = mod.trim_main_schedule_before(
        cfg=object(),
        exchange="SHFE",
        variety="cu",
        start_day=date(2025, 1, 2),
    )

    assert deleted == 3
    assert len(calls) == 1
    call = calls[0]
    assert call["table"] == "ghtrader_main_schedule_v2"
    assert "trading_day < %s" in str(call["delete_where_sql"])
    assert call["delete_params"] == ["SHFE", "cu", "2025-01-02"]


def test_main_l5_validate_gap_clear_delegates_to_row_cleanup(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validate as mod
    from ghtrader.questdb import row_cleanup as cleanup

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(cleanup, "replace_table_delete_where", lambda **kwargs: calls.append(dict(kwargs)) or 4)

    deleted = mod.clear_main_l5_validate_gap_rows(cfg=object(), symbol="KQ.m@SHFE.cu")

    assert deleted == 4
    assert len(calls) == 1
    call = calls[0]
    assert call["table"] == "ghtrader_main_l5_validate_gaps_v2"
    assert call["delete_where_sql"] == "symbol=%s"
    assert call["delete_params"] == ["KQ.m@SHFE.cu"]
