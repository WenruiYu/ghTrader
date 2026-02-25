from __future__ import annotations

from typing import Any


class _Cursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = list(rows)
        self.executed: list[tuple[str, list[Any] | None]] = []

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        self.executed.append((str(sql), list(params) if params is not None else None))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _Conn:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_query_symbol_day_bounds_uses_daily_agg_when_full(monkeypatch) -> None:
    import ghtrader.questdb.main_l5_daily_agg as agg_mod
    import ghtrader.questdb.queries as mod

    monkeypatch.setattr(mod, "_get_redis_client", lambda: None)

    def _fake_agg(**_kwargs):
        return {
            "KQ.m@SHFE.cu": {
                "first_day": "2026-02-01",
                "last_day": "2026-02-03",
                "n_days": 3,
                "first_ns": 100,
                "last_ns": 300,
                "first_ts": "x",
                "last_ts": "y",
            }
        }

    monkeypatch.setattr(agg_mod, "query_main_l5_day_bounds_from_agg", _fake_agg)
    monkeypatch.setattr(mod, "_connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("raw path should not run")))

    out = mod.query_symbol_day_bounds(
        cfg=object(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["KQ.m@SHFE.cu"],
        dataset_version="v2",
        ticks_kind="main_l5",
        l5_only=False,
    )

    assert out["KQ.m@SHFE.cu"]["n_days"] == 3
    assert out["KQ.m@SHFE.cu"]["first_day"] == "2026-02-01"


def test_query_symbol_day_bounds_falls_back_for_missing_symbols(monkeypatch) -> None:
    import ghtrader.questdb.main_l5_daily_agg as agg_mod
    import ghtrader.questdb.queries as mod

    monkeypatch.setattr(mod, "_get_redis_client", lambda: None)

    def _fake_agg(**_kwargs):
        return {
            "KQ.m@SHFE.cu": {
                "first_day": "2026-02-01",
                "last_day": "2026-02-01",
                "n_days": 1,
                "first_ns": 100,
                "last_ns": 200,
                "first_ts": "x",
                "last_ts": "y",
            }
        }

    monkeypatch.setattr(agg_mod, "query_main_l5_day_bounds_from_agg", _fake_agg)

    raw_cur = _Cursor(rows=[("KQ.m@SHFE.al", "2026-02-02", "2026-02-03", 2, 300, 600)])
    monkeypatch.setattr(mod, "_connect", lambda *_a, **_k: _Conn(raw_cur))

    out = mod.query_symbol_day_bounds(
        cfg=object(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["KQ.m@SHFE.cu", "KQ.m@SHFE.al"],
        dataset_version="v2",
        ticks_kind="main_l5",
        l5_only=False,
    )

    assert set(out.keys()) == {"KQ.m@SHFE.cu", "KQ.m@SHFE.al"}
    assert out["KQ.m@SHFE.cu"]["n_days"] == 1
    assert out["KQ.m@SHFE.al"]["n_days"] == 2
    assert len(raw_cur.executed) == 1
    assert raw_cur.executed[0][1] == ["KQ.m@SHFE.al", "main_l5", "v2"]


def test_query_symbol_latest_uses_daily_agg_when_full(monkeypatch) -> None:
    import ghtrader.questdb.main_l5_daily_agg as agg_mod
    import ghtrader.questdb.queries as mod

    def _fake_agg(**_kwargs):
        return {
            "KQ.m@SHFE.cu": {
                "last_day": "2026-02-05",
                "last_ns": 9000,
                "last_ts": "x",
            }
        }

    monkeypatch.setattr(agg_mod, "query_main_l5_latest_from_agg", _fake_agg)
    monkeypatch.setattr(mod, "_connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("raw path should not run")))

    out = mod.query_symbol_latest(
        cfg=object(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["KQ.m@SHFE.cu"],
        dataset_version="v2",
        ticks_kind="main_l5",
    )

    assert out["KQ.m@SHFE.cu"]["last_day"] == "2026-02-05"
    assert out["KQ.m@SHFE.cu"]["last_ns"] == 9000


def test_query_symbol_latest_falls_back_for_missing_symbols(monkeypatch) -> None:
    import ghtrader.questdb.main_l5_daily_agg as agg_mod
    import ghtrader.questdb.queries as mod

    def _fake_agg(**_kwargs):
        return {
            "KQ.m@SHFE.cu": {
                "last_day": "2026-02-05",
                "last_ns": 9000,
                "last_ts": "x",
            }
        }

    monkeypatch.setattr(agg_mod, "query_main_l5_latest_from_agg", _fake_agg)
    raw_cur = _Cursor(rows=[("KQ.m@SHFE.al", "2026-02-04", 8000)])
    monkeypatch.setattr(mod, "_connect", lambda *_a, **_k: _Conn(raw_cur))

    out = mod.query_symbol_latest(
        cfg=object(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["KQ.m@SHFE.cu", "KQ.m@SHFE.al"],
        dataset_version="v2",
        ticks_kind="main_l5",
    )

    assert set(out.keys()) == {"KQ.m@SHFE.cu", "KQ.m@SHFE.al"}
    assert out["KQ.m@SHFE.al"]["last_day"] == "2026-02-04"
    assert out["KQ.m@SHFE.al"]["last_ns"] == 8000
    assert len(raw_cur.executed) == 1
    assert raw_cur.executed[0][1] == ["KQ.m@SHFE.al", "main_l5", "v2"]


def test_query_symbol_recent_last_uses_daily_agg_when_full(monkeypatch) -> None:
    import ghtrader.questdb.main_l5_daily_agg as agg_mod
    import ghtrader.questdb.queries as mod

    def _fake_agg(**_kwargs):
        return {
            "KQ.m@SHFE.cu": {
                "last_day": "2026-02-05",
                "last_ns": 9000,
                "last_ts": "x",
            }
        }

    monkeypatch.setattr(agg_mod, "query_main_l5_recent_last_from_agg", _fake_agg)
    monkeypatch.setattr(mod, "_connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("raw path should not run")))

    out = mod.query_symbol_recent_last(
        cfg=object(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["KQ.m@SHFE.cu"],
        dataset_version="v2",
        ticks_kind="main_l5",
        trading_days=["2026-02-04", "2026-02-05"],
    )

    assert out["KQ.m@SHFE.cu"]["last_day"] == "2026-02-05"
    assert out["KQ.m@SHFE.cu"]["last_ns"] == 9000


def test_query_symbol_recent_last_falls_back_for_missing_symbols(monkeypatch) -> None:
    import ghtrader.questdb.main_l5_daily_agg as agg_mod
    import ghtrader.questdb.queries as mod

    def _fake_agg(**_kwargs):
        return {
            "KQ.m@SHFE.cu": {
                "last_day": "2026-02-05",
                "last_ns": 9000,
                "last_ts": "x",
            }
        }

    monkeypatch.setattr(agg_mod, "query_main_l5_recent_last_from_agg", _fake_agg)
    raw_cur = _Cursor(rows=[("KQ.m@SHFE.al", "2026-02-04", 8000)])
    monkeypatch.setattr(mod, "_connect", lambda *_a, **_k: _Conn(raw_cur))

    out = mod.query_symbol_recent_last(
        cfg=object(),
        table="ghtrader_ticks_main_l5_v2",
        symbols=["KQ.m@SHFE.cu", "KQ.m@SHFE.al"],
        dataset_version="v2",
        ticks_kind="main_l5",
        trading_days=["2026-02-03", "2026-02-04"],
    )

    assert set(out.keys()) == {"KQ.m@SHFE.cu", "KQ.m@SHFE.al"}
    assert out["KQ.m@SHFE.al"]["last_day"] == "2026-02-04"
    assert out["KQ.m@SHFE.al"]["last_ns"] == 8000
    assert len(raw_cur.executed) == 1
    assert raw_cur.executed[0][1] == ["KQ.m@SHFE.al", "main_l5", "v2", "2026-02-03", "2026-02-04"]
