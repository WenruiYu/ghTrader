from __future__ import annotations

from typing import Any


def test_fetch_top_gap_days(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validate as mod

    rows = [
        ("2024-01-01", "event", 12, 0, 45, "hash1"),
        ("2024-01-02", "event", 9, 0, 39, "hash2"),
    ]
    captured: dict[str, Any] = {}

    class FakeCursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

        def fetchall(self):
            return rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_connect_pg(cfg, connect_timeout_s=2):
        _ = cfg, connect_timeout_s
        return FakeConn()

    monkeypatch.setattr(mod, "connect_pg", fake_connect_pg)

    out = mod.fetch_main_l5_validate_top_gap_days(cfg=object(), symbol="KQ.m@SHFE.cu", limit=2)

    assert len(out) == 2
    assert out[0]["trading_day"] == "2024-01-01"
    assert out[0]["max_gap_s"] == 45
    assert captured["params"] == ["KQ.m@SHFE.cu", 2]
