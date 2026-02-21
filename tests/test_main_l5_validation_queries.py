from __future__ import annotations

from datetime import date


def test_get_last_validated_day_scopes_schedule_hash(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validation_queries as mod

    captured: dict[str, object] = {}

    class FakeCursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

        def fetchone(self):
            return ("2026-02-20",)

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

    monkeypatch.setattr(mod, "connect_pg", lambda *_args, **_kwargs: FakeConn())

    out = mod.get_last_validated_day(cfg=object(), symbol="KQ.m@SHFE.cu", schedule_hash="h123")
    assert out == date(2026, 2, 20)
    assert "schedule_hash=%s" in str(captured["sql"])
    assert captured["params"] == ["KQ.m@SHFE.cu", "h123"]
