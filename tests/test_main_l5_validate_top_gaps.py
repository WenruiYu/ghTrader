from __future__ import annotations

from datetime import datetime, timezone
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


def test_fetch_top_gap_days_with_schedule_hash(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validate as mod

    captured: dict[str, Any] = {}

    class FakeCursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

        def fetchall(self):
            return []

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

    out = mod.fetch_main_l5_validate_top_gap_days(
        cfg=object(),
        symbol="KQ.m@SHFE.cu",
        schedule_hash="h1",
        limit=2,
    )

    assert out == []
    assert "schedule_hash=%s" in str(captured["sql"])
    assert captured["params"] == ["KQ.m@SHFE.cu", "h1", 2]


def test_summary_upsert_placeholder_count_matches_params(monkeypatch) -> None:
    from ghtrader.questdb import main_l5_validate as mod

    captured: dict[str, Any] = {}

    class FakeCursor:
        def executemany(self, sql, params):
            captured["sql"] = sql
            captured["params"] = list(params)

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

    row = mod.MainL5ValidateSummaryRow(
        symbol="KQ.m@SHFE.cu",
        trading_day="2026-01-02",
        cadence_mode="event",
        expected_seconds=100,
        expected_seconds_strict=80,
        seconds_with_ticks=90,
        seconds_with_two_plus=30,
        two_plus_ratio=0.33,
        observed_segments=5,
        total_segments=10,
        missing_day=0,
        missing_segments=2,
        missing_seconds=10,
        missing_seconds_ratio=0.1,
        gap_bucket_2_5=1,
        gap_bucket_6_15=1,
        gap_bucket_16_30=0,
        gap_bucket_gt_30=0,
        gap_count_gt_30=0,
        missing_half_seconds=5,
        last_tick_ts=datetime(2026, 1, 2, 7, 0, tzinfo=timezone.utc),
        session_end_lag_s=2,
        max_gap_s=6,
        gap_threshold_s=2.0,
        schedule_hash="hash",
    )

    n = mod.upsert_main_l5_validate_summary_rows(cfg=object(), rows=[row])
    assert n == 1
    sql = str(captured.get("sql") or "")
    params = captured.get("params") or []
    assert len(params) == 1
    assert sql.count("%s") == len(params[0]) == 27
