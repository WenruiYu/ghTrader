from __future__ import annotations

from typing import Any


def test_upsert_field_quality_rows(monkeypatch) -> None:
    from ghtrader.questdb import field_quality as mod

    row = mod.FieldQualityRow(
        symbol="KQ.m@SHFE.cu",
        trading_day="2024-01-01",
        ticks_kind="main_l5",
        dataset_version="v2",
        rows_total=100,
        bid_price1_null_rate=0.0,
        ask_price1_null_rate=0.0,
        last_price_null_rate=0.0,
        volume_null_rate=0.0,
        open_interest_null_rate=0.0,
        l5_fields_null_rate=0.0,
        price_jump_outliers=0,
        volume_spike_outliers=0,
        spread_anomaly_outliers=0,
        bid_ask_cross_violations=1,
        bid_ask_cross_rate=0.01,
        bid_order_violations=2,
        bid_order_rate=0.02,
        ask_order_violations=3,
        ask_order_rate=0.03,
        negative_volume_violations=0,
        negative_volume_rate=0.0,
        negative_price_violations=0,
        negative_price_rate=0.0,
    )

    captured: dict[str, Any] = {}

    class FakeCursor:
        def executemany(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

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

    n = mod.upsert_field_quality_rows(cfg=object(), rows=[row])

    assert n == 1
    assert "INSERT INTO" in captured["sql"]
    assert len(captured["params"]) == 1
    assert len(captured["params"][0]) == 26
