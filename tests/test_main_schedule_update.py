from __future__ import annotations

from datetime import date


def test_main_schedule_noop_when_up_to_date(monkeypatch, tmp_path) -> None:
    import ghtrader.data.main_schedule as ms
    import ghtrader.questdb.client as qclient
    import ghtrader.tq.main_schedule as tqms
    import ghtrader.config as cfg
    import ghtrader.data.trading_calendar as cal

    events = [
        {"trading_day": date(2025, 1, 2), "underlying_symbol": "SHFE.cu2501"},
        {"trading_day": date(2025, 1, 4), "underlying_symbol": "SHFE.cu2502"},
    ]
    trading_days = [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]
    schedule = ms.build_daily_schedule_from_events(events=events, trading_days=trading_days, exchange="SHFE")
    schedule_hash = ms._stable_hash_df(schedule[["date", "main_contract", "segment_id"]])

    monkeypatch.setattr(cfg, "get_l5_start_date", lambda: trading_days[0])
    monkeypatch.setattr(cal, "latest_trading_day", lambda **kwargs: trading_days[-1])
    monkeypatch.setattr(tqms, "extract_main_schedule_events", lambda **kwargs: events)
    monkeypatch.setattr(ms, "get_trading_days", lambda **kwargs: trading_days)
    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(ms, "ensure_main_schedule_table", lambda **kwargs: None)
    monkeypatch.setattr(
        ms,
        "fetch_main_schedule_state",
        lambda **kwargs: {
            "first_day": trading_days[0],
            "last_day": trading_days[-1],
            "n_days": len(trading_days),
            "schedule_hashes": {schedule_hash},
        },
    )

    def _should_not_call(**kwargs):
        raise AssertionError("Unexpected rebuild call")

    monkeypatch.setattr(ms, "clear_main_schedule_rows", _should_not_call)
    monkeypatch.setattr(ms, "upsert_main_schedule_rows", _should_not_call)

    res = ms.build_tqsdk_main_schedule(var="cu", data_dir=tmp_path, runs_dir=tmp_path, exchange="SHFE")

    assert res.schedule_hash == schedule_hash
    assert res.schedule["main_contract"].tolist() == schedule["main_contract"].tolist()
