from __future__ import annotations

from datetime import date
import json

import pandas as pd
import pytest


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

    monkeypatch.setattr(cfg, "get_l5_start_date_with_source", lambda **kwargs: (trading_days[0], "GHTRADER_L5_START_DATE_CU"))
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


def test_main_schedule_fast_noop_uses_existing_rows(monkeypatch, tmp_path) -> None:
    import ghtrader.data.main_schedule as ms
    import ghtrader.questdb.client as qclient
    import ghtrader.config as cfg
    import ghtrader.data.trading_calendar as cal
    import ghtrader.tq.main_schedule as tqms

    trading_days = [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]
    existing_df = pd.DataFrame(
        {
            "trading_day": trading_days,
            "main_contract": ["SHFE.cu2501", "SHFE.cu2501", "SHFE.cu2502"],
            "segment_id": [0, 0, 1],
            "schedule_hash": ["hash-fast", "hash-fast", "hash-fast"],
        }
    )

    monkeypatch.setenv("GHTRADER_MAIN_SCHEDULE_FAST_NOOP", "1")
    monkeypatch.setattr(cfg, "get_l5_start_date_with_source", lambda **kwargs: (trading_days[0], "GHTRADER_L5_START_DATE_CU"))
    monkeypatch.setattr(cal, "latest_trading_day", lambda **kwargs: trading_days[-1])
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
            "schedule_hashes": {"hash-fast"},
        },
    )
    monkeypatch.setattr(ms, "fetch_schedule", lambda **kwargs: existing_df)
    monkeypatch.setattr(
        tqms,
        "extract_main_schedule_events",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("extract should not be called for fast noop")),
    )

    res = ms.build_tqsdk_main_schedule(var="cu", data_dir=tmp_path, runs_dir=tmp_path, exchange="SHFE")

    assert res.schedule_hash == "hash-fast"
    assert res.schedule["main_contract"].tolist() == ["SHFE.cu2501", "SHFE.cu2501", "SHFE.cu2502"]
    assert res.schedule["segment_id"].tolist() == [0, 0, 1]


def test_main_schedule_fast_noop_fallback_runs_extract(monkeypatch, tmp_path) -> None:
    import ghtrader.data.main_schedule as ms
    import ghtrader.questdb.client as qclient
    import ghtrader.config as cfg
    import ghtrader.data.trading_calendar as cal
    import ghtrader.tq.main_schedule as tqms

    events = [
        {"trading_day": date(2025, 1, 2), "underlying_symbol": "SHFE.cu2501"},
        {"trading_day": date(2025, 1, 4), "underlying_symbol": "SHFE.cu2502"},
    ]
    trading_days = [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]
    schedule = ms.build_daily_schedule_from_events(events=events, trading_days=trading_days, exchange="SHFE")
    schedule_hash = ms._stable_hash_df(schedule[["date", "main_contract", "segment_id"]])

    extracted = {"called": False}

    def _extract(**kwargs):
        extracted["called"] = True
        return events

    monkeypatch.setenv("GHTRADER_MAIN_SCHEDULE_FAST_NOOP", "1")
    monkeypatch.setattr(cfg, "get_l5_start_date_with_source", lambda **kwargs: (trading_days[0], "GHTRADER_L5_START_DATE_CU"))
    monkeypatch.setattr(cal, "latest_trading_day", lambda **kwargs: trading_days[-1])
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
    monkeypatch.setattr(
        ms,
        "fetch_schedule",
        lambda **kwargs: pd.DataFrame(
            {
                "trading_day": [date(2025, 1, 2), date(2025, 1, 4)],
                "main_contract": ["SHFE.cu2501", "SHFE.cu2502"],
                "segment_id": [0, 1],
                "schedule_hash": [schedule_hash, schedule_hash],
            }
        ),
    )
    monkeypatch.setattr(tqms, "extract_main_schedule_events", _extract)

    def _should_not_call(**kwargs):
        raise AssertionError("Unexpected rebuild call")

    monkeypatch.setattr(ms, "clear_main_schedule_rows", _should_not_call)
    monkeypatch.setattr(ms, "upsert_main_schedule_rows", _should_not_call)

    res = ms.build_tqsdk_main_schedule(var="cu", data_dir=tmp_path, runs_dir=tmp_path, exchange="SHFE")

    assert extracted["called"] is True
    assert res.schedule_hash == schedule_hash
    assert res.schedule["main_contract"].tolist() == schedule["main_contract"].tolist()


def test_main_schedule_health_check_detects_mismatch(monkeypatch) -> None:
    import ghtrader.data.main_schedule as ms

    expected = pd.DataFrame(
        {
            "date": [date(2025, 1, 2), date(2025, 1, 3)],
            "main_contract": ["SHFE.cu2501", "SHFE.cu2501"],
            "segment_id": [0, 0],
        }
    )
    persisted = pd.DataFrame(
        {
            "trading_day": [date(2025, 1, 2)],
            "main_contract": ["SHFE.cu2501"],
            "segment_id": [0],
            "schedule_hash": ["hash-x"],
        }
    )
    monkeypatch.setattr(ms, "fetch_schedule", lambda **kwargs: persisted)

    with pytest.raises(RuntimeError, match="rows mismatch"):
        ms._assert_main_schedule_health(
            cfg=object(),
            exchange="SHFE",
            variety="cu",
            expected_schedule=expected,
            expected_hash="hash-x",
        )


def test_main_schedule_writes_progress_state(monkeypatch, tmp_path) -> None:
    import ghtrader.data.main_schedule as ms
    import ghtrader.questdb.client as qclient
    import ghtrader.config as cfg
    import ghtrader.data.trading_calendar as cal

    trading_days = [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]
    existing_df = pd.DataFrame(
        {
            "trading_day": trading_days,
            "main_contract": ["SHFE.cu2501", "SHFE.cu2501", "SHFE.cu2502"],
            "segment_id": [0, 0, 1],
            "schedule_hash": ["hash-fast", "hash-fast", "hash-fast"],
        }
    )

    monkeypatch.setenv("GHTRADER_JOB_ID", "testmainscheduleprogress")
    monkeypatch.setenv("GHTRADER_MAIN_SCHEDULE_FAST_NOOP", "1")
    monkeypatch.setattr(cfg, "get_l5_start_date_with_source", lambda **kwargs: (trading_days[0], "GHTRADER_L5_START_DATE_CU"))
    monkeypatch.setattr(cal, "latest_trading_day", lambda **kwargs: trading_days[-1])
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
            "schedule_hashes": {"hash-fast"},
        },
    )
    monkeypatch.setattr(ms, "fetch_schedule", lambda **kwargs: existing_df)

    ms.build_tqsdk_main_schedule(var="cu", data_dir=tmp_path, runs_dir=tmp_path, exchange="SHFE")

    progress_path = tmp_path / "control" / "progress" / "job-testmainscheduleprogress.progress.json"
    assert progress_path.exists()
    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert float(payload.get("pct") or 0.0) == 1.0
    assert "complete" in str(payload.get("message") or "").lower()
