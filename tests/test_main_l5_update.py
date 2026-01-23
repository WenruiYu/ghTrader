from __future__ import annotations

from datetime import date

import pandas as pd


def _schedule_df(schedule_hash: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trading_day": [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)],
            "main_contract": ["SHFE.cu2501", "SHFE.cu2501", "SHFE.cu2501"],
            "segment_id": [0, 0, 0],
            "schedule_hash": [schedule_hash, schedule_hash, schedule_hash],
        }
    )


def test_main_l5_update_backfills_missing_days(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-123"
    schedule = _schedule_df(schedule_hash)

    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(qqueries, "list_schedule_hashes_for_symbol", lambda **kwargs: {schedule_hash})
    monkeypatch.setattr(
        qqueries,
        "list_trading_days_for_symbol",
        lambda **kwargs: [date(2025, 1, 2)],
    )
    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)

    cleared = {"called": False}
    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", lambda **kwargs: cleared.__setitem__("called", True) or 0)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)

    calls: list[list[date]] = []

    def _fake_download(**kwargs):
        calls.append(sorted(kwargs["trading_days"]))
        return {"rows_total": len(kwargs["trading_days"])}

    monkeypatch.setattr(ingest, "download_main_l5_for_days", _fake_download)

    res = ml5.build_main_l5(
        var="cu",
        derived_symbol="KQ.m@SHFE.cu",
        exchange="SHFE",
        data_dir=str(tmp_path),
        update_mode=True,
    )

    assert cleared["called"] is False
    assert calls == [[date(2025, 1, 3), date(2025, 1, 4)]]
    assert res.days_total == 2


def test_main_l5_update_hash_mismatch_forces_rebuild(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-123"
    schedule = _schedule_df(schedule_hash)

    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(qqueries, "list_schedule_hashes_for_symbol", lambda **kwargs: {"old-hash"})
    monkeypatch.setattr(qqueries, "list_trading_days_for_symbol", lambda **kwargs: [])
    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)

    cleared = {"called": False}

    def _clear(**kwargs):
        cleared["called"] = True
        return 0

    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", _clear)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)

    calls: list[list[date]] = []

    def _fake_download(**kwargs):
        calls.append(sorted(kwargs["trading_days"]))
        return {"rows_total": len(kwargs["trading_days"])}

    monkeypatch.setattr(ingest, "download_main_l5_for_days", _fake_download)

    res = ml5.build_main_l5(
        var="cu",
        derived_symbol="KQ.m@SHFE.cu",
        exchange="SHFE",
        data_dir=str(tmp_path),
        update_mode=True,
    )

    assert cleared["called"] is True
    assert calls == [[date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]]
    assert res.days_total == 3
