from __future__ import annotations

from datetime import date
import json
import threading
import time

import pandas as pd
import pytest


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


def test_main_l5_build_writes_single_aggregated_manifest(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.data.manifest as manifest
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-agg"
    schedule = pd.DataFrame(
        {
            "trading_day": [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4), date(2025, 1, 5)],
            "main_contract": ["SHFE.cu2501", "SHFE.cu2501", "SHFE.cu2502", "SHFE.cu2502"],
            "segment_id": [0, 0, 1, 1],
            "schedule_hash": [schedule_hash, schedule_hash, schedule_hash, schedule_hash],
        }
    )

    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)
    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", lambda **kwargs: 0)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)

    def _fake_download(**kwargs):
        days = sorted(kwargs["trading_days"])
        return {
            "rows_total": len(days),
            "row_counts": {d.isoformat(): 1 for d in days},
        }

    monkeypatch.setattr(ingest, "download_main_l5_for_days", _fake_download)

    manifest_calls: list[dict[str, object]] = []

    def _fake_write_manifest(**kwargs):
        manifest_calls.append(dict(kwargs))
        return tmp_path / "manifest.json"

    monkeypatch.setattr(manifest, "write_manifest", _fake_write_manifest)

    res = ml5.build_main_l5(
        var="cu",
        derived_symbol="KQ.m@SHFE.cu",
        exchange="SHFE",
        data_dir=str(tmp_path),
        update_mode=False,
    )

    assert res.days_total == 4
    assert len(manifest_calls) == 1
    row_counts = dict(manifest_calls[0].get("row_counts") or {})
    assert row_counts == {
        "2025-01-02": 1,
        "2025-01-03": 1,
        "2025-01-04": 1,
        "2025-01-05": 1,
    }


def test_main_l5_build_can_parallelize_segments(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.data.manifest as manifest
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-parallel"
    schedule = pd.DataFrame(
        {
            "trading_day": [date(2025, 1, 2), date(2025, 1, 3)],
            "main_contract": ["SHFE.cu2501", "SHFE.cu2502"],
            "segment_id": [0, 1],
            "schedule_hash": [schedule_hash, schedule_hash],
        }
    )

    monkeypatch.setenv("GHTRADER_MAIN_L5_SEGMENT_WORKERS", "2")
    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)
    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", lambda **kwargs: 0)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)
    monkeypatch.setattr(manifest, "write_manifest", lambda **kwargs: tmp_path / "manifest.json")

    thread_ids: set[int] = set()
    lock = threading.Lock()

    def _fake_download(**kwargs):
        with lock:
            thread_ids.add(threading.get_ident())
        time.sleep(0.05)
        days = sorted(kwargs["trading_days"])
        return {"rows_total": len(days), "row_counts": {d.isoformat(): len(days) for d in days}}

    monkeypatch.setattr(ingest, "download_main_l5_for_days", _fake_download)

    ml5.build_main_l5(
        var="cu",
        derived_symbol="KQ.m@SHFE.cu",
        exchange="SHFE",
        data_dir=str(tmp_path),
        update_mode=False,
    )

    assert len(thread_ids) >= 2


def test_main_l5_splits_download_budget_across_segments(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.data.manifest as manifest
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-budget"
    schedule = pd.DataFrame(
        {
            "trading_day": [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)],
            "main_contract": ["SHFE.cu2501", "SHFE.cu2502", "SHFE.cu2503"],
            "segment_id": [0, 1, 2],
            "schedule_hash": [schedule_hash, schedule_hash, schedule_hash],
        }
    )

    monkeypatch.setenv("GHTRADER_MAIN_L5_TOTAL_WORKERS", "6")
    monkeypatch.setenv("GHTRADER_MAIN_L5_SEGMENT_WORKERS", "3")
    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)
    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", lambda **kwargs: 0)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)
    monkeypatch.setattr(manifest, "write_manifest", lambda **kwargs: tmp_path / "manifest.json")

    passed_workers: list[int] = []

    def _fake_download(**kwargs):
        passed_workers.append(int(kwargs.get("workers") or 0))
        days = sorted(kwargs["trading_days"])
        return {"rows_total": len(days), "row_counts": {d.isoformat(): len(days) for d in days}}

    monkeypatch.setattr(ingest, "download_main_l5_for_days", _fake_download)

    ml5.build_main_l5(
        var="cu",
        derived_symbol="KQ.m@SHFE.cu",
        exchange="SHFE",
        data_dir=str(tmp_path),
        update_mode=False,
    )

    assert passed_workers == [2, 2, 2]


def test_main_l5_enforce_health_raises_on_missing_coverage(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-health"
    schedule = _schedule_df(schedule_hash)

    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)
    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", lambda **kwargs: 0)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)

    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(qqueries, "list_schedule_hashes_for_symbol", lambda **kwargs: {schedule_hash})
    monkeypatch.setattr(qqueries, "list_trading_days_for_symbol", lambda **kwargs: [date(2025, 1, 2)])

    monkeypatch.setattr(
        ingest,
        "download_main_l5_for_days",
        lambda **kwargs: {"rows_total": len(kwargs["trading_days"]), "row_counts": {}},
    )

    with pytest.raises(RuntimeError, match="main_l5 health check failed"):
        ml5.build_main_l5(
            var="cu",
            derived_symbol="KQ.m@SHFE.cu",
            exchange="SHFE",
            data_dir=str(tmp_path),
            update_mode=True,
            enforce_health=True,
        )


def test_main_l5_writes_progress_state(monkeypatch, tmp_path) -> None:
    from ghtrader.data import main_l5 as ml5
    import ghtrader.questdb.client as qclient
    import ghtrader.questdb.main_schedule as qms
    import ghtrader.questdb.queries as qqueries
    import ghtrader.tq.ingest as ingest
    import ghtrader.config as cfg

    schedule_hash = "hash-progress"
    schedule = _schedule_df(schedule_hash)
    schedule_days = [d for d in schedule["trading_day"].tolist() if isinstance(d, date)]

    monkeypatch.setenv("GHTRADER_JOB_ID", "testmainl5progress")
    monkeypatch.setattr(qclient, "make_questdb_query_config_from_env", lambda: object())
    monkeypatch.setattr(qms, "fetch_schedule", lambda **kwargs: schedule)
    monkeypatch.setattr(cfg, "get_runs_dir", lambda: tmp_path)
    monkeypatch.setattr(ml5, "_clear_main_l5_symbol", lambda **kwargs: 0)
    monkeypatch.setattr(ml5, "_purge_main_l5_l1", lambda **kwargs: 0)
    monkeypatch.setattr(qqueries, "query_symbol_day_bounds", lambda **kwargs: {})
    monkeypatch.setattr(qqueries, "list_schedule_hashes_for_symbol", lambda **kwargs: {schedule_hash})
    monkeypatch.setattr(qqueries, "list_trading_days_for_symbol", lambda **kwargs: schedule_days)
    monkeypatch.setattr(
        ingest,
        "download_main_l5_for_days",
        lambda **kwargs: {
            "rows_total": len(kwargs["trading_days"]),
            "row_counts": {d.isoformat(): 1 for d in kwargs["trading_days"]},
        },
    )

    ml5.build_main_l5(
        var="cu",
        derived_symbol="KQ.m@SHFE.cu",
        exchange="SHFE",
        data_dir=str(tmp_path),
        update_mode=False,
    )

    progress_path = tmp_path / "control" / "progress" / "job-testmainl5progress.progress.json"
    assert progress_path.exists()
    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert float(payload.get("pct") or 0.0) == 1.0
    assert "Complete:" in str(payload.get("message") or "")


def test_main_l5_clear_uses_replace_table_strategy_only(monkeypatch) -> None:
    from ghtrader.data import main_l5 as ml5
    from ghtrader.questdb import row_cleanup as cleanup

    calls: list[dict[str, object]] = []

    def _fake_replace(**kwargs):
        calls.append(dict(kwargs))
        return 7

    monkeypatch.setattr(cleanup, "replace_table_delete_where", _fake_replace)

    deleted = ml5._clear_main_l5_symbol(cfg=object(), symbol="KQ.m@SHFE.cu")

    assert deleted == 7
    assert len(calls) == 1
    call = calls[0]
    assert call["table"] == "ghtrader_ticks_main_l5_v2"
    assert call["delete_where_sql"] == "symbol = %s"
    assert call["delete_params"] == ["KQ.m@SHFE.cu"]


def test_main_l5_clear_is_fail_fast_when_replace_table_fails(monkeypatch) -> None:
    from ghtrader.data import main_l5 as ml5
    from ghtrader.questdb import row_cleanup as cleanup

    monkeypatch.setattr(
        cleanup,
        "replace_table_delete_where",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("rewrite failed")),
    )

    with pytest.raises(RuntimeError, match="rewrite failed"):
        ml5._clear_main_l5_symbol(cfg=object(), symbol="KQ.m@SHFE.cu")
