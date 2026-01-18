import json
from pathlib import Path

import pandas as pd
import pytest


def test_df_has_l5_detection():
    from ghtrader.tq.l5_probe import _df_has_l5

    df = pd.DataFrame({"bid_price2": [float("nan"), float("nan")], "ask_price2": [float("nan"), float("nan")]})
    assert _df_has_l5(df) is False

    df2 = pd.DataFrame({"bid_price2": [0.0, 50000.0], "ask_price2": [float("nan"), 50001.0]})
    assert _df_has_l5(df2) is True

    df3 = pd.DataFrame({"bid_price1": [50000.0], "ask_price1": [50001.0]})
    assert _df_has_l5(df3) is False


def test_contract_status_completeness_uses_questdb_present_dates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    PRD (QuestDB-first): contract completeness is computed from QuestDB index coverage,
    not from local lake partitions.
    """
    from ghtrader.data.contract_status import compute_contract_statuses

    data_dir = tmp_path / "data"

    # Hermetic: avoid calendar downloads and QuestDB connections.
    import ghtrader.data.contract_status as cs
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(cs, "get_trading_calendar", lambda **_kwargs: [])
    monkeypatch.setattr(qix, "list_no_data_trading_days", lambda **_kwargs: set())

    cov = {
        "SHFE.cu2001": {
            "first_tick_day": "2020-01-06",
            "last_tick_day": "2020-01-10",
            "present_dates": {"2020-01-06", "2020-01-07", "2020-01-10"},
        }
    }

    res = compute_contract_statuses(
        exchange="SHFE",
        var="cu",
        data_dir=data_dir,
        contracts=[{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None}],
        questdb_cov_by_symbol=cov,
    )

    row = res["contracts"][0]
    assert res["active_ranges_available"] is False
    assert row["days_expected"] == 5  # weekday fallback
    assert row["days_done"] == 3  # 3 present in QuestDB index
    assert abs(float(row["pct"]) - 0.6) < 1e-9
    assert row["status"] == "incomplete"


def test_contract_status_prefers_questdb_bounds_for_expected_range(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ghtrader.data.contract_status import compute_contract_statuses

    data_dir = tmp_path / "data"

    # Hermetic: avoid calendar downloads and QuestDB connections.
    import ghtrader.data.contract_status as cs
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(cs, "get_trading_calendar", lambda **_kwargs: [])
    monkeypatch.setattr(qix, "list_no_data_trading_days", lambda **_kwargs: set())

    # QuestDB says contract spans a longer range (5 weekdays).
    cov = {
        "SHFE.cu2001": {
            "first_tick_day": "2020-01-06",
            "last_tick_day": "2020-01-10",
            "present_dates": {"2020-01-06", "2020-01-07"},
        }
    }

    res = compute_contract_statuses(
        exchange="SHFE",
        var="cu",
        data_dir=data_dir,
        contracts=[{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None}],
        questdb_cov_by_symbol=cov,
    )
    row = res["contracts"][0]
    assert row["expected_first"] == "2020-01-06"
    assert row["expected_last"] == "2020-01-10"
    assert row["days_expected"] == 5
    assert row["days_done"] == 2
    assert row["status"] == "incomplete"


def test_tqsdk_scheduler_tick_respects_max_parallel(tmp_path: Path, monkeypatch):
    from ghtrader.control.app import _tqsdk_scheduler_tick
    from ghtrader.control.db import JobStore
    from ghtrader.control.jobs import JobManager

    db_path = tmp_path / "jobs.db"
    logs_dir = tmp_path / "logs"
    store = JobStore(db_path)
    jm = JobManager(store=store, logs_dir=logs_dir)

    def make_job(job_id: str, status: str, pid: int | None) -> None:
        store.create_job(
            job_id=job_id,
            title=job_id,
            command=["python", "-m", "ghtrader.cli", "download", "--symbol", "SHFE.cu2001", "--start", "2020-01-01", "--end", "2020-01-02"],
            cwd=tmp_path,
            source="dashboard",
            log_path=logs_dir / f"job-{job_id}.log",
        )
        if status != "queued" or pid is not None:
            store.update_job(job_id, status=status, pid=pid or 0, started_at="t")

    # 3 active heavy jobs already running
    make_job("run0", "running", 100)
    make_job("run1", "running", 101)
    make_job("run2", "running", 102)

    # 10 queued heavy jobs not started
    for i in range(10):
        make_job(f"q{i}", "queued", None)

    started_ids: list[str] = []

    def fake_start_queued_job(job_id: str):
        store.update_job(job_id, status="running", pid=200 + len(started_ids), started_at="t")
        started_ids.append(job_id)
        return store.get_job(job_id)

    monkeypatch.setattr(jm, "start_queued_job", fake_start_queued_job)

    started = _tqsdk_scheduler_tick(store=store, jm=jm, max_parallel=4)
    assert started == 1
    assert len(started_ids) == 1

    started2 = _tqsdk_scheduler_tick(store=store, jm=jm, max_parallel=4)
    assert started2 == 0


def test_tqsdk_scheduler_starts_data_health_jobs(tmp_path: Path, monkeypatch):
    """
    data health can be TqSdk-heavy (auto-repair downloads) and must be scheduler-throttled.
    """
    from ghtrader.control.app import _tqsdk_scheduler_tick
    from ghtrader.control.db import JobStore
    from ghtrader.control.jobs import JobManager

    db_path = tmp_path / "jobs.db"
    logs_dir = tmp_path / "logs"
    store = JobStore(db_path)
    jm = JobManager(store=store, logs_dir=logs_dir)

    # One queued health job (pid is NULL).
    store.create_job(
        job_id="qhealth1",
        title="data-health SHFE.cu",
        command=[
            "python",
            "-m",
            "ghtrader.cli",
            "data",
            "health",
            "--exchange",
            "SHFE",
            "--var",
            "cu",
            "--thoroughness",
            "standard",
            "--auto-repair",
            "--refresh-catalog",
            "0",
            "--chunk-days",
            "5",
            "--data-dir",
            "data",
            "--runs-dir",
            "runs",
        ],
        cwd=tmp_path,
        source="dashboard",
        log_path=logs_dir / "job-qhealth1.log",
    )

    started_ids: list[str] = []

    def fake_start_queued_job(job_id: str):
        store.update_job(job_id, status="running", pid=999, started_at="t")
        started_ids.append(job_id)
        return store.get_job(job_id)

    monkeypatch.setattr(jm, "start_queued_job", fake_start_queued_job)

    started = _tqsdk_scheduler_tick(store=store, jm=jm, max_parallel=4)
    assert started == 1
    assert started_ids == ["qhealth1"]


def test_pre20_quotes_filter_and_expire_datetime_iso(tmp_path: Path, monkeypatch):
    import lzma

    import ghtrader.tq.catalog as tqsdk_catalog

    # Create a fake expired_quotes.json.lzma
    pkg_dir = tmp_path / "tqsdk"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    p = pkg_dir / "expired_quotes.json.lzma"
    payload = {
        "SHFE.cu1601": {"exchange_id": "SHFE", "product_id": "cu", "ins_class": "FUTURE", "expired": True, "expire_datetime": 1451606400},
        "DCE.a2001": {"exchange_id": "DCE", "product_id": "a", "ins_class": "FUTURE", "expired": True, "expire_datetime": 0},
        "SHFE.cu_opt": {"exchange_id": "SHFE", "product_id": "cu", "ins_class": "OPTION", "expired": True, "expire_datetime": 1},
    }
    with lzma.open(str(p), "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload))

    monkeypatch.setattr(tqsdk_catalog, "_tqsdk_pre20_cache_path", lambda: p)

    out = tqsdk_catalog._pre20_contracts(exchange="SHFE", var="cu")
    assert "SHFE.cu1601" in out
    assert out["SHFE.cu1601"]["catalog_source"] == "pre20_cache"
    assert out["SHFE.cu1601"]["expired"] is True
    assert "T" in str(out["SHFE.cu1601"]["expire_datetime"])
    assert "DCE.a2001" not in out
    assert "SHFE.cu_opt" not in out

