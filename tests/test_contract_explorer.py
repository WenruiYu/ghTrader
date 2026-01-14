import json
from pathlib import Path

import pandas as pd


def test_df_has_l5_detection():
    from ghtrader.tqsdk_l5_probe import _df_has_l5

    df = pd.DataFrame({"bid_price2": [float("nan"), float("nan")], "ask_price2": [float("nan"), float("nan")]})
    assert _df_has_l5(df) is False

    df2 = pd.DataFrame({"bid_price2": [0.0, 50000.0], "ask_price2": [float("nan"), 50001.0]})
    assert _df_has_l5(df2) is True

    df3 = pd.DataFrame({"bid_price1": [50000.0], "ask_price1": [50001.0]})
    assert _df_has_l5(df3) is False


def test_parquet_local_l5_detection(tmp_path: Path):
    from ghtrader.control.contract_status import _parquet_file_has_local_l5

    p0 = tmp_path / "l1_only.parquet"
    pd.DataFrame({"bid_price2": [float("nan")], "ask_price2": [float("nan")]}).to_parquet(p0, index=False)
    assert _parquet_file_has_local_l5(p0) is False

    p1 = tmp_path / "l5.parquet"
    pd.DataFrame({"bid_price2": [50000.0], "ask_price2": [50001.0]}).to_parquet(p1, index=False)
    assert _parquet_file_has_local_l5(p1) is True


def test_contract_status_completeness_includes_no_data(tmp_path: Path):
    from ghtrader.control.contract_status import compute_contract_statuses

    data_dir = tmp_path / "data"
    runs_dir = tmp_path / "runs"

    sym_dir = data_dir / "lake" / "ticks" / "symbol=SHFE.cu2001"
    (sym_dir / "date=2020-01-06").mkdir(parents=True, exist_ok=True)
    (sym_dir / "date=2020-01-07").mkdir(parents=True, exist_ok=True)
    (sym_dir / "date=2020-01-10").mkdir(parents=True, exist_ok=True)
    (sym_dir / "_no_data_dates.json").write_text(json.dumps(["2020-01-08"]))

    res = compute_contract_statuses(
        exchange="SHFE",
        var="cu",
        lake_version="v1",
        data_dir=data_dir,
        runs_dir=runs_dir,
        contracts=[{"symbol": "SHFE.cu2001", "expired": True, "expire_datetime": None}],
        refresh_l5=False,
        max_l5_scan_per_symbol=0,
    )

    row = res["contracts"][0]
    assert res["active_ranges_available"] is False
    assert row["days_expected"] == 5  # weekday fallback
    assert row["days_done"] == 4  # 3 downloaded + 1 no-data marker
    assert abs(float(row["pct"]) - 0.8) < 1e-9
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


def test_pre20_quotes_filter_and_expire_datetime_iso(tmp_path: Path, monkeypatch):
    import lzma

    from ghtrader import tqsdk_catalog

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

