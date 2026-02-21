from __future__ import annotations

from pathlib import Path

from ghtrader.control.db import JobStore
from ghtrader.control.job_metadata import infer_job_metadata


def test_infer_job_metadata_main_schedule_variety() -> None:
    meta = infer_job_metadata(
        argv=[
            "python",
            "-m",
            "ghtrader.cli",
            "main-schedule",
            "--var",
            "au",
            "--data-dir",
            "data",
        ],
        title="main-schedule au env->latest",
    )
    assert meta.get("kind") == "main_schedule"
    assert meta.get("subcommand") == "main-schedule"
    assert meta.get("variety") == "au"


def test_infer_job_metadata_strategy_symbols() -> None:
    meta = infer_job_metadata(
        argv=[
            "python",
            "-m",
            "ghtrader.cli",
            "strategy",
            "run",
            "--account",
            "default",
            "--symbols",
            "SHFE.ag2602",
            "--symbols",
            "SHFE.ag2604",
        ],
        title="strategy default xgboost h=50",
    )
    assert meta.get("kind") == "strategy"
    assert meta.get("account_profile") == "default"
    assert meta.get("variety") == "ag"
    assert meta.get("symbols") == ["SHFE.ag2602", "SHFE.ag2604"]


def test_job_store_metadata_roundtrip(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "jobs.db")
    store.create_job(
        job_id="jobmeta001",
        title="main-schedule cu env->latest",
        command=["python", "-m", "ghtrader.cli", "main-schedule", "--var", "cu"],
        cwd=tmp_path,
        source="dashboard",
        log_path=None,
        metadata={"kind": "main_schedule", "variety": "cu"},
    )
    j = store.get_job("jobmeta001")
    assert j is not None
    assert isinstance(j.metadata, dict)
    assert j.metadata.get("variety") == "cu"

    store.update_job("jobmeta001", metadata={"kind": "main_schedule", "variety": "au"})
    j2 = store.get_job("jobmeta001")
    assert j2 is not None
    assert isinstance(j2.metadata, dict)
    assert j2.metadata.get("variety") == "au"
