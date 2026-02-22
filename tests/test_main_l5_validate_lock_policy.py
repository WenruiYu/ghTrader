from __future__ import annotations

import os
from pathlib import Path

from click.testing import CliRunner

from ghtrader.cli import main
from ghtrader.control.db import JobStore
from ghtrader.control.locks import LockStore


def test_main_l5_validate_fail_fast_when_main_l5_running(tmp_path: Path, monkeypatch) -> None:
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_JOB_ID", "waiter_validate")
    monkeypatch.setenv("GHTRADER_JOB_SOURCE", "terminal")
    monkeypatch.setenv("GHTRADER_MAIN_L5_VALIDATE_LOCK_WAIT_TIMEOUT_S", "0.2")

    symbol = "KQ.m@SHFE.cu"
    db_path = runs_dir / "control" / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)

    owner_id = "owner_main_l5"
    store.create_job(
        job_id=owner_id,
        title=f"main-l5 SHFE.cu {symbol}",
        command=["python", "-m", "ghtrader.cli", "data", "main-l5", "--symbol", symbol],
        cwd=tmp_path,
        source="dashboard",
        log_path=None,
        metadata={
            "kind": "main_l5",
            "subcommand": "data",
            "subcommand2": "main-l5",
            "symbol": symbol,
        },
    )
    store.update_job(owner_id, status="running", pid=os.getpid())
    ok, _ = locks.acquire(lock_keys=[f"main_l5:symbol={symbol}"], job_id=owner_id, pid=os.getpid(), wait=False)
    assert ok is True

    runner = CliRunner()
    res = runner.invoke(
        main,
        [
            "data",
            "main-l5-validate",
            "--exchange",
            "SHFE",
            "--var",
            "cu",
            "--symbol",
            symbol,
            "--no-tqsdk-check",
        ],
    )

    assert res.exit_code != 0
    assert isinstance(res.exception, RuntimeError)
    err = str(res.exception or "")
    assert "build in progress" in err
    assert owner_id in err
