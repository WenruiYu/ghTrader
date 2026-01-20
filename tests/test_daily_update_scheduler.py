from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


def _make_app(tmp_path: Path):
    from ghtrader.control.db import JobStore
    from ghtrader.control.jobs import JobManager

    runs_dir = tmp_path / "runs"
    data_dir = tmp_path / "data"
    runs_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    store = JobStore(runs_dir / "control" / "jobs.db")
    jm = JobManager(store=store, logs_dir=runs_dir / "control" / "logs")
    app = SimpleNamespace(state=SimpleNamespace(job_store=store, job_manager=jm))
    return app, runs_dir, data_dir, store


def test_daily_update_dedup_queued(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from ghtrader.control.app import _daily_update_tick

    app, runs_dir, data_dir, store = _make_app(tmp_path)
    today = datetime.now(timezone.utc).date()

    monkeypatch.setattr("ghtrader.control.app.get_runs_dir", lambda: runs_dir)
    monkeypatch.setattr("ghtrader.control.app.get_data_dir", lambda: data_dir)
    monkeypatch.setattr("ghtrader.data.trading_calendar.get_trading_calendar", lambda **_kw: [today])
    monkeypatch.setenv("GHTRADER_DAILY_UPDATE_TARGETS", "SHFE:cu")

    _daily_update_tick(app=app)
    _daily_update_tick(app=app)

    queued = store.list_unstarted_queued_jobs(limit=1000)
    assert len(queued) == 1


def test_daily_update_backoff_on_failed_job(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from ghtrader.control.app import _daily_update_state_path, _daily_update_tick

    app, runs_dir, data_dir, store = _make_app(tmp_path)
    today = datetime.now(timezone.utc).date()

    monkeypatch.setattr("ghtrader.control.app.get_runs_dir", lambda: runs_dir)
    monkeypatch.setattr("ghtrader.control.app.get_data_dir", lambda: data_dir)
    monkeypatch.setattr("ghtrader.data.trading_calendar.get_trading_calendar", lambda **_kw: [today])
    monkeypatch.setenv("GHTRADER_DAILY_UPDATE_TARGETS", "SHFE:cu")
    monkeypatch.setenv("GHTRADER_DAILY_UPDATE_RETRY_BACKOFF_SECONDS", "300")

    # Seed a failed job in the store and reference it in state.
    job_id = "failedjob123"
    store.create_job(job_id=job_id, title="daily-update SHFE.cu", command=["ghtrader", "update"], cwd=Path.cwd())
    store.update_job(job_id, status="failed")

    st_path = _daily_update_state_path(runs_dir=runs_dir)
    st_path.parent.mkdir(parents=True, exist_ok=True)
    st_path.write_text(
        (
            "{"
            f"\"last_enqueued_trading_day\": {{\"SHFE:cu\": \"{today.isoformat()}\"}},"
            f"\"last_job_id\": {{\"SHFE:cu\": \"{job_id}\"}},"
            f"\"last_attempt_at\": {{\"SHFE:cu\": \"{datetime.now(timezone.utc).isoformat()}\"}},"
            "\"failure_count\": {\"SHFE:cu\": 0}"
            "}"
        )
    )

    _daily_update_tick(app=app)

    queued = store.list_unstarted_queued_jobs(limit=1000)
    assert len(queued) == 0

    import json

    state = json.loads(st_path.read_text())
    assert int(state.get("failure_count", {}).get("SHFE:cu", 0)) == 1
