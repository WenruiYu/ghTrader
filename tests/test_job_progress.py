"""Tests for job progress tracking."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from ghtrader.control.progress import JobProgress, ProgressState, get_job_progress


class TestJobProgress:
    """Test JobProgress class."""

    def test_start_creates_progress_file(self, tmp_path: Path) -> None:
        """Test that start() creates the progress file."""
        progress = JobProgress(job_id="test123", runs_dir=tmp_path)
        progress.start(total_phases=2, message="Starting test...")

        progress_file = tmp_path / "control" / "progress" / "job-test123.progress.json"
        assert progress_file.exists()

        with open(progress_file) as f:
            data = json.load(f)

        assert data["job_id"] == "test123"
        assert data["total_phases"] == 2
        assert data["message"] == "Starting test..."
        assert data["pct"] == 0.0
        assert data["started_at"]
        assert data["updated_at"]

    def test_update_modifies_progress(self, tmp_path: Path) -> None:
        """Test that update() modifies the progress file."""
        progress = JobProgress(job_id="test456", runs_dir=tmp_path)
        progress.start(total_phases=1)

        progress.update(
            phase="diagnose",
            step="schema_validation",
            step_idx=1,
            total_steps=5,
            message="Validating schema...",
        )

        data = get_job_progress("test456", tmp_path)
        assert data is not None
        assert data["phase"] == "diagnose"
        assert data["step"] == "schema_validation"
        assert data["step_idx"] == 1
        assert data["total_steps"] == 5
        assert data["message"] == "Validating schema..."

    def test_item_tracking(self, tmp_path: Path) -> None:
        """Test item-level progress tracking."""
        progress = JobProgress(job_id="test789", runs_dir=tmp_path)
        progress.start(total_phases=1)

        progress.update(
            phase="diagnose",
            step="field_quality",
            step_idx=2,
            total_steps=5,
            item="SHFE.cu2602",
            item_idx=5,
            total_items=20,
            message="Checking SHFE.cu2602...",
        )

        data = get_job_progress("test789", tmp_path)
        assert data is not None
        assert data["item"] == "SHFE.cu2602"
        assert data["item_idx"] == 5
        assert data["total_items"] == 20

    def test_pct_calculation(self, tmp_path: Path) -> None:
        """Test percentage calculation across phases/steps/items."""
        progress = JobProgress(job_id="testpct", runs_dir=tmp_path)
        progress.start(total_phases=2)

        # Phase 0, step 0 of 4
        progress.update(phase="diagnose", phase_idx=0, total_phases=2, step="step1", step_idx=0, total_steps=4)
        data = get_job_progress("testpct", tmp_path)
        # Expected: 0/2 phases = 0%
        assert data is not None
        assert data["pct"] == 0.0

        # Phase 0, step 2 of 4 (halfway through first phase)
        progress.update(step="step2", step_idx=2, total_steps=4)
        data = get_job_progress("testpct", tmp_path)
        # Expected: 0 + 2/4 * 0.5 = 0.25
        assert data is not None
        assert 0.2 <= data["pct"] <= 0.3

        # Phase 1 (second phase starts)
        progress.update(phase="repair", phase_idx=1, step="repair1", step_idx=0, total_steps=2)
        data = get_job_progress("testpct", tmp_path)
        # Expected: 1/2 = 0.5
        assert data is not None
        assert 0.45 <= data["pct"] <= 0.55

    def test_finish_sets_complete(self, tmp_path: Path) -> None:
        """Test that finish() sets pct to 1.0."""
        progress = JobProgress(job_id="testfin", runs_dir=tmp_path)
        progress.start(total_phases=1)
        progress.update(phase="test", step="step1", step_idx=0, total_steps=2)
        progress.finish(message="All done!")

        data = get_job_progress("testfin", tmp_path)
        assert data is not None
        assert data["pct"] == 1.0
        assert data["eta_seconds"] == 0
        assert data["message"] == "All done!"

    def test_set_error(self, tmp_path: Path) -> None:
        """Test that set_error() records the error."""
        progress = JobProgress(job_id="testerr", runs_dir=tmp_path)
        progress.start(total_phases=1)
        progress.set_error("Something went wrong")

        data = get_job_progress("testerr", tmp_path)
        assert data is not None
        assert data["error"] == "Something went wrong"

    def test_read_nonexistent_returns_none(self, tmp_path: Path) -> None:
        """Test that reading a nonexistent progress file returns None."""
        state = JobProgress.read("nonexistent", tmp_path)
        assert state is None

        data = get_job_progress("nonexistent", tmp_path)
        assert data is None

    def test_eta_calculation(self, tmp_path: Path) -> None:
        """Test ETA calculation based on elapsed time and progress."""
        progress = JobProgress(job_id="testeta", runs_dir=tmp_path)
        progress.start(total_phases=1)

        # Simulate some progress after a delay
        time.sleep(0.1)
        progress.update(phase="test", step="step1", step_idx=1, total_steps=4)

        data = get_job_progress("testeta", tmp_path)
        assert data is not None
        # ETA should be calculated (positive or None)
        if data["eta_seconds"] is not None:
            assert data["eta_seconds"] >= 0


class TestProgressState:
    """Test ProgressState dataclass."""

    def test_to_dict(self) -> None:
        """Test to_dict() serialization."""
        state = ProgressState(
            job_id="abc",
            phase="test",
            phase_idx=1,
            total_phases=2,
            step="step1",
            step_idx=0,
            total_steps=3,
            item="item1",
            item_idx=5,
            total_items=10,
            message="Testing...",
            pct=0.5,
            eta_seconds=120.5,
            started_at="2026-01-18T00:00:00Z",
            updated_at="2026-01-18T00:01:00Z",
            error=None,
        )

        d = state.to_dict()
        assert d["job_id"] == "abc"
        assert d["phase"] == "test"
        assert d["pct"] == 0.5
        assert d["eta_seconds"] == 120.5
        assert d["error"] is None


@pytest.fixture
def test_app_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Create a test client with the progress API."""
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    import importlib

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)

    from fastapi.testclient import TestClient

    return TestClient(mod.app), tmp_path


class TestProgressAPI:
    """Test progress API endpoint."""

    def test_progress_endpoint_no_progress_file(self, test_app_client) -> None:
        """Test progress endpoint when no progress file exists."""
        client, tmp_path = test_app_client

        # First create a job
        store = client.app.state.job_store
        from pathlib import Path

        job = store.create_job(
            job_id="testjob123",
            command=["echo", "test"],
            title="Test job",
            cwd=Path("/tmp"),
        )

        resp = client.get("/api/jobs/testjob123/progress")
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == "testjob123"
        assert data["available"] is False
        assert data["state"] == "queued"

    def test_progress_endpoint_with_progress_file(self, test_app_client) -> None:
        """Test progress endpoint when progress file exists."""
        client, tmp_path = test_app_client
        runs_dir = tmp_path / "runs"

        # First create a job
        store = client.app.state.job_store
        from pathlib import Path

        job = store.create_job(
            job_id="testjob456",
            command=["echo", "test"],
            title="Test job",
            cwd=Path("/tmp"),
        )

        # Create progress file
        progress = JobProgress(job_id="testjob456", runs_dir=runs_dir)
        progress.start(total_phases=1)
        progress.update(
            phase="diagnose",
            step="schema_validation",
            step_idx=1,
            total_steps=5,
            message="Testing...",
        )

        resp = client.get("/api/jobs/testjob456/progress")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True
        assert data["phase"] == "diagnose"
        assert data["step"] == "schema_validation"
        assert data["job_status"] == "queued"  # Job created with queued status
        assert data["state"] == "queued"
        assert "stale" in data

    def test_progress_endpoint_job_not_found(self, test_app_client) -> None:
        """Test progress endpoint returns 404 for unknown job."""
        client, _ = test_app_client

        resp = client.get("/api/jobs/unknownjob/progress")
        assert resp.status_code == 404
