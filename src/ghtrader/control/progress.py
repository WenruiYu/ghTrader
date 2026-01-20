"""
Job progress tracking for dashboard observability.

This module provides structured progress reporting for long-running jobs.
Progress is written to JSON files that the dashboard can poll for real-time updates.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ProgressState:
    """Current state of job progress."""

    job_id: str
    phase: str = ""
    phase_idx: int = 0
    total_phases: int = 1
    step: str = ""
    step_idx: int = 0
    total_steps: int = 1
    item: str = ""
    item_idx: int = 0
    total_items: int = 0
    message: str = ""
    pct: float = 0.0
    eta_seconds: float | None = None
    started_at: str = ""
    updated_at: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "phase": self.phase,
            "phase_idx": self.phase_idx,
            "total_phases": self.total_phases,
            "step": self.step,
            "step_idx": self.step_idx,
            "total_steps": self.total_steps,
            "item": self.item,
            "item_idx": self.item_idx,
            "total_items": self.total_items,
            "message": self.message,
            "pct": round(self.pct, 4),
            "eta_seconds": round(self.eta_seconds, 1) if self.eta_seconds is not None else None,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "error": self.error,
        }


class JobProgress:
    """
    Thread-safe progress writer for long-running jobs.

    Usage:
        progress = JobProgress(job_id="abc123", runs_dir=Path("runs"))
        progress.start(total_phases=2)
        progress.update(phase="diagnose", step="schema", step_idx=0, total_steps=5)
        progress.update(phase="diagnose", step="completeness", step_idx=1, total_steps=5,
                       item="SHFE.cu2602", item_idx=3, total_items=20)
        progress.finish()
    """

    def __init__(self, job_id: str, runs_dir: Path | str):
        self.job_id = str(job_id)
        self.runs_dir = Path(runs_dir)
        self.progress_dir = self.runs_dir / "control" / "progress"
        self.path = self.progress_dir / f"job-{self.job_id}.progress.json"
        self._started_at: float | None = None
        self._phase_started_at: float | None = None
        self._step_started_at: float | None = None
        self._last_pct: float = 0.0
        self._last_pct_time: float | None = None
        self._state = ProgressState(job_id=self.job_id)

    def start(self, *, total_phases: int = 1, message: str = "Starting...") -> None:
        """Initialize progress tracking."""
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).isoformat()
        self._started_at = time.monotonic()
        self._state = ProgressState(
            job_id=self.job_id,
            phase="",
            phase_idx=0,
            total_phases=max(1, total_phases),
            step="",
            step_idx=0,
            total_steps=1,
            item="",
            item_idx=0,
            total_items=0,
            message=message,
            pct=0.0,
            eta_seconds=None,
            started_at=now,
            updated_at=now,
        )
        self._write()

    def update(
        self,
        *,
        phase: str | None = None,
        phase_idx: int | None = None,
        total_phases: int | None = None,
        step: str | None = None,
        step_idx: int | None = None,
        total_steps: int | None = None,
        item: str | None = None,
        item_idx: int | None = None,
        total_items: int | None = None,
        message: str | None = None,
    ) -> None:
        """Update progress state."""
        now_mono = time.monotonic()

        # Track phase transitions
        if phase is not None and phase != self._state.phase:
            self._phase_started_at = now_mono
            self._step_started_at = now_mono

        # Track step transitions
        if step is not None and step != self._state.step:
            self._step_started_at = now_mono

        # Update state
        if phase is not None:
            self._state.phase = phase
        if phase_idx is not None:
            self._state.phase_idx = phase_idx
        if total_phases is not None:
            self._state.total_phases = max(1, total_phases)
        if step is not None:
            self._state.step = step
        if step_idx is not None:
            self._state.step_idx = step_idx
        if total_steps is not None:
            self._state.total_steps = max(1, total_steps)
        if item is not None:
            self._state.item = item
        if item_idx is not None:
            self._state.item_idx = item_idx
        if total_items is not None:
            self._state.total_items = total_items
        if message is not None:
            self._state.message = message

        # Calculate overall percentage
        self._state.pct = self._calculate_pct()
        self._state.eta_seconds = self._calculate_eta(now_mono)
        self._state.updated_at = datetime.now(timezone.utc).isoformat()

        self._write()

    def set_error(self, error: str) -> None:
        """Mark job as having an error."""
        self._state.error = error
        self._state.updated_at = datetime.now(timezone.utc).isoformat()
        self._write()

    def finish(self, *, message: str = "Complete") -> None:
        """Mark progress as complete."""
        self._state.pct = 1.0
        self._state.eta_seconds = 0
        self._state.message = message
        self._state.updated_at = datetime.now(timezone.utc).isoformat()
        self._write()

    def _calculate_pct(self) -> float:
        """Calculate overall progress percentage (0.0 to 1.0)."""
        total_phases = max(1, self._state.total_phases)
        total_steps = max(1, self._state.total_steps)
        total_items = max(1, self._state.total_items) if self._state.total_items > 0 else 1

        # Weight: phase contributes 1/total_phases, step contributes within phase, item within step
        phase_weight = 1.0 / total_phases
        step_weight = phase_weight / total_steps

        # Base progress from completed phases
        pct = self._state.phase_idx * phase_weight

        # Add progress from current phase's completed steps
        pct += self._state.step_idx * step_weight

        # Add progress from current step's items
        if self._state.total_items > 0:
            item_weight = step_weight / total_items
            pct += self._state.item_idx * item_weight

        return min(1.0, max(0.0, pct))

    def _calculate_eta(self, now_mono: float) -> float | None:
        """Estimate time remaining based on progress rate."""
        if self._started_at is None:
            return None

        elapsed = now_mono - self._started_at
        pct = self._state.pct

        if pct <= 0 or elapsed <= 0:
            return None

        # Simple linear extrapolation
        total_time_estimate = elapsed / pct
        remaining = total_time_estimate - elapsed

        return max(0.0, remaining)

    def _write(self) -> None:
        """Write progress state to file atomically."""
        try:
            data = self._state.to_dict()
            tmp_path = self.path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.path)
        except Exception:
            # Ignore write failures - progress is best-effort
            pass

    @classmethod
    def read(cls, job_id: str, runs_dir: Path | str) -> ProgressState | None:
        """Read progress state for a job."""
        runs = Path(runs_dir)
        path = runs / "control" / "progress" / f"job-{job_id}.progress.json"
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return ProgressState(
                job_id=str(data.get("job_id") or job_id),
                phase=str(data.get("phase") or ""),
                phase_idx=int(data.get("phase_idx") or 0),
                total_phases=int(data.get("total_phases") or 1),
                step=str(data.get("step") or ""),
                step_idx=int(data.get("step_idx") or 0),
                total_steps=int(data.get("total_steps") or 1),
                item=str(data.get("item") or ""),
                item_idx=int(data.get("item_idx") or 0),
                total_items=int(data.get("total_items") or 0),
                message=str(data.get("message") or ""),
                pct=float(data.get("pct") or 0.0),
                eta_seconds=float(data["eta_seconds"]) if data.get("eta_seconds") is not None else None,
                started_at=str(data.get("started_at") or ""),
                updated_at=str(data.get("updated_at") or ""),
                error=str(data["error"]) if data.get("error") else None,
            )
        except Exception:
            return None


def get_job_progress(job_id: str, runs_dir: Path | str) -> dict[str, Any] | None:
    """
    Get progress for a job.

    Returns the progress state as a dict, or None if not available.
    """
    state = JobProgress.read(job_id, runs_dir)
    if state is None:
        return None
    return state.to_dict()
