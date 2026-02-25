from __future__ import annotations

from pathlib import Path
from typing import Any

from ghtrader.config import get_env


def job_progress_from_env(*, runs_dir: Path, total_phases: int, message: str) -> Any | None:
    job_id = str(get_env("GHTRADER_JOB_ID", "") or "").strip()
    if not job_id:
        return None
    try:
        from ghtrader.control.progress import JobProgress

        progress = JobProgress(job_id=job_id, runs_dir=Path(runs_dir))
        progress.start(total_phases=max(1, int(total_phases)), message=str(message))
        return progress
    except Exception:
        return None


def progress_update(progress: Any | None, **kwargs: Any) -> None:
    if progress is None:
        return
    try:
        progress.update(**kwargs)
    except Exception:
        pass


def progress_finish(progress: Any | None, *, message: str) -> None:
    if progress is None:
        return
    try:
        progress.finish(message=str(message))
    except Exception:
        pass
