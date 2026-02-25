from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import Request

from ghtrader.control.job_metadata import infer_job_metadata, merge_job_metadata
from ghtrader.control.jobs import JobSpec


def safe_int(raw: Any, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        val = int(str(raw).strip())
    except Exception:
        val = int(default)
    if min_value is not None:
        val = max(int(min_value), val)
    if max_value is not None:
        val = min(int(max_value), val)
    return int(val)


SECTION_TITLE_MAP: dict[str, str] = {
    "dashboard": "Dashboard",
    "jobs": "Jobs",
    "data": "Data Hub",
    "models": "Models",
    "trading": "Trading",
    "config": "Configuration",
    "sql": "SQL Explorer",
    "system": "System",
}


def variety_nav_ctx(
    variety: str,
    *,
    section: str = "",
    page_title: str = "",
    breadcrumbs: list[dict[str, str]] | None = None,
    page_variety: Callable[[str | None], str],
    allowed_varieties: Callable[[], set[str]],
) -> dict[str, Any]:
    v = page_variety(variety)
    section_key = str(section or "").strip().lower()
    current_title = str(page_title or SECTION_TITLE_MAP.get(section_key) or "Workspace").strip()
    crumb_list = breadcrumbs or [
        {"label": "Workspace", "href": f"/v/{v}/dashboard"},
        {"label": current_title, "href": ""},
    ]
    return {
        "variety": v,
        "allowed_varieties": list(allowed_varieties()),
        "variety_path_prefix": f"/v/{v}",
        "page_context": {
            "section": section_key,
            "page_title": current_title,
            "workspace_label": f"{v.upper()} workspace",
            "breadcrumbs": crumb_list,
        },
    }


def job_summary(*, request: Request, limit: int) -> dict[str, Any]:
    store = request.app.state.job_store
    jobs = store.list_jobs(limit=int(limit))
    running = [j for j in jobs if j.status == "running"]
    queued = [j for j in jobs if j.status == "queued"]
    return {
        "jobs": jobs,
        "running": running,
        "queued": queued,
        "running_count": len(running),
        "queued_count": len(queued),
    }


def find_existing_job_by_command(*, request: Request, argv: list[str]) -> Any | None:
    store = request.app.state.job_store
    target = [str(x) for x in list(argv)]
    try:
        active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
    except Exception:
        return None
    for job in active:
        cmd = [str(x) for x in (job.command or [])]
        if cmd == target:
            return job
    return None


def start_or_reuse_job_by_command(
    *, request: Request, title: str, argv: list[str], cwd: Path, metadata: dict[str, Any] | None = None
) -> Any:
    jm = request.app.state.job_manager
    auto_metadata = infer_job_metadata(argv=argv, title=title)
    merged_metadata = merge_job_metadata(auto_metadata, metadata)
    existing = find_existing_job_by_command(request=request, argv=argv)
    if existing is not None:
        if merged_metadata:
            try:
                store = request.app.state.job_store
                current_meta = dict(existing.metadata) if isinstance(getattr(existing, "metadata", None), dict) else None
                next_meta = merge_job_metadata(current_meta, merged_metadata)
                if next_meta and next_meta != current_meta:
                    store.update_job(existing.id, metadata=next_meta)
            except Exception:
                pass
        if existing.pid is None and str(existing.status or "").strip().lower() == "queued":
            try:
                started = jm.start_queued_job(existing.id)
                if started is not None:
                    return started
            except Exception:
                pass
        return existing
    return jm.start_job(JobSpec(title=title, argv=argv, cwd=cwd, metadata=merged_metadata))
