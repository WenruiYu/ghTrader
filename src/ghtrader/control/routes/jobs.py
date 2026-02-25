"""Jobs API routes (modularized from control.app)."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from ghtrader.config import get_runs_dir
from ghtrader.control.app_helpers import job_matches_variety as _job_matches_variety
from ghtrader.control import auth
from ghtrader.control.job_command import infer_job_kind
from ghtrader.control.jobs import JobSpec
from ghtrader.control.routes.query_budget import (
    bounded_pagination,
    bounded_scan_limit,
    bounded_int,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])


from ghtrader.util.time import now_iso as _now_iso


def _job_kind(job: Any) -> str:
    meta = getattr(job, "metadata", None)
    if isinstance(meta, dict):
        mk = str(meta.get("kind") or "").strip().lower()
        if mk:
            return mk
    return str(infer_job_kind(list(getattr(job, "command", None) or [])) or "").strip().lower()


def _parse_iso_utc(value: str) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _progress_state(*, job_status: str, pct: float | None, error: str | None) -> str:
    st = str(job_status or "").strip().lower()
    if error:
        return "error"
    if st in {"queued", "running", "succeeded", "failed", "cancelled"}:
        return st
    p = float(pct or 0.0)
    if p >= 1.0:
        return "succeeded"
    if p > 0:
        return "running"
    return "unknown"

@router.get("", response_class=JSONResponse)
def api_list_jobs(
    request: Request,
    limit: int = 200,
    offset: int = 0,
    status: str = "",
    kind: str = "",
    var: str = "",
    q: str = "",
) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    store = request.app.state.job_store
    lim, off = bounded_pagination(
        limit=limit,
        offset=offset,
        default_limit=200,
        max_limit=1000,
        max_offset=5000,
    )
    status_filter = str(status or "").strip().lower()
    kind_filter = str(kind or "").strip().lower()
    var_filter = str(var or "").strip().lower()
    query = str(q or "").strip().lower()

    # Pull a bounded window large enough for common filtering/pagination use-cases.
    jobs_all = store.list_jobs(limit=bounded_scan_limit(limit=lim, offset=off, floor=2000, headroom=200, max_scan=6200))
    filtered: list[Any] = []
    for job in jobs_all:
        st = str(getattr(job, "status", "") or "").strip().lower()
        if status_filter and st != status_filter:
            continue
        jk = _job_kind(job)
        if kind_filter and jk != kind_filter:
            continue
        if var_filter and not _job_matches_variety(job, var_filter):
            continue
        if query:
            hay = " ".join(
                [
                    str(getattr(job, "id", "") or ""),
                    str(getattr(job, "title", "") or ""),
                    str(getattr(job, "source", "") or ""),
                    " ".join([str(x or "") for x in (getattr(job, "command", None) or [])]),
                ]
            ).lower()
            if query not in hay:
                continue
        filtered.append(job)

    total = len(filtered)
    rows = filtered[off : off + lim]
    next_offset = off + lim if (off + lim) < total else None
    return {
        "ok": True,
        "jobs": [asdict(j) for j in rows],
        "total": int(total),
        "offset": int(off),
        "limit": int(lim),
        "next_offset": next_offset,
        "state": "ok",
        "text": f"{len(rows)}/{total} jobs",
        "error": "",
        "stale": False,
        "updated_at": _now_iso(),
        "filters": {
            "status": status_filter or None,
            "kind": kind_filter or None,
            "var": var_filter or None,
            "q": query or None,
        },
    }


@router.post("", response_class=JSONResponse)
async def api_create_job(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()
    title = str(payload.get("title") or "job")
    argv = payload.get("argv")
    if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
        raise HTTPException(status_code=400, detail="argv must be a list[str]")

    cwd = Path(str(payload.get("cwd") or Path.cwd()))
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise HTTPException(status_code=400, detail="metadata must be an object")
    jm = request.app.state.job_manager
    rec = jm.start_job(JobSpec(title=title, argv=list(argv), cwd=cwd, metadata=(dict(metadata) if isinstance(metadata, dict) else None)))
    return {"job": asdict(rec)}


@router.post("/{job_id}/cancel", response_class=JSONResponse)
def api_cancel_job(request: Request, job_id: str) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    jm = request.app.state.job_manager
    ok = jm.cancel_job(job_id)
    return {"ok": bool(ok)}


@router.post("/cancel-batch", response_class=JSONResponse)
async def api_cancel_jobs_batch(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()
    kinds_raw = payload.get("kinds")
    statuses_raw = payload.get("statuses")
    include_unstarted_queued = bool(payload.get("include_unstarted_queued", False))

    if kinds_raw is None:
        kinds: set[str] = set()
    elif isinstance(kinds_raw, list) and all(isinstance(x, str) for x in kinds_raw):
        kinds = {str(x).strip() for x in kinds_raw if str(x).strip()}
    else:
        raise HTTPException(status_code=400, detail="kinds must be list[str]")

    if statuses_raw is None:
        statuses = {"running", "queued"}
    elif isinstance(statuses_raw, list) and all(isinstance(x, str) for x in statuses_raw):
        statuses = {str(x).strip().lower() for x in statuses_raw if str(x).strip()}
    else:
        raise HTTPException(status_code=400, detail="statuses must be list[str]")

    if not kinds:
        kinds = {"main_schedule", "main_l5"}

    store = request.app.state.job_store
    jm = request.app.state.job_manager
    matched: list[str] = []
    cancelled: list[str] = []
    failed: list[dict[str, Any]] = []

    jobs = store.list_jobs(limit=2000)
    for job in jobs:
        st = str(job.status or "").lower().strip()
        if st not in statuses:
            continue

        kind = infer_job_kind(list(job.command or []))
        if kind not in kinds:
            continue

        if st == "queued" and job.pid is None and not include_unstarted_queued:
            continue

        matched.append(job.id)
        ok = False
        try:
            ok = bool(jm.cancel_job(job.id))
        except Exception as e:
            ok = False
            failed.append({"job_id": job.id, "error": str(e), "kind": kind, "status": st})
        if ok:
            cancelled.append(job.id)
        else:
            failed.append({"job_id": job.id, "error": "cancel_failed", "kind": kind, "status": st})

    return {
        "ok": True,
        "kinds": sorted(list(kinds)),
        "statuses": sorted(list(statuses)),
        "matched": int(len(matched)),
        "cancelled": int(len(cancelled)),
        "failed": failed,
        "job_ids": {"matched": matched, "cancelled": cancelled},
    }


@router.get("/{job_id}/log", response_class=PlainTextResponse)
def api_job_log(request: Request, job_id: str, max_bytes: int = 64000) -> str:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    jm = request.app.state.job_manager
    bounded_max_bytes = bounded_int(max_bytes, default=64000, min_value=1024, max_value=1_048_576)
    return jm.read_log_tail(job_id, max_bytes=int(bounded_max_bytes))


@router.get("/{job_id}/log/download")
def api_job_log_download(request: Request, job_id: str) -> Any:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    store = request.app.state.job_store
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if not job.log_path:
        raise HTTPException(status_code=404, detail="No log path for job")

    p = Path(str(job.log_path)).resolve()
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    logs_root = (get_runs_dir() / "control" / "logs").resolve()
    if logs_root not in p.parents:
        raise HTTPException(status_code=400, detail="Invalid log path")

    return FileResponse(path=str(p), filename=p.name, media_type="text/plain")


@router.get("/{job_id}/progress", response_class=JSONResponse)
def api_job_progress(request: Request, job_id: str) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    store = request.app.state.job_store
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    from ghtrader.control.progress import get_job_progress

    progress = get_job_progress(job_id=job.id, runs_dir=get_runs_dir())
    if progress is None:
        return {
            "job_id": job.id,
            "job_status": job.status,
            "state": str(job.status or "").strip().lower() or "unknown",
            "available": False,
            "message": "No progress tracking for this job",
        }

    stale_s_raw = str((request.headers.get("x-ghtrader-progress-stale-s") or "")).strip()
    try:
        stale_threshold_s = int(stale_s_raw) if stale_s_raw else 45
    except Exception:
        stale_threshold_s = 45
    stale_threshold_s = max(5, stale_threshold_s)
    updated_age_s: int | None = None
    updated_at_dt = _parse_iso_utc(str(progress.get("updated_at") or ""))
    if updated_at_dt is not None:
        updated_age_s = max(0, int((datetime.now(timezone.utc) - updated_at_dt).total_seconds()))
    running_like = str(job.status or "").strip().lower() in {"queued", "running"}
    stale = bool(running_like and updated_age_s is not None and updated_age_s > stale_threshold_s)

    progress["job_status"] = job.status
    progress["state"] = _progress_state(
        job_status=str(job.status or ""),
        pct=(float(progress.get("pct") or 0.0) if progress.get("pct") is not None else None),
        error=(str(progress.get("error") or "") or None),
    )
    progress["updated_age_s"] = updated_age_s
    progress["stale"] = bool(stale)
    progress["available"] = True
    return progress


