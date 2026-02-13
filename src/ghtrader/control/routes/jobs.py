"""Jobs API routes (modularized from control.app)."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from ghtrader.config import get_runs_dir
from ghtrader.control import auth
from ghtrader.control.jobs import JobSpec

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _job_kind_from_command(cmd: list[str] | None) -> str:
    parts = [str(p) for p in (cmd or [])]
    if not parts:
        return "unknown"
    if "ghtrader" in parts:
        i = parts.index("ghtrader")
        if i + 1 < len(parts):
            return parts[i + 1].replace("-", "_")
    if "ghtrader.cli" in parts:
        i = parts.index("ghtrader.cli")
        if i + 1 < len(parts):
            return parts[i + 1].replace("-", "_")
    if "-m" in parts:
        i = parts.index("-m")
        if i + 2 < len(parts) and parts[i + 1].endswith("ghtrader.cli"):
            return parts[i + 2].replace("-", "_")
    return "unknown"


@router.get("", response_class=JSONResponse)
def api_list_jobs(request: Request, limit: int = 200) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    store = request.app.state.job_store
    jobs = store.list_jobs(limit=int(limit))
    return {"jobs": [asdict(j) for j in jobs]}


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
    jm = request.app.state.job_manager
    rec = jm.start_job(JobSpec(title=title, argv=list(argv), cwd=cwd))
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

        kind = _job_kind_from_command(list(job.command or []))
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
    return jm.read_log_tail(job_id, max_bytes=int(max_bytes))


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
            "available": False,
            "message": "No progress tracking for this job",
        }

    progress["job_status"] = job.status
    progress["available"] = True
    return progress


