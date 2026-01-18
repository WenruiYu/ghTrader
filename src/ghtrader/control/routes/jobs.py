"""Jobs API routes."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from ghtrader.control import auth
from ghtrader.control.jobs import JobSpec, python_module_argv

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_class=JSONResponse)
def api_list_jobs(request: Request, limit: int = 200) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    store = request.app.state.job_store
    jobs = store.list_jobs(limit=int(limit))
    return {"jobs": [asdict(j) for j in jobs]}


@router.post("", response_class=JSONResponse)
def api_enqueue_job(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    argv = body.get("argv") or []
    if not isinstance(argv, list):
        raise HTTPException(status_code=400, detail="argv must be a list")
    jm = request.app.state.job_manager
    spec = JobSpec(argv=argv)
    job = jm.enqueue(spec)
    return {"ok": True, "job": asdict(job)}


@router.post("/{job_id}/cancel", response_class=JSONResponse)
def api_cancel_job(request: Request, job_id: str) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    jm = request.app.state.job_manager
    ok = jm.cancel(job_id)
    return {"ok": ok}


@router.post("/cancel-batch", response_class=JSONResponse)
async def api_cancel_batch(request: Request) -> dict[str, Any]:
    """
    Cancel multiple jobs by status or job_ids.

    Body:
    - status: optional filter (queued|running)
    - job_ids: optional list of job IDs to cancel
    """
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        body = {}

    jm = request.app.state.job_manager
    store = request.app.state.job_store
    status_filter = str(body.get("status") or "").strip().lower()
    job_ids = body.get("job_ids")
    cancelled = 0

    if isinstance(job_ids, list) and job_ids:
        for jid in job_ids:
            if jm.cancel(str(jid)):
                cancelled += 1
    elif status_filter in {"queued", "running"}:
        jobs = store.list_jobs(limit=1000)
        for j in jobs:
            if j.status == status_filter:
                if jm.cancel(j.id):
                    cancelled += 1
    else:
        raise HTTPException(status_code=400, detail="Must provide status or job_ids")

    return {"ok": True, "cancelled": cancelled}


@router.get("/{job_id}/log", response_class=PlainTextResponse)
def api_job_log(request: Request, job_id: str) -> str:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    jm = request.app.state.job_manager
    return jm.read_log(job_id)


@router.get("/{job_id}/log/download")
def api_job_log_download(request: Request, job_id: str):
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    jm = request.app.state.job_manager
    log_path = jm.log_path(job_id)
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    filename = f"job_{job_id}.log"
    return FileResponse(path=str(log_path), media_type="text/plain", filename=filename)


@router.get("/{job_id}/ingest_status", response_class=JSONResponse)
def api_job_ingest_status(request: Request, job_id: str) -> dict[str, Any]:
    """Return parsed ingest status from job log (if available)."""
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from ghtrader.control.ingest_status import parse_ingest_status

    jm = request.app.state.job_manager
    log = jm.read_log(job_id)
    parsed = parse_ingest_status(log)
    return {"ok": True, "job_id": job_id, "ingest_status": parsed}
