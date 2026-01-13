from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from ghtrader.config import get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.db import JobStore
from ghtrader.control.jobs import JobManager, JobSpec
from ghtrader.control.views import build_router

log = structlog.get_logger()


def _control_root(runs_dir: Path) -> Path:
    return runs_dir / "control"


def _jobs_db_path(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "jobs.db"


def _logs_dir(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "logs"


def create_app() -> Any:
    runs_dir = get_runs_dir()
    store = JobStore(_jobs_db_path(runs_dir))
    jm = JobManager(store=store, logs_dir=_logs_dir(runs_dir))
    jm.reconcile()

    app = FastAPI(title="ghTrader Control", version="0.1.0")
    app.state.job_store = store
    app.state.job_manager = jm

    # Static assets (CSS)
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # HTML pages
    app.include_router(build_router())

    # JSON API (kept small; UI uses HTML routes above)
    @app.get("/health", response_class=JSONResponse)
    def health() -> dict[str, Any]:
        return {"ok": True}

    @app.get("/api/jobs", response_class=JSONResponse)
    def api_list_jobs(request: Request, limit: int = 200) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        jobs = store.list_jobs(limit=int(limit))
        return {"jobs": [asdict(j) for j in jobs]}

    @app.post("/api/jobs", response_class=JSONResponse)
    async def api_create_job(request: Request) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        title = str(payload.get("title") or "job")
        argv = payload.get("argv")
        if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
            raise HTTPException(status_code=400, detail="argv must be a list[str]")

        cwd = Path(str(payload.get("cwd") or Path.cwd()))
        rec = jm.start_job(JobSpec(title=title, argv=list(argv), cwd=cwd))
        return {"job": asdict(rec)}

    @app.post("/api/jobs/{job_id}/cancel", response_class=JSONResponse)
    def api_cancel_job(request: Request, job_id: str) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ok = jm.cancel_job(job_id)
        return {"ok": bool(ok)}

    @app.get("/api/jobs/{job_id}/log", response_class=PlainTextResponse)
    def api_job_log(request: Request, job_id: str, max_bytes: int = 64000) -> str:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return jm.read_log_tail(job_id, max_bytes=int(max_bytes))

    @app.get("/api/jobs/{job_id}/log/download")
    def api_job_log_download(request: Request, job_id: str) -> Any:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if not job.log_path:
            raise HTTPException(status_code=404, detail="No log path for job")

        p = Path(str(job.log_path)).resolve()
        if not p.exists():
            raise HTTPException(status_code=404, detail="Log file not found")

        logs_root = _logs_dir(get_runs_dir()).resolve()
        if logs_root not in p.parents:
            raise HTTPException(status_code=400, detail="Invalid log path")

        return FileResponse(path=str(p), filename=p.name, media_type="text/plain")

    @app.get("/api/jobs/{job_id}/ingest_status", response_class=JSONResponse)
    def api_job_ingest_status(request: Request, job_id: str) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        from ghtrader.control.ingest_status import ingest_status_for_job

        status = ingest_status_for_job(
            job_id=job.id,
            command=job.command,
            log_path=job.log_path,
            default_data_dir=get_data_dir(),
        )
        # Attach minimal job metadata for UI convenience.
        status.update(
            {
                "job_status": job.status,
                "title": job.title,
                "source": job.source,
                "created_at": job.created_at,
                "started_at": job.started_at,
            }
        )
        return status

    @app.get("/api/ingest/status", response_class=JSONResponse)
    def api_ingest_status(request: Request, limit: int = 200) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        from ghtrader.control.ingest_status import ingest_status_for_job, parse_ingest_command

        jobs = store.list_jobs(limit=int(limit))
        out: list[dict[str, Any]] = []
        for job in jobs:
            if job.status not in {"queued", "running"}:
                continue
            kind = parse_ingest_command(job.command).get("kind")
            if kind not in {"download", "download_contract_range", "record"}:
                continue
            status = ingest_status_for_job(
                job_id=job.id,
                command=job.command,
                log_path=job.log_path,
                default_data_dir=get_data_dir(),
            )
            status.update(
                {
                    "job_status": job.status,
                    "title": job.title,
                    "source": job.source,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                }
            )
            out.append(status)
        return {"jobs": out}

    @app.post("/api/db/query", response_class=JSONResponse)
    async def api_db_query(request: Request) -> dict[str, Any]:
        """
        Read-only DuckDB query endpoint (guarded).

        Intended for local SSH-forwarded dashboard use only.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        query = str(payload.get("query") or "").strip()
        include_metrics = bool(payload.get("include_metrics") or False)
        try:
            limit = int(payload.get("limit") or 200)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        try:
            from ghtrader.db import DuckDBBackend, DuckDBConfig, DuckDBNotInstalled

            data_dir = get_data_dir()
            runs_dir = get_runs_dir()
            db_path = data_dir / "ghtrader.duckdb"

            # Prefer a prepared DB file; fall back to in-memory views.
            if db_path.exists():
                try:
                    backend = DuckDBBackend(config=DuckDBConfig(db_path=db_path, read_only=True))
                    with backend.connect() as con:
                        df = backend.query_df_limited(con=con, sql=query, limit=limit)
                    return {"columns": df.columns.tolist(), "rows": df.astype(str).to_dict(orient="records")}
                except Exception:
                    pass

            backend = DuckDBBackend(config=DuckDBConfig(db_path=None, read_only=False))
            with backend.connect() as con:
                backend.init_views(con=con, data_dir=data_dir)
                if include_metrics:
                    backend.ingest_runs_metrics(con=con, runs_dir=runs_dir)
                df = backend.query_df_limited(con=con, sql=query, limit=limit)
            return {"columns": df.columns.tolist(), "rows": df.astype(str).to_dict(orient="records")}
        except DuckDBNotInstalled as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    return app


app = create_app()

