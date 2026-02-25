from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates

RequireAuth = Callable[[Request], None]
TokenQueryString = Callable[[Request], str]
RequestVarietyHint = Callable[[Request], str]
VarietyNavCtx = Callable[..., dict[str, Any]]


def register_job_detail_routes(
    *,
    router: APIRouter,
    templates: Jinja2Templates,
    require_auth: RequireAuth,
    token_qs: TokenQueryString,
    request_variety_hint: RequestVarietyHint,
    variety_nav_ctx: VarietyNavCtx,
) -> None:
    @router.get("/jobs/{job_id}")
    def job_detail(request: Request, job_id: str):
        require_auth(request)
        nav_var = request_variety_hint(request)
        store = request.app.state.job_store
        jm = request.app.state.job_manager
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        log_text = jm.read_log_tail(job_id)
        return templates.TemplateResponse(
            request,
            "job_detail.html",
            {
                "request": request,
                "title": f"Job {job_id}",
                "token_qs": token_qs(request),
                "job": job,
                "log_text": log_text,
                **variety_nav_ctx(
                    nav_var,
                    section="jobs",
                    page_title=f"Job {job_id}",
                    breadcrumbs=[
                        {"label": "Workspace", "href": f"/v/{nav_var}/dashboard"},
                        {"label": "Jobs", "href": f"/v/{nav_var}/jobs{token_qs(request)}"},
                        {"label": f"Job {job_id}", "href": ""},
                    ],
                ),
            },
        )
