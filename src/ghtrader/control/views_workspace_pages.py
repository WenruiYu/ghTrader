from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_data_dir
from ghtrader.control.views_helpers import safe_int as _safe_int

RequireAuth = Callable[[Request], None]
TokenQueryString = Callable[[Request], str]
VarPage = Callable[..., str]
PageVariety = Callable[[str | None], str]
JobSummary = Callable[..., dict[str, Any]]
JobMatchesVariety = Callable[[Any, str], bool]
VarietyNavCtx = Callable[..., dict[str, Any]]
L5CalendarContext = Callable[..., dict[str, str]]
PipelineGuardrailsContext = Callable[[], dict[str, Any]]


def register_workspace_page_routes(
    *,
    router: APIRouter,
    templates: Jinja2Templates,
    require_auth: RequireAuth,
    token_qs: TokenQueryString,
    var_page: VarPage,
    page_variety: PageVariety,
    job_summary: JobSummary,
    job_matches_variety: JobMatchesVariety,
    variety_nav_ctx: VarietyNavCtx,
    l5_calendar_context: L5CalendarContext,
    pipeline_guardrails_context: PipelineGuardrailsContext,
) -> None:
    @router.get("/")
    def index_redirect(request: Request):
        require_auth(request)
        return RedirectResponse(url=var_page("dashboard", token_qs=token_qs(request)), status_code=303)

    @router.get("/v/{variety}/dashboard")
    def index(request: Request, variety: str):
        require_auth(request)
        v = page_variety(variety)
        summary = job_summary(request=request, limit=50)
        jobs_filtered = [j for j in list(summary["jobs"]) if job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if str(j.status) == "running"])
        queued_count = len([j for j in jobs_filtered if str(j.status) == "queued"])
        data_dir = get_data_dir()
        calendar_ctx = l5_calendar_context(data_dir=data_dir, variety=v)
        guardrails = pipeline_guardrails_context()

        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "title": "ghTrader Dashboard",
                "token_qs": token_qs(request),
                "jobs": jobs_filtered,
                "running_count": running_count,
                "queued_count": queued_count,
                "recent_count": len(jobs_filtered),
                "guardrails": guardrails,
                **calendar_ctx,
                **variety_nav_ctx(v, section="dashboard", page_title="Dashboard"),
            },
        )

    @router.get("/jobs")
    def jobs_redirect(request: Request):
        require_auth(request)
        return RedirectResponse(url=var_page("jobs", token_qs=token_qs(request)), status_code=303)

    @router.get("/v/{variety}/jobs")
    def jobs(request: Request, variety: str):
        require_auth(request)
        v = page_variety(variety)
        summary = job_summary(request=request, limit=200)
        jobs_scope = str(request.query_params.get("scope") or "var").strip().lower()
        page = _safe_int(request.query_params.get("page"), 1, min_value=1)
        per_page = _safe_int(request.query_params.get("per_page"), 50, min_value=20, max_value=200)
        show_all = jobs_scope == "all"
        jobs_all = list(summary["jobs"])
        jobs_filtered_all = jobs_all if show_all else [j for j in jobs_all if job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered_all if str(j.status) == "running"])
        status_counts = {
            "running": len([j for j in jobs_filtered_all if str(j.status) == "running"]),
            "queued": len([j for j in jobs_filtered_all if str(j.status) == "queued"]),
            "succeeded": len([j for j in jobs_filtered_all if str(j.status) == "succeeded"]),
            "failed": len([j for j in jobs_filtered_all if str(j.status) == "failed"]),
            "cancelled": len([j for j in jobs_filtered_all if str(j.status) == "cancelled"]),
        }
        total_jobs = len(jobs_filtered_all)
        total_pages = max(1, (total_jobs + per_page - 1) // per_page)
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * per_page
        jobs_filtered = jobs_filtered_all[offset : offset + per_page]
        data_dir = get_data_dir()
        calendar_ctx = l5_calendar_context(data_dir=data_dir, variety=v)
        guardrails = pipeline_guardrails_context()

        return templates.TemplateResponse(
            request,
            "jobs.html",
            {
                "request": request,
                "title": "Jobs",
                "token_qs": token_qs(request),
                "jobs": jobs_filtered,
                "running_count": running_count,
                "jobs_status_counts": status_counts,
                "jobs_scope": "all" if show_all else "var",
                "jobs_pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total_jobs,
                    "total_pages": total_pages,
                    "has_prev": page > 1,
                    "has_next": page < total_pages,
                    "prev_page": max(1, page - 1),
                    "next_page": min(total_pages, page + 1),
                },
                "guardrails": guardrails,
                **calendar_ctx,
                **variety_nav_ctx(v, section="jobs", page_title="Jobs"),
            },
        )

    @router.get("/models")
    def models_redirect(request: Request):
        require_auth(request)
        return RedirectResponse(url=var_page("models", token_qs=token_qs(request)), status_code=303)

    @router.get("/v/{variety}/models")
    def models_page(request: Request, variety: str):
        require_auth(request)
        v = page_variety(variety)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running" and job_matches_variety(j, v)]
        return templates.TemplateResponse(
            request,
            "models.html",
            {
                "request": request,
                "title": "Models",
                "token_qs": token_qs(request),
                "running_count": len(running),
                **variety_nav_ctx(v, section="models", page_title="Models"),
            },
        )

    @router.get("/trading")
    def trading_redirect(request: Request):
        require_auth(request)
        return RedirectResponse(url=var_page("trading", token_qs=token_qs(request)), status_code=303)

    @router.get("/v/{variety}/trading")
    def trading_page(request: Request, variety: str):
        require_auth(request)
        v = page_variety(variety)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running" and job_matches_variety(j, v)]

        return templates.TemplateResponse(
            request,
            "trading.html",
            {
                "request": request,
                "title": "Trading",
                "token_qs": token_qs(request),
                "running_count": len(running),
                **variety_nav_ctx(v, section="trading", page_title="Trading"),
            },
        )
