from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

RequireAuth = Callable[[Request], None]
TokenQueryString = Callable[[Request], str]
PageVariety = Callable[[str | None], str]
VarietyNavCtx = Callable[..., dict[str, Any]]
VarPage = Callable[..., str]
JobSummary = Callable[..., dict[str, Any]]
JobMatchesVariety = Callable[[Any, str], bool]


def register_config_routes(
    *,
    router: APIRouter,
    templates: Jinja2Templates,
    require_auth: RequireAuth,
    token_qs: TokenQueryString,
    page_variety: PageVariety,
    variety_nav_ctx: VarietyNavCtx,
    var_page: VarPage,
    job_summary: JobSummary,
    job_matches_variety: JobMatchesVariety,
) -> None:
    @router.get("/config")
    def config_redirect(request: Request):
        require_auth(request)
        return RedirectResponse(url=var_page("config", token_qs=token_qs(request)), status_code=303)

    @router.get("/v/{variety}/config")
    def config_page(request: Request, variety: str):
        require_auth(request)
        v = page_variety(variety)
        summary = job_summary(request=request, limit=120)
        jobs_filtered = [j for j in list(summary["jobs"]) if job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if str(j.status) == "running"])
        queued_count = len([j for j in jobs_filtered if str(j.status) == "queued"])
        return templates.TemplateResponse(
            request,
            "config.html",
            {
                "request": request,
                "title": "Configuration",
                "token_qs": token_qs(request),
                "running_count": running_count,
                "queued_count": queued_count,
                **variety_nav_ctx(v, section="config", page_title="Configuration"),
            },
        )
