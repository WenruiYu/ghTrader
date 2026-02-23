from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir

RequireAuth = Callable[[Request], None]
TokenQueryString = Callable[[Request], str]
RequestVarietyHint = Callable[[Request], str]
VarietyNavCtx = Callable[..., dict[str, Any]]


def register_system_routes(
    *,
    router: APIRouter,
    templates: Jinja2Templates,
    require_auth: RequireAuth,
    token_qs: TokenQueryString,
    request_variety_hint: RequestVarietyHint,
    variety_nav_ctx: VarietyNavCtx,
) -> None:
    @router.get("/explorer")
    def explorer_page(request: Request):
        require_auth(request)
        nav_var = request_variety_hint(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        q = "SELECT count() AS n_ticks FROM ghtrader_ticks_main_l5_v2"
        return templates.TemplateResponse(
            request,
            "explorer.html",
            {
                "request": request,
                "title": "SQL Explorer",
                "token_qs": token_qs(request),
                "running_count": len(running),
                "query": q,
                "limit": 200,
                "columns": [],
                "rows": [],
                "error": "",
                **variety_nav_ctx(nav_var, section="sql", page_title="SQL Explorer"),
            },
        )

    @router.post("/explorer")
    async def explorer_run(request: Request):
        require_auth(request)
        nav_var = request_variety_hint(request)
        form = await request.form()
        query = str(form.get("query") or "").strip()
        limit_raw = str(form.get("limit") or "200").strip()

        try:
            limit = int(limit_raw)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        columns: list[str] = []
        rows: list[dict[str, str]] = []
        err = ""

        if not query:
            err = "Query is required."
        else:
            try:
                from ghtrader.questdb.client import make_questdb_query_config_from_env
                from ghtrader.questdb.queries import query_sql_read_only

                cfg = make_questdb_query_config_from_env()
                columns, rows = query_sql_read_only(cfg=cfg, query=query, limit=int(limit), connect_timeout_s=2)
            except Exception as e:
                err = str(e)

        return templates.TemplateResponse(
            request,
            "explorer.html",
            {
                "request": request,
                "title": "SQL Explorer",
                "token_qs": token_qs(request),
                "query": query,
                "limit": limit,
                "columns": columns,
                "rows": rows,
                "error": err,
                **variety_nav_ctx(nav_var, section="sql", page_title="SQL Explorer"),
            },
        )

    @router.get("/system")
    def system_page(request: Request):
        require_auth(request)
        nav_var = request_variety_hint(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()

        return templates.TemplateResponse(
            request,
            "system.html",
            {
                "request": request,
                "title": "System",
                "token_qs": token_qs(request),
                "running_count": len(running),
                "paths": [
                    {"key": "data", "path": str(data_dir), "exists": bool(data_dir.exists())},
                    {"key": "runs", "path": str(runs_dir), "exists": bool(runs_dir.exists())},
                    {"key": "artifacts", "path": str(artifacts_dir), "exists": bool(artifacts_dir.exists())},
                ],
                **variety_nav_ctx(nav_var, section="system", page_title="System"),
            },
        )
