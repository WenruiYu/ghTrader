from __future__ import annotations

import time
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_data_dir, get_runs_dir
from ghtrader.control.cache import TTLCacheMap
from ghtrader.control.views_data_page_orchestrator import (
    resolve_data_page_snapshot as _resolve_data_page_snapshot,
)
from ghtrader.control.settings import get_tqsdk_scheduler_state

_DATA_PAGE_CACHE_TTL_S = 5.0
_DATA_PAGE_CACHE = TTLCacheMap()


RequireAuth = Callable[[Request], None]
TokenQueryString = Callable[[Request], str]
VarPage = Callable[..., str]
PageVariety = Callable[[str | None], str]
JobSummary = Callable[..., dict[str, Any]]
JobMatchesVariety = Callable[[Any, str], bool]
VarietyNavCtx = Callable[..., dict[str, Any]]
L5CalendarContext = Callable[..., dict[str, str]]
PipelineGuardrailsContext = Callable[[], dict[str, Any]]
PipelineHealthContext = Callable[..., dict[str, dict[str, Any]]]
ValidationProfileSuggestion = Callable[[str], dict[str, Any]]


def _data_page_cache_get(key: str, *, ttl_s: float | None = None) -> Any | None:
    ttl = float(_DATA_PAGE_CACHE_TTL_S if ttl_s is None else ttl_s)
    return _DATA_PAGE_CACHE.get(str(key), ttl_s=ttl, now=time.time())


def _data_page_cache_set(key: str, payload: Any) -> None:
    _DATA_PAGE_CACHE.set(str(key), payload, now=time.time())


def clear_data_page_cache() -> None:
    _DATA_PAGE_CACHE.clear()


def register_data_page_routes(
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
    pipeline_health_context: PipelineHealthContext,
    validation_profile_suggestion: ValidationProfileSuggestion,
) -> None:
    @router.get("/data")
    def data_redirect(request: Request):
        require_auth(request)
        return RedirectResponse(url=var_page("data", token_qs=token_qs(request)), status_code=303)

    @router.get("/v/{variety}/data")
    def data_page(request: Request, variety: str):
        require_auth(request)
        v = page_variety(variety)
        summary = job_summary(request=request, limit=200)
        jobs_filtered = [j for j in list(summary["jobs"]) if job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if str(j.status) == "running"])
        queued_count = len([j for j in jobs_filtered if str(j.status) == "queued"])
        runs_dir = get_runs_dir()

        data_dir = get_data_dir()
        calendar_ctx = l5_calendar_context(data_dir=data_dir, variety=v)
        lv = "v2"

        # Locks
        locks = []
        try:
            from ghtrader.control.locks import LockStore

            locks = LockStore(runs_dir / "control" / "jobs.db").list_locks()
        except Exception:
            locks = []

        snapshot = _resolve_data_page_snapshot(
            runs_dir=runs_dir,
            coverage_var=v,
            cache_get=_data_page_cache_get,
            cache_set=_data_page_cache_set,
        )
        reports = list(snapshot.get("reports") or [])
        questdb = dict(snapshot.get("questdb") or {"ok": False, "error": "questdb unavailable"})
        coverage_var = str(snapshot.get("coverage_var") or v)
        coverage_symbol = str(snapshot.get("coverage_symbol") or f"KQ.m@SHFE.{coverage_var}")
        main_schedule_coverage = dict(snapshot.get("main_schedule_coverage") or {})
        main_l5_coverage = dict(snapshot.get("main_l5_coverage") or {})
        coverage_error = str(snapshot.get("coverage_error") or "")
        main_l5_validation = dict(snapshot.get("main_l5_validation") or {})
        validation_error = str(snapshot.get("validation_error") or "")

        # TqSdk scheduler settings (max parallel heavy jobs)
        tqsdk_scheduler = get_tqsdk_scheduler_state(runs_dir=runs_dir)
        guardrails = pipeline_guardrails_context()
        pipeline_health = pipeline_health_context(
            main_schedule_coverage=main_schedule_coverage,
            main_l5_coverage=main_l5_coverage,
            main_l5_validation=main_l5_validation,
            coverage_error=coverage_error,
            validation_error=validation_error,
        )

        # Coverage lists can get very large; keep /data page load fast by default.
        # The Contracts tab will lazy-load any detailed coverage via API.
        return templates.TemplateResponse(
            request,
            "data.html",
            {
                "request": request,
                "title": "Data Hub",
                "token_qs": token_qs(request),
                "running_count": running_count,
                "queued_count": queued_count,
                "dataset_version": lv,
                "ticks_v2": [],
                "main_l5_v2": [],
                "features": [],
                "labels": [],
                "questdb": questdb,
                "locks": locks,
                "reports": reports,
                **calendar_ctx,
                "tqsdk_scheduler": tqsdk_scheduler,
                "coverage_var": coverage_var,
                "coverage_symbol": coverage_symbol,
                "main_schedule_coverage": main_schedule_coverage,
                "main_l5_coverage": main_l5_coverage,
                "coverage_error": coverage_error,
                "main_l5_validation": main_l5_validation,
                "validation_error": validation_error,
                "guardrails": guardrails,
                "pipeline_health": pipeline_health,
                "validation_profile_suggestion": validation_profile_suggestion(v),
                **variety_nav_ctx(v, section="data", page_title="Data Hub"),
            },
        )
