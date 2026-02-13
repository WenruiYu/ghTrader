"""Core health/system API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.ops_compat import ops_compat_contract
from ghtrader.control.slo import collect_slo_snapshot

router = APIRouter(tags=["core"])


@router.get("/health", response_class=JSONResponse)
def health() -> dict[str, Any]:
    return {"ok": True}


@router.get("/api/system", response_class=JSONResponse)
def api_system(request: Request, include_dir_sizes: bool = False, refresh: str = "none") -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from ghtrader.control.system_info import system_snapshot

    return system_snapshot(
        data_dir=get_data_dir(),
        runs_dir=get_runs_dir(),
        artifacts_dir=get_artifacts_dir(),
        include_dir_sizes=bool(include_dir_sizes),
        refresh=str(refresh or "none"),
    )


@router.get("/api/questdb/metrics", response_class=JSONResponse)
def api_questdb_metrics(request: Request, refresh: bool = False) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from ghtrader.control.system_info import questdb_metrics_snapshot

    return questdb_metrics_snapshot(refresh=bool(refresh))


@router.get("/api/ops/compat", response_class=JSONResponse)
def api_ops_compat(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return ops_compat_contract()


@router.get("/api/observability/slo", response_class=JSONResponse)
def api_observability_slo(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    store = getattr(request.app.state, "job_store", None)
    return collect_slo_snapshot(store=store)
