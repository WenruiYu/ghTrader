"""Health and system routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth

router = APIRouter(tags=["health"])


@router.get("/health", response_class=JSONResponse)
def health() -> dict[str, Any]:
    return {"ok": True}


@router.get("/system", response_class=JSONResponse)
def api_system(request: Request, include_dir_sizes: bool = False, refresh: str = "none") -> dict[str, Any]:
    """
    Cached system snapshot for the dashboard System page.

    Query params:
    - include_dir_sizes: include cached directory sizes (may still be computing)
    - refresh: none|fast|dir
    """
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
