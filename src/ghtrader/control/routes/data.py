"""Data-domain API routers extracted from app composition."""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

Handler = Callable[..., Any]


async def _invoke(handler: Handler, *args: Any, **kwargs: Any) -> Any:
    out = handler(*args, **kwargs)
    if inspect.isawaitable(out):
        return await out  # type: ignore[no-any-return]
    return out


def build_data_router(
    *,
    data_coverage_handler: Handler,
    data_coverage_summary_handler: Handler,
    data_quality_detail_handler: Handler,
    data_diagnose_handler: Handler,
    data_repair_handler: Handler,
    data_health_handler: Handler,
    contracts_explorer_handler: Handler,
    contracts_fill_handler: Handler,
    contracts_update_handler: Handler,
    contracts_audit_handler: Handler,
) -> APIRouter:
    router = APIRouter(tags=["data-api"])

    @router.get("/api/data/coverage", response_class=JSONResponse)
    async def api_data_coverage(
        request: Request, kind: str = "ticks", limit: int = 200, search: str = "", refresh: bool = False
    ) -> dict[str, Any]:
        return await _invoke(data_coverage_handler, request, kind, limit, search, refresh)

    @router.get("/api/data/coverage/summary", response_class=JSONResponse)
    async def api_data_coverage_summary(request: Request, refresh: bool = False) -> dict[str, Any]:
        return await _invoke(data_coverage_summary_handler, request, refresh)

    @router.get("/api/data/quality-detail/{symbol}", response_class=JSONResponse)
    async def api_data_quality_detail(request: Request, symbol: str, limit: int = 30) -> dict[str, Any]:
        return await _invoke(data_quality_detail_handler, request, symbol, limit)

    @router.post("/api/data/diagnose", response_class=JSONResponse)
    async def api_data_diagnose(request: Request) -> dict[str, Any]:
        return await _invoke(data_diagnose_handler, request)

    @router.post("/api/data/repair", response_class=JSONResponse)
    async def api_data_repair(request: Request) -> dict[str, Any]:
        return await _invoke(data_repair_handler, request)

    @router.post("/api/data/health", response_class=JSONResponse)
    async def api_data_health(request: Request) -> dict[str, Any]:
        return await _invoke(data_health_handler, request)

    @router.get("/api/contracts", response_class=JSONResponse)
    async def api_contracts_explorer(
        request: Request, exchange: str = "SHFE", var: str = "cu", refresh: bool = False
    ) -> dict[str, Any]:
        return await _invoke(contracts_explorer_handler, request, exchange, var, refresh)

    @router.post("/api/contracts/enqueue-fill", response_class=JSONResponse)
    async def api_contracts_fill(request: Request) -> dict[str, Any]:
        return await _invoke(contracts_fill_handler, request)

    @router.post("/api/contracts/enqueue-update", response_class=JSONResponse)
    async def api_contracts_update(request: Request) -> dict[str, Any]:
        return await _invoke(contracts_update_handler, request)

    @router.post("/api/contracts/enqueue-audit", response_class=JSONResponse)
    async def api_contracts_audit(request: Request) -> dict[str, Any]:
        return await _invoke(contracts_audit_handler, request)

    return router
