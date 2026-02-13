"""Gateway-domain API routers extracted from app composition."""

from __future__ import annotations

import inspect
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

Handler = Callable[..., Any]


async def _invoke(handler: Handler, *args: Any, **kwargs: Any) -> Any:
    out = handler(*args, **kwargs)
    if inspect.isawaitable(out):
        return await out  # type: ignore[no-any-return]
    return out


def build_gateway_router(
    *,
    gateway_status_handler: Handler,
    gateway_list_handler: Handler,
    gateway_desired_handler: Handler,
    gateway_command_handler: Handler,
) -> APIRouter:
    router = APIRouter(tags=["gateway-api"])

    @router.get("/api/gateway/status", response_class=JSONResponse)
    async def api_gateway_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
        return await _invoke(gateway_status_handler, request, account_profile)

    @router.get("/api/gateway/list", response_class=JSONResponse)
    async def api_gateway_list(request: Request, limit: int = 200) -> dict[str, Any]:
        return await _invoke(gateway_list_handler, request, limit)

    @router.post("/api/gateway/desired", response_class=JSONResponse)
    async def api_gateway_desired(request: Request) -> dict[str, Any]:
        return await _invoke(gateway_desired_handler, request)

    @router.post("/api/gateway/command", response_class=JSONResponse)
    async def api_gateway_command(request: Request) -> dict[str, Any]:
        return await _invoke(gateway_command_handler, request)

    return router
