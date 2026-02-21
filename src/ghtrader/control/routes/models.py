"""Models-domain API routers extracted from app composition."""

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


def build_models_router(*, models_inventory_handler: Handler, models_benchmarks_handler: Handler) -> APIRouter:
    router = APIRouter(tags=["models-api"])

    @router.get("/api/models/inventory", response_class=JSONResponse)
    async def api_models_inventory(
        request: Request, include_temp: bool = False, max_rows: int = 500, var: str = ""
    ) -> dict[str, Any]:
        _ = (include_temp, max_rows)
        return await _invoke(models_inventory_handler, request, var)

    @router.get("/api/models/benchmarks", response_class=JSONResponse)
    async def api_models_benchmarks(request: Request, limit: int = 20, var: str = "") -> dict[str, Any]:
        return await _invoke(models_benchmarks_handler, request, limit, var)

    return router
