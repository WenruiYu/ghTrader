"""Accounts-domain API routers extracted from app composition."""

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


def build_accounts_router(
    *,
    accounts_handler: Handler,
    accounts_upsert_handler: Handler,
    accounts_delete_handler: Handler,
    brokers_handler: Handler,
    accounts_enqueue_verify_handler: Handler,
) -> APIRouter:
    router = APIRouter(tags=["accounts-api"])

    @router.get("/api/accounts", response_class=JSONResponse)
    async def api_accounts(request: Request) -> dict[str, Any]:
        return await _invoke(accounts_handler, request)

    @router.post("/api/accounts/upsert", response_class=JSONResponse)
    async def api_accounts_upsert(request: Request) -> dict[str, Any]:
        return await _invoke(accounts_upsert_handler, request)

    @router.post("/api/accounts/delete", response_class=JSONResponse)
    async def api_accounts_delete(request: Request) -> dict[str, Any]:
        return await _invoke(accounts_delete_handler, request)

    @router.get("/api/brokers", response_class=JSONResponse)
    async def api_brokers(request: Request) -> dict[str, Any]:
        return await _invoke(brokers_handler, request)

    @router.post("/api/accounts/enqueue-verify", response_class=JSONResponse)
    async def api_accounts_enqueue_verify(request: Request) -> dict[str, Any]:
        return await _invoke(accounts_enqueue_verify_handler, request)

    return router
