"""Health and system routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_class=JSONResponse)
def health() -> dict[str, Any]:
    return {"ok": True}
