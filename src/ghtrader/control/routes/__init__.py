"""
API route modules for the ghTrader control dashboard.

Each module defines an APIRouter that is mounted in app.py.
"""

from __future__ import annotations

from fastapi import APIRouter

from .core import router as core_router
from .health import router as health_router
from .jobs import router as jobs_router

# Note: accounts API is implemented directly in app.py (PRD-aligned TQ_* env vars).


def build_api_router() -> APIRouter:
    """Build combined API router from all sub-routers."""
    api = APIRouter(prefix="/api")
    api.include_router(jobs_router)
    return api


def build_root_router() -> APIRouter:
    """Build router for root-level routes (like /health)."""
    root = APIRouter()
    root.include_router(core_router)
    root.include_router(health_router)
    return root
