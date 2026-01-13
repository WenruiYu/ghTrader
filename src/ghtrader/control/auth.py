from __future__ import annotations

import os
from typing import Any


def auth_enabled() -> bool:
    return bool(os.environ.get("GHTRADER_DASHBOARD_TOKEN"))


def is_authorized(request: Any) -> bool:
    """
    Optional token auth (defense-in-depth).

    If GHTRADER_DASHBOARD_TOKEN is unset, auth is disabled.
    """
    token = os.environ.get("GHTRADER_DASHBOARD_TOKEN")
    if not token:
        return True

    hdr = request.headers.get("x-auth-token")
    if hdr and hdr == token:
        return True

    q = request.query_params.get("token")
    return bool(q and q == token)


def token_query_string(request: Any) -> str:
    """Return '?token=...' if token is provided via query param, else ''."""
    q = request.query_params.get("token")
    return f"?token={q}" if q else ""

