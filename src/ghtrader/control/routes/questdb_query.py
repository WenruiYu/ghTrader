from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.control.routes.query_budget import bounded_limit


def mount_questdb_query_route(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
) -> None:
    @app.post("/api/questdb/query", response_class=JSONResponse)
    async def api_questdb_query(request: Request) -> dict[str, Any]:
        """
        Read-only QuestDB query endpoint (guarded).

        Intended for local SSH-forwarded dashboard use only.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        query = str(payload.get("query") or "").strip()
        limit = bounded_limit(payload.get("limit"), default=200, max_limit=500)

        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.queries import query_sql_read_only

            cfg = make_questdb_query_config_from_env()
            cols, rows = query_sql_read_only(cfg=cfg, query=query, limit=limit, connect_timeout_s=2)
            return {"ok": True, "columns": cols, "rows": rows}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except RuntimeError as e:
            # Missing dependency / config issues.
            raise HTTPException(status_code=500, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
