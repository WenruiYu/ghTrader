"""Models-domain API routes."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config import get_artifacts_dir, get_runs_dir
from ghtrader.control.app_helpers import (
    normalize_variety_for_api as _normalize_variety_for_api,
    scan_model_files as _scan_model_files,
)
from ghtrader.control import auth
from ghtrader.control.cache import TTLCacheSlot
from ghtrader.control.routes.query_budget import bounded_limit
from ghtrader.control.variety_context import symbol_matches_variety as _symbol_matches_variety

log = structlog.get_logger()
router = APIRouter(tags=["models-api"])

_BENCHMARKS_TTL_S = 10.0
_benchmarks_cache = TTLCacheSlot()


def _human_size(size_bytes: int) -> str:
    x = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if x < 1024:
            return f"{x:.1f} {unit}"
        x /= 1024
    return f"{x:.1f} TB"


@router.get("/api/models/inventory", response_class=JSONResponse)
def api_models_inventory(
    request: Request,
    include_temp: bool = False,
    max_rows: int = 500,
    var: str = "",
) -> dict[str, Any]:
    _ = include_temp
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    artifacts_dir = get_artifacts_dir()
    rows_limit = bounded_limit(max_rows, default=500, max_limit=2000)
    models: list[dict[str, Any]] = []
    var_filter = _normalize_variety_for_api(var) if str(var or "").strip() else ""

    try:
        if not artifacts_dir.exists():
            return {"ok": True, "models": []}

        files = _scan_model_files(artifacts_dir)
        files_sorted = sorted(files, key=lambda x: float(x.get("mtime") or 0.0), reverse=True)
        for f in files_sorted:
            try:
                mtime = float(f.get("mtime") or 0.0)
                created_at = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
                p = Path(str(f.get("path") or ""))
                rel = str(p.relative_to(artifacts_dir)) if artifacts_dir in p.parents else str(p.name)
                size_bytes = int(f.get("size_bytes") or 0)
                models.append(
                    {
                        "name": rel,
                        "model_type": str(f.get("model_type") or ""),
                        "symbol": str(f.get("symbol") or ""),
                        "horizon": int(f.get("horizon") or 0),
                        "namespace": str(f.get("namespace") or ""),
                        "created_at": created_at,
                        "size_bytes": size_bytes,
                        "size_human": _human_size(size_bytes),
                        "path": str(p),
                    }
                )
            except Exception:
                continue
    except Exception as e:
        log.warning("api_models_inventory.error", error=str(e))
        return {"ok": False, "models": [], "error": str(e)}

    if var_filter:
        models = [m for m in models if _symbol_matches_variety(str(m.get("symbol") or ""), var_filter)]
    models = models[:rows_limit]
    return {"ok": True, "models": models, "var": (var_filter or None)}


@router.get("/api/models/benchmarks", response_class=JSONResponse)
def api_models_benchmarks(request: Request, limit: int = 20, var: str = "") -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    var_filter = _normalize_variety_for_api(var) if str(var or "").strip() else ""
    lim = bounded_limit(limit, default=20, max_limit=200)
    runs_dir = get_runs_dir()
    root = runs_dir / "benchmarks"
    cache_root = str(root.resolve()) if root.exists() else str(root)
    now = time.time()
    cached = _benchmarks_cache.get(ttl_s=_BENCHMARKS_TTL_S, now=now)
    if isinstance(cached, dict):
        payload = dict(cached)
        if str(payload.get("_cache_root") or "") != cache_root:
            payload = {}
        if payload:
            rows = list(payload.get("benchmarks") or [])
            if var_filter:
                rows = [r for r in rows if _symbol_matches_variety(str((r or {}).get("symbol") or ""), var_filter)]
            payload["benchmarks"] = rows[:lim]
            payload["var"] = var_filter or None
            payload.pop("_cache_root", None)
            return payload

    if not root.exists():
        return {"ok": True, "benchmarks": [], "var": var_filter or None}

    out: list[dict[str, Any]] = []
    try:
        for p in root.rglob("*.json"):
            try:
                if not p.is_file():
                    continue
                rel = p.relative_to(runs_dir).as_posix()
                st = p.stat()
                created_at = datetime.fromtimestamp(float(st.st_mtime), tz=timezone.utc).isoformat()
                payload = json.loads(p.read_text(encoding="utf-8"))
                offline = payload.get("offline") if isinstance(payload.get("offline"), dict) else {}
                latency = payload.get("latency") if isinstance(payload.get("latency"), dict) else {}
                out.append(
                    {
                        "run_id": str(payload.get("run_id") or p.stem),
                        "timestamp": str(payload.get("timestamp") or ""),
                        "model_type": str(payload.get("model_type") or ""),
                        "symbol": str(payload.get("symbol") or ""),
                        "horizon": int(payload.get("horizon") or 0),
                        "accuracy": offline.get("accuracy"),
                        "f1_macro": offline.get("f1_macro"),
                        "log_loss": offline.get("log_loss"),
                        "ece": offline.get("ece"),
                        "inference_p95_ms": latency.get("inference_p95_ms"),
                        "created_at": created_at,
                        "path": rel,
                    }
                )
            except Exception:
                continue
    except Exception as e:
        return {"ok": False, "benchmarks": [], "error": str(e)}

    out = sorted(out, key=lambda x: (str(x.get("timestamp") or ""), str(x.get("created_at") or "")), reverse=True)
    payload2 = {"ok": True, "benchmarks": out, "_cache_root": cache_root}
    _benchmarks_cache.set(dict(payload2), now=time.time())
    rows = list(out)
    if var_filter:
        rows = [r for r in rows if _symbol_matches_variety(str((r or {}).get("symbol") or ""), var_filter)]
    return {"ok": True, "benchmarks": rows[:lim], "var": var_filter or None}
