"""Core health/system API routes."""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config_service import get_config_resolver
from ghtrader.config_service.schema import (
    coerce_value_for_key,
    key_is_managed,
    key_is_ui_editable,
    list_schema_descriptors,
)
from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.slo import collect_slo_snapshot
from ghtrader.util.observability import snapshot_all

router = APIRouter(tags=["core"])


@router.get("/api/system", response_class=JSONResponse)
def api_system(request: Request, include_dir_sizes: bool = False, refresh: str = "none") -> dict[str, Any]:
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


@router.get("/api/questdb/metrics", response_class=JSONResponse)
def api_questdb_metrics(request: Request, refresh: bool = False) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from ghtrader.control.system_info import questdb_metrics_snapshot

    return questdb_metrics_snapshot(refresh=bool(refresh))


@router.get("/api/observability/slo", response_class=JSONResponse)
def api_observability_slo(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    store = getattr(request.app.state, "job_store", None)
    return collect_slo_snapshot(store=store)


@router.get("/api/observability/runtime", response_class=JSONResponse)
def api_observability_runtime(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {
        "ok": True,
        "domains": snapshot_all(),
    }


def _config_actor(request: Request, payload: dict[str, Any] | None = None) -> str:
    if isinstance(payload, dict):
        actor = str(payload.get("actor") or "").strip()
        if actor:
            return actor
    hdr = str(request.headers.get("x-ghtrader-actor") or "").strip()
    if hdr:
        return hdr
    client = request.client.host if request.client else "unknown"
    return f"api:{client}"


def _config_preview_items(*, resolver: Any, prefix: str = "") -> list[dict[str, Any]]:
    snap = resolver.snapshot()
    pfx = str(prefix or "").strip()
    schema_rows = list_schema_descriptors(snapshot_keys=list(snap.keys()))
    out: list[dict[str, Any]] = []
    for row in schema_rows:
        key = str(row.get("key") or "")
        if not key:
            continue
        if pfx and not key.startswith(pfx):
            continue
        raw, src = resolver.get_raw_with_source(key, None)
        val = raw
        source = str(src)
        if raw is None and row.get("default") is not None:
            val = row.get("default")
            source = "default"
        if raw is None and row.get("default") is None:
            val = ""
            source = "unset"
        out.append(
            {
                **row,
                "value": str(val if val is not None else ""),
                "source": source,
            }
        )
    return out


@router.get("/api/config/effective", response_class=JSONResponse)
def api_config_effective(request: Request, prefix: str = "") -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    resolver = get_config_resolver()
    snap = resolver.snapshot()
    pfx = str(prefix or "").strip()
    if pfx:
        snap = {k: v for k, v in snap.items() if k.startswith(pfx)}
    return {
        "ok": True,
        "revision": resolver.revision,
        "config_hash": resolver.snapshot_hash,
        "values": snap,
    }


@router.get("/api/config/effective-preview", response_class=JSONResponse)
def api_config_effective_preview(request: Request, prefix: str = "") -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    resolver = get_config_resolver()
    return {
        "ok": True,
        "revision": resolver.revision,
        "config_hash": resolver.snapshot_hash,
        "items": _config_preview_items(resolver=resolver, prefix=prefix),
    }


@router.get("/api/config/schema", response_class=JSONResponse)
def api_config_schema(request: Request, prefix: str = "", editable_only: bool = False) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    resolver = get_config_resolver()
    pfx = str(prefix or "").strip()
    rows = list_schema_descriptors(snapshot_keys=list(resolver.snapshot().keys()))
    if pfx:
        rows = [r for r in rows if str(r.get("key") or "").startswith(pfx)]
    if editable_only:
        rows = [r for r in rows if bool(r.get("editable"))]
    return {
        "ok": True,
        "items": rows,
    }


@router.get("/api/config/history", response_class=JSONResponse)
def api_config_history(request: Request, limit: int = 50) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    resolver = get_config_resolver()
    rows = resolver.list_revisions(limit=max(1, min(int(limit), 500)))
    return {
        "ok": True,
        "items": [
            {
                "revision": int(r.revision),
                "created_at": str(r.created_at),
                "actor": str(r.actor),
                "reason": str(r.reason),
                "config_hash": str(r.snapshot_hash),
                "changed_keys": list(r.changed_keys),
            }
            for r in rows
        ],
    }


@router.post("/api/config/set", response_class=JSONResponse)
async def api_config_set(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_payload")
    values = payload.get("values")
    if not isinstance(values, dict) or not values:
        raise HTTPException(status_code=400, detail="values_required")
    value_types = payload.get("types")
    if value_types is not None and not isinstance(value_types, dict):
        raise HTTPException(status_code=400, detail="types_must_be_object")
    reason = str(payload.get("reason") or "").strip() or "api_set"
    actor = _config_actor(request, payload)

    issues: list[dict[str, str]] = []
    coerced: dict[str, Any] = {}
    for key_raw, val_raw in values.items():
        key = str(key_raw or "").strip()
        if not key:
            continue
        if not key_is_managed(key):
            issues.append({"key": key, "error": "key_not_managed"})
            continue
        if not key_is_ui_editable(key):
            issues.append({"key": key, "error": "key_not_editable"})
            continue
        type_override = None
        if isinstance(value_types, dict):
            tv = value_types.get(key)
            if tv is not None and str(tv).strip():
                type_override = str(tv).strip().lower()
        try:
            coerced[key] = coerce_value_for_key(key, val_raw, value_type_override=type_override)
        except Exception as e:
            issues.append({"key": key, "error": str(e)})
    if issues:
        raise HTTPException(status_code=400, detail={"error": "invalid_config_values", "issues": issues})
    if not coerced:
        raise HTTPException(status_code=400, detail="no_valid_values")

    resolver = get_config_resolver()
    try:
        rev = resolver.set_values(
            values=coerced,
            actor=actor,
            reason=reason,
            action="set",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": "invalid_config_values", "issues": [{"key": "*", "error": str(e)}]}) from e
    return {
        "ok": True,
        "actor": actor,
        "reason": reason,
        "revision": int(rev.revision),
        "config_hash": str(rev.snapshot_hash),
        "changed_keys": list(rev.changed_keys),
    }


@router.post("/api/config/rollback", response_class=JSONResponse)
async def api_config_rollback(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_payload")
    try:
        revision = int(payload.get("revision"))
    except Exception as e:
        raise HTTPException(status_code=400, detail="invalid_revision") from e
    reason = str(payload.get("reason") or "").strip() or f"rollback_to:{revision}"
    actor = _config_actor(request, payload)
    resolver = get_config_resolver()
    try:
        rev = resolver.rollback(
            revision=int(revision),
            actor=actor,
            reason=reason,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {
        "ok": True,
        "actor": actor,
        "reason": reason,
        "revision": int(rev.revision),
        "config_hash": str(rev.snapshot_hash),
        "changed_keys": list(rev.changed_keys),
        "rollback_to": int(revision),
    }


@router.post("/api/config/migrate-env", response_class=JSONResponse)
async def api_config_migrate_env(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_payload")
    reason = str(payload.get("reason") or "").strip() or "api_migrate_env"
    resolver = get_config_resolver()
    rev = resolver.store.migrate_from_env(
        env={str(k): str(v if v is not None else "") for k, v in os.environ.items()},
        actor=_config_actor(request, payload),
        reason=reason,
    )
    resolver.refresh(force=True)
    return {
        "ok": True,
        "revision": int(rev.revision),
        "config_hash": str(rev.snapshot_hash),
        "changed_keys": list(rev.changed_keys),
        "migrated_count": int(len(rev.changed_keys)),
    }
