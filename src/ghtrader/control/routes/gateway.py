"""Gateway-domain API routes."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import redis.asyncio as redis
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config import get_runs_dir
from ghtrader.control import auth
from ghtrader.control.state_helpers import (
    read_json_file as _read_json_file,
    read_redis_json as _read_redis_json,
)
from ghtrader.util.time import now_iso as _now_iso

router = APIRouter(tags=["gateway-api"])


def _canonical_account_profile(account_profile: str) -> str:
    try:
        from ghtrader.tq.runtime import canonical_account_profile

        return canonical_account_profile(str(account_profile or "default"))
    except Exception:
        return str(account_profile or "default").strip() or "default"


def gateway_status_payload(*, account_profile: str) -> dict[str, Any]:
    runs_dir = get_runs_dir()
    prof = _canonical_account_profile(account_profile)
    root = runs_dir / "gateway" / f"account={prof}"
    st = _read_redis_json(f"ghtrader:gateway:state:{prof}") or (_read_json_file(root / "state.json") if root.exists() else None)
    desired = _read_redis_json(f"ghtrader:gateway:desired:{prof}") or (_read_json_file(root / "desired.json") if root.exists() else None)
    targets = _read_json_file(root / "targets.json") if root.exists() else None
    return {
        "ok": True,
        "account_profile": prof,
        "root": str(root),
        "exists": bool(root.exists()),
        "state": st,
        "desired": desired,
        "targets": targets,
        "generated_at": _now_iso(),
    }


def gateway_list_payload(*, limit: int) -> dict[str, Any]:
    runs_dir = get_runs_dir()
    root = runs_dir / "gateway"
    lim = max(1, min(int(limit or 200), 500))
    rows: list[dict[str, Any]] = []
    try:
        if root.exists():
            for d in sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("account=")], key=lambda x: x.name)[:lim]:
                prof = d.name.split("=", 1)[-1] if "=" in d.name else d.name
                st = _read_redis_json(f"ghtrader:gateway:state:{prof}") or (_read_json_file(d / "state.json") if (d / "state.json").exists() else None)
                desired = _read_redis_json(f"ghtrader:gateway:desired:{prof}") or (_read_json_file(d / "desired.json") if (d / "desired.json").exists() else None)
                health = st.get("health") if isinstance(st, dict) and isinstance(st.get("health"), dict) else {}
                effective = st.get("effective") if isinstance(st, dict) and isinstance(st.get("effective"), dict) else {}
                rows.append(
                    {
                        "account_profile": str(prof),
                        "root": str(d),
                        "health": health,
                        "effective": effective,
                        "desired": desired.get("desired") if isinstance(desired, dict) else desired,
                        "updated_at": (st or {}).get("updated_at") if isinstance(st, dict) else None,
                    }
                )
    except Exception as e:
        return {"ok": False, "error": str(e), "profiles": [], "generated_at": _now_iso()}

    return {"ok": True, "profiles": rows, "generated_at": _now_iso()}


@router.get("/api/gateway/status", response_class=JSONResponse)
def api_gateway_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return gateway_status_payload(account_profile=account_profile)


@router.get("/api/gateway/list", response_class=JSONResponse)
def api_gateway_list(request: Request, limit: int = 200) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return gateway_list_payload(limit=limit)


@router.post("/api/gateway/desired", response_class=JSONResponse)
async def api_gateway_desired(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    ap = str(payload.get("account_profile") or "default").strip() or "default"
    desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else {}

    try:
        from ghtrader.tq.gateway import GatewayDesired, write_gateway_desired
        from ghtrader.tq.runtime import canonical_account_profile

        prof = canonical_account_profile(ap)
        d = GatewayDesired(
            mode=str(desired_in.get("mode") or "idle").strip(),  # type: ignore[arg-type]
            symbols=list(desired_in.get("symbols")) if isinstance(desired_in.get("symbols"), list) else None,
            executor=str(desired_in.get("executor") or "targetpos").strip().lower() in {"direct"} and "direct" or "targetpos",  # type: ignore[arg-type]
            sim_account=str(desired_in.get("sim_account") or "tqsim").strip().lower() in {"tqkq"} and "tqkq" or "tqsim",  # type: ignore[arg-type]
            confirm_live=str(desired_in.get("confirm_live") or "").strip(),
            max_abs_position=max(0, int(desired_in.get("max_abs_position") or 1)),
            max_order_size=max(1, int(desired_in.get("max_order_size") or 1)),
            max_ops_per_sec=max(1, int(desired_in.get("max_ops_per_sec") or 10)),
            max_daily_loss=(float(desired_in.get("max_daily_loss")) if str(desired_in.get("max_daily_loss") or "").strip() else None),
            enforce_trading_time=bool(desired_in.get("enforce_trading_time")) if ("enforce_trading_time" in desired_in) else True,
        )
        write_gateway_desired(runs_dir=get_runs_dir(), profile=prof, desired=d)
        return {"ok": True, "account_profile": prof}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"gateway_desired_failed: {e}")


@router.post("/api/gateway/command", response_class=JSONResponse)
async def api_gateway_command(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    ap = str(payload.get("account_profile") or "default").strip() or "default"
    cmd_type = str(payload.get("type") or "").strip()
    if not cmd_type:
        raise HTTPException(status_code=400, detail="missing type")
    allowed_types = {"cancel_all", "flatten", "disarm_live", "set_target", "reset_risk_kill"}
    if cmd_type not in allowed_types:
        raise HTTPException(status_code=400, detail=f"unsupported type: {cmd_type}")

    try:
        from ghtrader.tq.gateway import commands_path
        from ghtrader.tq.runtime import canonical_account_profile

        prof = canonical_account_profile(ap)
        p = commands_path(runs_dir=get_runs_dir(), profile=prof)
        p.parent.mkdir(parents=True, exist_ok=True)

        cmd = {
            "ts": _now_iso(),
            "command_id": uuid.uuid4().hex[:12],
            "account_profile": prof,
            "type": cmd_type,
            "params": payload.get("params") if isinstance(payload.get("params"), dict) else {},
        }
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(cmd, ensure_ascii=False, default=str) + "\n")

        try:
            r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            params = cmd.get("params") if isinstance(cmd.get("params"), dict) else {}
            await r.xadd(
                f"ghtrader:commands:{prof}",
                {
                    "ts": str(cmd.get("ts") or ""),
                    "command_id": str(cmd.get("command_id") or ""),
                    "account_profile": prof,
                    "type": str(cmd.get("type") or ""),
                    "params": json.dumps(params, ensure_ascii=False, default=str),
                    "symbol": str(params.get("symbol") or ""),
                    "target": str(params.get("target") or ""),
                },
                maxlen=10000,
                approximate=True,
            )
            await r.aclose()
        except Exception:
            pass
        return {"ok": True, "account_profile": prof, "command_id": cmd["command_id"]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"gateway_command_failed: {e}")
