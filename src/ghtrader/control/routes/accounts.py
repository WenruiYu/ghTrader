"""Accounts-domain API routes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config import get_runs_dir
from ghtrader.control import auth
from ghtrader.control.accounts_env import (
    accounts_env_delete_profile as _accounts_env_delete_profile,
    accounts_env_get_profile_values as _accounts_env_get_profile_values,
    accounts_env_is_configured as _accounts_env_is_configured,
    accounts_env_upsert_profile as _accounts_env_upsert_profile,
    canonical_profile as _canonical_profile,
    parse_profiles_csv as _parse_profiles_csv,
    read_accounts_env_values as _read_accounts_env_values,
)
from ghtrader.control.brokers import get_supported_brokers as _get_supported_brokers
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.state_helpers import (
    artifact_age_sec as _artifact_age_sec,
    read_json_file as _read_json_file,
    read_redis_json as _read_redis_json,
    status_from_desired_and_state as _status_from_desired_and_state,
)
from ghtrader.util.time import now_iso as _now_iso

router = APIRouter(tags=["accounts-api"])


def _mask_account_id(aid: str) -> str:
    s = str(aid or "")
    return (s[:2] + "***" + s[-2:]) if len(s) >= 6 else "***"


@router.get("/api/accounts", response_class=JSONResponse)
def api_accounts(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    runs_dir = get_runs_dir()
    verify_dir = runs_dir / "control" / "cache" / "accounts"
    gateway_root = runs_dir / "gateway"
    strategy_root = runs_dir / "strategy"

    env = _read_accounts_env_values(runs_dir=runs_dir)
    profiles: list[str] = ["default"]
    for p in _parse_profiles_csv(env.get("GHTRADER_TQ_ACCOUNT_PROFILES", "")):
        cp = _canonical_profile(p)
        if cp and cp not in profiles:
            profiles.append(cp)

    out_profiles: list[dict[str, Any]] = []
    for p in profiles:
        p = _canonical_profile(p)
        vals = _accounts_env_get_profile_values(env=env, profile=p)
        broker_id = str(vals.get("broker_id") or "")
        acc = str(vals.get("account_id") or "")
        configured = _accounts_env_is_configured(env=env, profile=p)
        verify = _read_json_file(verify_dir / f"account={p}.json") if verify_dir.exists() else None

        gw_root = gateway_root / f"account={p}"
        st_root = strategy_root / f"account={p}"
        gw_state_path = gw_root / "state.json"
        st_state_path = st_root / "state.json"
        gw_desired_path = gw_root / "desired.json"
        st_desired_path = st_root / "desired.json"

        gw_desired = _read_json_file(gw_desired_path) if gw_desired_path.exists() else None
        st_desired = _read_json_file(st_desired_path) if st_desired_path.exists() else None

        gw_age = _artifact_age_sec(gw_state_path) if gw_state_path.exists() else None
        st_age = _artifact_age_sec(st_state_path) if st_state_path.exists() else None

        gw_cfg = (
            gw_desired.get("desired")
            if isinstance(gw_desired, dict) and isinstance(gw_desired.get("desired"), dict)
            else (gw_desired or {})
        )
        st_cfg = (
            st_desired.get("desired")
            if isinstance(st_desired, dict) and isinstance(st_desired.get("desired"), dict)
            else (st_desired or {})
        )
        gw_mode = str((gw_cfg or {}).get("mode") or "idle")
        st_mode = str((st_cfg or {}).get("mode") or "idle")

        gw_status = _status_from_desired_and_state(root_exists=gw_root.exists(), desired_mode=gw_mode, state_age=gw_age)
        st_status = _status_from_desired_and_state(root_exists=st_root.exists(), desired_mode=st_mode, state_age=st_age)

        out_profiles.append(
            {
                "profile": p,
                "configured": configured,
                "broker_id": broker_id,
                "account_id_masked": _mask_account_id(acc) if acc else "",
                "verify": verify,
                "gateway": {"exists": bool(gw_root.exists()), "status": gw_status, "desired_mode": gw_mode, "state_age_sec": gw_age},
                "strategy": {"exists": bool(st_root.exists()), "status": st_status, "desired_mode": st_mode, "state_age_sec": st_age},
            }
        )

    return {"ok": True, "profiles": out_profiles, "generated_at": _now_iso()}


@router.post("/api/accounts/upsert", response_class=JSONResponse)
async def api_accounts_upsert(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    profile = str(payload.get("profile") or "").strip()
    broker_id = str(payload.get("broker_id") or "").strip()
    account_id = str(payload.get("account_id") or "").strip()
    password = str(payload.get("password") or "").strip()
    if not profile:
        raise HTTPException(status_code=400, detail="missing profile")
    if not broker_id or not account_id or not password:
        raise HTTPException(status_code=400, detail="missing broker_id/account_id/password")

    runs_dir = get_runs_dir()
    try:
        _accounts_env_upsert_profile(
            runs_dir=runs_dir,
            profile=profile,
            broker_id=broker_id,
            account_id=account_id,
            password=password,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"accounts_env_upsert_failed: {e}")

    return {"ok": True, "profile": _canonical_profile(profile)}


@router.post("/api/accounts/delete", response_class=JSONResponse)
async def api_accounts_delete(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    profile = str(payload.get("profile") or "").strip()
    if not profile:
        raise HTTPException(status_code=400, detail="missing profile")
    p = _canonical_profile(profile)

    runs_dir = get_runs_dir()
    try:
        _accounts_env_delete_profile(runs_dir=runs_dir, profile=p)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"accounts_env_delete_failed: {e}")

    verify_path = runs_dir / "control" / "cache" / "accounts" / f"account={p}.json"
    try:
        if verify_path.exists():
            verify_path.unlink()
    except Exception:
        pass

    return {"ok": True, "profile": p}


@router.get("/api/brokers", response_class=JSONResponse)
def api_brokers(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    runs_dir = get_runs_dir()
    brokers, source = _get_supported_brokers(runs_dir=runs_dir)
    return {"ok": True, "brokers": brokers, "source": source, "generated_at": _now_iso()}


@router.post("/api/accounts/enqueue-verify", response_class=JSONResponse)
async def api_accounts_enqueue_verify(request: Request) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload = await request.json()
    prof = str(payload.get("account_profile") or "default").strip() or "default"

    argv = python_module_argv("ghtrader.cli", "account", "verify", "--account", prof, "--json")
    title = f"account-verify {prof}"
    jm = request.app.state.job_manager
    rec = jm.enqueue_job(
        JobSpec(
            title=title,
            argv=argv,
            cwd=Path.cwd(),
            metadata={"kind": "account_verify", "account_profile": prof},
        )
    )
    return {"ok": True, "enqueued": [rec.id], "count": 1}


@router.get("/api/accounts/{profile}/status", response_class=JSONResponse)
def api_account_gateway_strategy_status(request: Request, profile: str) -> dict[str, Any]:
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    p = _canonical_profile(profile)
    runs_dir = get_runs_dir()
    gw_root = runs_dir / "gateway" / f"account={p}"
    st_root = runs_dir / "strategy" / f"account={p}"

    gw_state = _read_redis_json(f"ghtrader:gateway:state:{p}") or _read_json_file(gw_root / "state.json")
    st_state = _read_redis_json(f"ghtrader:strategy:state:{p}") or _read_json_file(st_root / "state.json")
    gw_desired = _read_redis_json(f"ghtrader:gateway:desired:{p}") or _read_json_file(gw_root / "desired.json")
    st_desired = _read_redis_json(f"ghtrader:strategy:desired:{p}") or _read_json_file(st_root / "desired.json")

    return {
        "ok": True,
        "profile": p,
        "gateway": {"root": str(gw_root), "state": gw_state, "desired": gw_desired},
        "strategy": {"root": str(st_root), "state": st_state, "desired": st_desired},
        "generated_at": _now_iso(),
    }
