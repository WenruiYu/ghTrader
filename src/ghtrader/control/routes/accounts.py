"""Accounts and brokers API routes."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.config import get_runs_dir
from ghtrader.control import auth
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.util.time import now_iso as _now_iso
from ghtrader.util.json_io import read_json as _read_json, write_json_atomic as _write_json_atomic

router = APIRouter(tags=["accounts"])


# ---------------------------------------------------------------------------
# Helper functions (migrated from app.py)
# ---------------------------------------------------------------------------

def _accounts_env_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "accounts.env"


def _read_accounts_env_values(*, runs_dir: Path) -> dict[str, str]:
    p = _accounts_env_path(runs_dir=runs_dir)
    if not p.exists():
        return {}
    env: dict[str, str] = {}
    try:
        import dotenv
        env = dict(dotenv.dotenv_values(str(p)))
    except Exception:
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _dotenv_quote(value: str) -> str:
    if any(c in value for c in ['"', "'", " ", "\n", "\t", "=", "#"]):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return value


def _write_accounts_env_atomic(*, runs_dir: Path, env: dict[str, str]) -> None:
    p = _accounts_env_path(runs_dir=runs_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for k in sorted(env.keys()):
        v = env[k]
        lines.append(f"{k}={_dotenv_quote(v)}")
    tmp = p.with_suffix(".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    tmp.rename(p)


def _canonical_profile(p: str) -> str:
    s = str(p).strip().upper()
    if not s:
        return "DEFAULT"
    return s


def _profile_env_suffixes(profile: str) -> list[str]:
    c = _canonical_profile(profile)
    if c == "DEFAULT":
        return ["", "_DEFAULT"]
    return [f"_{c}"]


def _parse_profiles_csv(raw: str) -> list[str]:
    out: list[str] = []
    for p in str(raw).split(","):
        c = _canonical_profile(p)
        if c and c not in out:
            out.append(c)
    return out


def _set_profiles_csv(env: dict[str, str], profiles: list[str]) -> None:
    env["TQSDK_PROFILES"] = ",".join([_canonical_profile(p) for p in profiles if _canonical_profile(p)])


def _accounts_env_upsert_profile(
    *,
    runs_dir: Path,
    profile: str,
    broker_id: str | None = None,
    account_id: str | None = None,
    password: str | None = None,
    td_url: str | None = None,
) -> None:
    env = _read_accounts_env_values(runs_dir=runs_dir)
    c = _canonical_profile(profile)
    suffixes = _profile_env_suffixes(c)
    sfx = suffixes[0] if suffixes else ""

    if broker_id is not None:
        env[f"TQSDK_BROKER_ID{sfx}"] = str(broker_id).strip()
    if account_id is not None:
        env[f"TQSDK_ACCOUNT_ID{sfx}"] = str(account_id).strip()
    if password is not None:
        env[f"TQSDK_PASSWORD{sfx}"] = str(password)
    if td_url is not None:
        env[f"TQSDK_TD_URL{sfx}"] = str(td_url).strip()

    profiles = _parse_profiles_csv(env.get("TQSDK_PROFILES") or "")
    if c not in profiles:
        profiles.append(c)
        _set_profiles_csv(env, profiles)

    _write_accounts_env_atomic(runs_dir=runs_dir, env=env)


def _accounts_env_delete_profile(*, runs_dir: Path, profile: str) -> None:
    env = _read_accounts_env_values(runs_dir=runs_dir)
    c = _canonical_profile(profile)
    suffixes = _profile_env_suffixes(c)
    for sfx in suffixes:
        for base in ["TQSDK_BROKER_ID", "TQSDK_ACCOUNT_ID", "TQSDK_PASSWORD", "TQSDK_TD_URL"]:
            key = f"{base}{sfx}"
            if key in env:
                del env[key]
    profiles = _parse_profiles_csv(env.get("TQSDK_PROFILES") or "")
    if c in profiles:
        profiles.remove(c)
        _set_profiles_csv(env, profiles)
    _write_accounts_env_atomic(runs_dir=runs_dir, env=env)


def _accounts_env_get_profile_values(*, env: dict[str, str], profile: str) -> dict[str, str]:
    c = _canonical_profile(profile)
    suffixes = _profile_env_suffixes(c)
    out: dict[str, str] = {}
    for base in ["TQSDK_BROKER_ID", "TQSDK_ACCOUNT_ID", "TQSDK_PASSWORD", "TQSDK_TD_URL"]:
        for sfx in suffixes:
            key = f"{base}{sfx}"
            if key in env:
                out[base] = str(env[key])
                break
    return out


def _accounts_env_is_configured(*, env: dict[str, str], profile: str) -> bool:
    vals = _accounts_env_get_profile_values(env=env, profile=profile)
    return all(
        [
            vals.get("TQSDK_BROKER_ID"),
            vals.get("TQSDK_ACCOUNT_ID"),
            vals.get("TQSDK_PASSWORD"),
        ]
    )


def _mask_account_id(aid: str) -> str:
    if len(aid) <= 4:
        return aid
    return aid[:2] + "*" * (len(aid) - 4) + aid[-2:]


def _brokers_cache_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "cache" / "brokers.json"


def _fallback_brokers() -> list[str]:
    return ["simnow", "快期模拟"]


def _parse_brokers_from_shinny_html(html: str) -> list[str]:
    import re
    out: list[str] = []
    for m in re.finditer(r'"broker_id"\s*:\s*"([^"]+)"', html):
        b = m.group(1).strip()
        if b and b not in out:
            out.append(b)
    if not out:
        for m in re.finditer(r'value="([^"]+)"', html):
            v = m.group(1).strip()
            if v and len(v) < 32 and v not in out:
                out.append(v)
    return out


def _fetch_shinny_brokers(*, timeout_s: float = 5.0) -> list[str]:
    import urllib.request
    url = "https://www.shinnytech.com/blog/tq-broker-list/"
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        return _parse_brokers_from_shinny_html(html)
    except Exception:
        return []


def _get_supported_brokers(*, runs_dir: Path) -> tuple[list[str], str]:
    cache = _brokers_cache_path(runs_dir=runs_dir)
    if cache.exists():
        try:
            obj = _read_json(cache)
            if isinstance(obj, dict):
                brokers = obj.get("brokers")
                source = obj.get("source") or "cache"
                if isinstance(brokers, list) and brokers:
                    return ([str(b) for b in brokers], str(source))
        except Exception:
            pass

    fetched = _fetch_shinny_brokers(timeout_s=5.0)
    if fetched:
        cache.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(cache, {"brokers": fetched, "source": "shinny", "fetched_at": _now_iso()})
        return (fetched, "shinny")

    fb = _fallback_brokers()
    return (fb, "fallback")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/accounts", response_class=JSONResponse)
def api_accounts(request: Request) -> dict[str, Any]:
    """Return configured TQSDK profiles (credentials masked)."""
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    runs_dir = get_runs_dir()
    env = _read_accounts_env_values(runs_dir=runs_dir)
    raw = env.get("TQSDK_PROFILES") or ""
    profiles = _parse_profiles_csv(raw)

    # Auto-populate DEFAULT if env vars exist but TQSDK_PROFILES is missing.
    if not profiles:
        vals = _accounts_env_get_profile_values(env=env, profile="DEFAULT")
        if vals.get("TQSDK_BROKER_ID") or vals.get("TQSDK_ACCOUNT_ID"):
            profiles = ["DEFAULT"]

    out: list[dict[str, Any]] = []
    for p in profiles:
        vals = _accounts_env_get_profile_values(env=env, profile=p)
        out.append(
            {
                "profile": p,
                "broker_id": vals.get("TQSDK_BROKER_ID") or "",
                "account_id_masked": _mask_account_id(vals.get("TQSDK_ACCOUNT_ID") or ""),
                "has_password": bool(vals.get("TQSDK_PASSWORD")),
                "td_url": vals.get("TQSDK_TD_URL") or "",
                "configured": _accounts_env_is_configured(env=env, profile=p),
            }
        )

    return {"ok": True, "profiles": out, "generated_at": _now_iso()}


@router.post("/accounts/upsert", response_class=JSONResponse)
async def api_accounts_upsert(request: Request) -> dict[str, Any]:
    """Create or update a TQSDK profile."""
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    profile = str(body.get("profile") or "").strip() or "DEFAULT"
    broker_id = body.get("broker_id")
    account_id = body.get("account_id")
    password = body.get("password")
    td_url = body.get("td_url")

    runs_dir = get_runs_dir()
    _accounts_env_upsert_profile(
        runs_dir=runs_dir,
        profile=profile,
        broker_id=broker_id,
        account_id=account_id,
        password=password,
        td_url=td_url,
    )
    return {"ok": True, "profile": _canonical_profile(profile)}


@router.post("/accounts/delete", response_class=JSONResponse)
async def api_accounts_delete(request: Request) -> dict[str, Any]:
    """Delete a TQSDK profile."""
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    profile = str(body.get("profile") or "").strip()
    if not profile:
        raise HTTPException(status_code=400, detail="profile is required")

    runs_dir = get_runs_dir()
    _accounts_env_delete_profile(runs_dir=runs_dir, profile=profile)
    return {"ok": True, "deleted": _canonical_profile(profile)}


@router.get("/brokers", response_class=JSONResponse)
def api_brokers(request: Request) -> dict[str, Any]:
    """Return list of supported broker IDs for TQSDK."""
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    runs_dir = get_runs_dir()
    brokers, source = _get_supported_brokers(runs_dir=runs_dir)
    return {"ok": True, "brokers": brokers, "source": source}


@router.post("/accounts/enqueue-verify", response_class=JSONResponse)
async def api_accounts_enqueue_verify(request: Request) -> dict[str, Any]:
    """Enqueue an account-verify job for a profile."""
    if not auth.is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        body = {}

    profile = str(body.get("profile") or "").strip() or "DEFAULT"
    jm = request.app.state.job_manager
    argv = python_module_argv("ghtrader.cli", "account", "verify", "--profile", profile)
    spec = JobSpec(argv=argv)
    job = jm.enqueue(spec)
    return {"ok": True, "job_id": job.id}
