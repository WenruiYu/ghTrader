from __future__ import annotations

from dataclasses import asdict
import os
import re
import sys
import threading
import time
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
import asyncio
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
import redis.asyncio as redis

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.db import JobStore
from ghtrader.control.jobs import JobManager, JobSpec, python_module_argv
from ghtrader.control.routes import build_root_router
from ghtrader.control.views import build_router
from ghtrader.util.json_io import read_json as _read_json, write_json_atomic as _write_json_atomic

log = structlog.get_logger()

_TQSDK_HEAVY_SUBCOMMANDS = {"download", "download-contract-range", "record", "update", "account"}
_TQSDK_HEAVY_DATA_SUBCOMMANDS = {"repair", "health", "main-l5-validate"}

_UI_STATUS_TTL_S = 5.0
_ui_status_at: float = 0.0
_ui_status_payload: dict[str, Any] | None = None
_ui_status_lock = threading.Lock()

_DASH_SUMMARY_TTL_S = 10.0
_dash_summary_at: float = 0.0
_dash_summary_payload: dict[str, Any] | None = None
_dash_summary_lock = threading.Lock()

_BENCHMARKS_TTL_S = 10.0
_benchmarks_at: float = 0.0
_benchmarks_payload: dict[str, Any] | None = None
_benchmarks_lock = threading.Lock()

_DATA_COVERAGE_TTL_S = 10.0
_data_coverage_at: float = 0.0
_data_coverage_cache_key: str = ""
_data_coverage_payload: dict[str, Any] | None = None
_data_coverage_lock = threading.Lock()

_DATA_COVERAGE_SUMMARY_TTL_S = 10.0
_data_coverage_summary_at: float = 0.0
_data_coverage_summary_payload: dict[str, Any] | None = None
_data_coverage_summary_lock = threading.Lock()

_DATA_QUALITY_TTL_S = 60.0
_data_quality_at: float = 0.0
_data_quality_cache_key: str = ""
_data_quality_payload: dict[str, Any] | None = None
_data_quality_lock = threading.Lock()


from ghtrader.util.time import now_iso as _now_iso


def _iter_symbol_dirs(root: Path) -> set[str]:
    out: set[str] = set()
    try:
        if not root.exists():
            return out
        for p in root.iterdir():
            if not p.is_dir() or not p.name.startswith("symbol="):
                continue
            sym = p.name.split("=", 1)[-1]
            if sym:
                out.add(sym)
    except Exception:
        return out
    return out


def _scan_model_files(artifacts_dir: Path) -> list[dict[str, Any]]:
    """
    Return a list of discovered model artifact files.

    ghTrader canonical layout:
      artifacts/<symbol>/<model_type>/model_h<h>.(json|pt|pkl)
    Also allow namespaced layouts:
      artifacts/(production|candidates)/<symbol>/<model_type>/model_h<h>.*
    """
    out: list[dict[str, Any]] = []
    if not artifacts_dir.exists():
        return out
    try:
        import re

        pat = re.compile(r"^model_h(?P<h>\d+)\.[a-zA-Z0-9]+$")
        allowed_ext = {".json", ".pt", ".pkl"}
        for f in artifacts_dir.rglob("model_h*.*"):
            try:
                if not f.is_file():
                    continue
                if f.suffix.lower() not in allowed_ext:
                    continue
                m = pat.match(f.name)
                if not m:
                    continue
                rel = f.relative_to(artifacts_dir).parts
                if len(rel) < 3:
                    continue
                namespace = None
                sym = None
                model_type = None
                if rel[0] in {"production", "candidates", "temp"} and len(rel) >= 4:
                    namespace = rel[0]
                    sym = rel[1]
                    model_type = rel[2]
                else:
                    sym = rel[0]
                    model_type = rel[1]
                horizon = int(m.group("h"))
                st = f.stat()
                out.append(
                    {
                        "symbol": str(sym),
                        "model_type": str(model_type),
                        "horizon": int(horizon),
                        "namespace": str(namespace) if namespace else "",
                        "path": str(f),
                        "size_bytes": int(st.st_size),
                        "mtime": float(st.st_mtime),
                    }
                )
            except Exception:
                continue
    except Exception:
        return []
    return out


def _job_subcommand(argv: list[str]) -> str | None:
    """
    Best-effort extraction of `ghtrader <subcommand>` from argv.
    """
    try:
        for i, t in enumerate(argv):
            if str(t) == "ghtrader.cli" and i + 1 < len(argv):
                return str(argv[i + 1])
        # Fallback for direct `ghtrader <cmd>` style
        if len(argv) >= 2 and str(argv[0]).endswith("ghtrader") and not str(argv[1]).startswith("-"):
            return str(argv[1])
    except Exception:
        return None
    return None


def _job_subcommand2(argv: list[str]) -> tuple[str | None, str | None]:
    """
    Best-effort extraction of `ghtrader <subcommand> [subcommand2]` from argv.

    Examples:
    - python -m ghtrader.cli data health ...
    - ghtrader data repair ...
    """
    try:
        for i, t in enumerate(argv):
            if str(t) == "ghtrader.cli" and i + 1 < len(argv):
                sub1 = str(argv[i + 1])
                sub2 = None
                if i + 2 < len(argv) and not str(argv[i + 2]).startswith("-"):
                    sub2 = str(argv[i + 2])
                return sub1, sub2
        # Fallback for direct `ghtrader <cmd>` style
        if len(argv) >= 2 and str(argv[0]).endswith("ghtrader") and not str(argv[1]).startswith("-"):
            sub1 = str(argv[1])
            sub2 = None
            if len(argv) >= 3 and not str(argv[2]).startswith("-"):
                sub2 = str(argv[2])
            return sub1, sub2
    except Exception:
        return None, None
    return None, None


def _is_tqsdk_heavy_job(argv: list[str]) -> bool:
    sub1, sub2 = _job_subcommand2(argv)
    s1 = sub1 or ""
    s2 = sub2 or ""
    if s1 in _TQSDK_HEAVY_SUBCOMMANDS:
        return True
    # Nested CLI groups (e.g., `ghtrader data health`) may use TqSdk and must be throttled.
    if s1 == "data" and s2 in _TQSDK_HEAVY_DATA_SUBCOMMANDS:
        return True
    return False


def _scheduler_enabled() -> bool:
    if str(os.environ.get("GHTRADER_DISABLE_TQSDK_SCHEDULER", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    # Avoid background threads during pytest unless explicitly enabled.
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_TQSDK_SCHEDULER_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def _daily_update_targets_from_env() -> list[tuple[str, str]]:
    """
    Parse `GHTRADER_DAILY_UPDATE_TARGETS` into [(EXCHANGE, var), ...].

    Examples:
    - "SHFE:cu"
    - "SHFE:cu,SHFE:au"
    - "SHFE.cu" (also accepted)
    """
    raw = str(os.environ.get("GHTRADER_DAILY_UPDATE_TARGETS", "")).strip()
    if not raw:
        return []
    out: list[tuple[str, str]] = []
    for part in [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]:
        ex = ""
        v = ""
        if ":" in part:
            ex, v = part.split(":", 1)
        elif "." in part:
            ex, v = part.split(".", 1)
        ex = str(ex).upper().strip()
        v = str(v).lower().strip()
        if ex and v:
            out.append((ex, v))
    # De-dupe while preserving order
    seen: set[str] = set()
    uniq: list[tuple[str, str]] = []
    for ex, v in out:
        k = f"{ex}:{v}"
        if k in seen:
            continue
        seen.add(k)
        uniq.append((ex, v))
    return uniq


def _daily_update_enabled() -> bool:
    return False


def _daily_update_state_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "cache" / "daily_update" / "state.json"


def _contracts_snapshot_path(*, runs_dir: Path, exchange: str, var: str) -> Path:
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    return runs_dir / "control" / "cache" / "contracts_snapshot" / f"contracts_exchange={ex}_var={v}.json"


def _contracts_snapshot_age_s(*, path: Path, obj: dict[str, Any] | None) -> float:
    try:
        if obj is not None:
            ts = float(obj.get("snapshot_cached_at_unix") or obj.get("cached_at_unix") or 0.0)
            if ts > 0:
                return float(max(0.0, time.time() - ts))
    except Exception:
        pass
    try:
        return float(max(0.0, time.time() - float(path.stat().st_mtime)))
    except Exception:
        return 1e9


def _accounts_env_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "accounts.env"


def _read_accounts_env_values(*, runs_dir: Path) -> dict[str, str]:
    path = _accounts_env_path(runs_dir=runs_dir)
    if not path.exists():
        return {}
    try:
        from dotenv import dotenv_values

        raw = dotenv_values(path)
        out: dict[str, str] = {}
        for k, v in raw.items():
            if not k:
                continue
            out[str(k)] = "" if v is None else str(v)
        return out
    except Exception:
        # Fall back to an empty mapping; callers should handle "no accounts" gracefully.
        return {}


def _dotenv_quote(value: str) -> str:
    v = str(value or "")
    v = v.replace("\\", "\\\\").replace('"', '\\"').replace("\r", "\\r").replace("\n", "\\n")
    return f'"{v}"'


def _write_accounts_env_atomic(*, runs_dir: Path, env: dict[str, str]) -> None:
    path = _accounts_env_path(runs_dir=runs_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "# ghTrader dashboard-managed broker accounts",
        "# WARNING: contains secrets. Do not commit.",
        "",
    ]

    # Deterministic-ish ordering for readability.
    keys: list[str] = []
    if "GHTRADER_TQ_ACCOUNT_PROFILES" in env:
        keys.append("GHTRADER_TQ_ACCOUNT_PROFILES")
    for k in ["TQ_BROKER_ID", "TQ_ACCOUNT_ID", "TQ_ACCOUNT_PASSWORD"]:
        if k in env:
            keys.append(k)
    for k in sorted([k for k in env.keys() if k not in set(keys)]):
        keys.append(k)

    lines: list[str] = []
    lines.extend(header)
    for k in keys:
        kk = str(k).strip()
        if not kk:
            continue
        vv = env.get(k, "")
        lines.append(f"{kk}={_dotenv_quote(vv)}")
    lines.append("")  # trailing newline
    content = "\n".join(lines)

    tmp = path.with_suffix(f".tmp-{uuid.uuid4().hex}")
    tmp.write_text(content, encoding="utf-8")
    try:
        tmp.chmod(0o600)
    except Exception:
        pass
    tmp.replace(path)
    try:
        path.chmod(0o600)
    except Exception:
        pass


def _canonical_profile(p: str) -> str:
    try:
        from ghtrader.tq.runtime import canonical_account_profile

        return canonical_account_profile(p)
    except Exception:
        return str(p or "").strip().lower() or "default"


def _profile_env_suffixes(profile: str) -> list[str]:
    p = _canonical_profile(profile)
    if p == "default":
        return [""]
    up = "".join([ch.upper() if ch.isalnum() else "_" for ch in p]).strip("_")
    lo = "".join([ch.lower() if ch.isalnum() else "_" for ch in p]).strip("_")
    out: list[str] = []
    for s in [up, lo]:
        if s and s not in out:
            out.append(s)
    return out


def _parse_profiles_csv(raw: str) -> list[str]:
    out: list[str] = []
    for part in [p.strip() for p in str(raw or "").split(",") if p.strip()]:
        p = _canonical_profile(part)
        if p and p != "default" and p not in out:
            out.append(p)
    return out


def _set_profiles_csv(env: dict[str, str], profiles: list[str]) -> None:
    ps = [p for p in profiles if p and p != "default"]
    if ps:
        env["GHTRADER_TQ_ACCOUNT_PROFILES"] = ",".join(ps)
    else:
        env.pop("GHTRADER_TQ_ACCOUNT_PROFILES", None)


def _accounts_env_upsert_profile(
    *,
    runs_dir: Path,
    profile: str,
    broker_id: str,
    account_id: str,
    password: str,
) -> None:
    p = _canonical_profile(profile)
    env = _read_accounts_env_values(runs_dir=runs_dir)

    # Maintain profile registry (excluding default).
    profiles = _parse_profiles_csv(env.get("GHTRADER_TQ_ACCOUNT_PROFILES", ""))
    if p != "default" and p not in profiles:
        profiles.append(p)
    _set_profiles_csv(env, profiles)

    b = str(broker_id or "").strip()
    a = str(account_id or "").strip()
    pw = str(password or "").strip()

    if p == "default":
        env["TQ_BROKER_ID"] = b
        env["TQ_ACCOUNT_ID"] = a
        env["TQ_ACCOUNT_PASSWORD"] = pw
    else:
        suf = _profile_env_suffixes(p)[0]  # uppercase preferred
        env[f"TQ_BROKER_ID_{suf}"] = b
        env[f"TQ_ACCOUNT_ID_{suf}"] = a
        env[f"TQ_ACCOUNT_PASSWORD_{suf}"] = pw

    _write_accounts_env_atomic(runs_dir=runs_dir, env=env)


def _accounts_env_delete_profile(*, runs_dir: Path, profile: str) -> None:
    p = _canonical_profile(profile)
    env = _read_accounts_env_values(runs_dir=runs_dir)

    if p == "default":
        env.pop("TQ_BROKER_ID", None)
        env.pop("TQ_ACCOUNT_ID", None)
        env.pop("TQ_ACCOUNT_PASSWORD", None)
    else:
        for suf in _profile_env_suffixes(p):
            if not suf:
                continue
            env.pop(f"TQ_BROKER_ID_{suf}", None)
            env.pop(f"TQ_ACCOUNT_ID_{suf}", None)
            env.pop(f"TQ_ACCOUNT_PASSWORD_{suf}", None)

        profiles = _parse_profiles_csv(env.get("GHTRADER_TQ_ACCOUNT_PROFILES", ""))
        profiles = [x for x in profiles if x != p]
        _set_profiles_csv(env, profiles)

    _write_accounts_env_atomic(runs_dir=runs_dir, env=env)


def _accounts_env_get_profile_values(*, env: dict[str, str], profile: str) -> dict[str, str]:
    p = _canonical_profile(profile)
    if p == "default":
        return {
            "broker_id": str(env.get("TQ_BROKER_ID", "") or "").strip(),
            "account_id": str(env.get("TQ_ACCOUNT_ID", "") or "").strip(),
        }

    for suf in _profile_env_suffixes(p):
        if not suf:
            continue
        broker = str(env.get(f"TQ_BROKER_ID_{suf}", "") or "").strip()
        acc = str(env.get(f"TQ_ACCOUNT_ID_{suf}", "") or "").strip()
        if broker or acc:
            return {"broker_id": broker, "account_id": acc}
    return {"broker_id": "", "account_id": ""}


def _accounts_env_is_configured(*, env: dict[str, str], profile: str) -> bool:
    p = _canonical_profile(profile)
    if p == "default":
        broker = str(env.get("TQ_BROKER_ID", "") or "").strip()
        acc = str(env.get("TQ_ACCOUNT_ID", "") or "").strip()
        pwd = str(env.get("TQ_ACCOUNT_PASSWORD", "") or "").strip()
        return bool(broker and acc and pwd)
    for suf in _profile_env_suffixes(p):
        if not suf:
            continue
        broker = str(env.get(f"TQ_BROKER_ID_{suf}", "") or "").strip()
        acc = str(env.get(f"TQ_ACCOUNT_ID_{suf}", "") or "").strip()
        pwd = str(env.get(f"TQ_ACCOUNT_PASSWORD_{suf}", "") or "").strip()
        if broker and acc and pwd:
            return True
    return False


def _mask_account_id(aid: str) -> str:
    s = str(aid or "")
    return (s[:2] + "***" + s[-2:]) if len(s) >= 6 else "***"


def _brokers_cache_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "cache" / "brokers.json"


def _fallback_brokers() -> list[str]:
    # Keep this list tiny; it is only a UX fallback when network is unavailable.
    return ["T铜冠金源"]


def _parse_brokers_from_shinny_html(html: str) -> list[str]:
    try:
        import html as html_mod
        import re

        # Quick-and-dirty tag strip to get searchable text.
        text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", "\n", str(html))
        text = re.sub(r"(?s)<[^>]+>", "\n", text)
        text = html_mod.unescape(text)

        # Broker IDs are typically like: "T铜冠金源", "H海通期货", "SIMNOW", etc.
        # We only extract the ones containing at least one CJK character.
        pat = re.compile(r"(?<![A-Za-z0-9_])([A-Z][A-Za-z0-9_]*[\u4e00-\u9fff][\u4e00-\u9fffA-Za-z0-9_]*)(?![A-Za-z0-9_])")
        found = pat.findall(text)

        out: list[str] = []
        for s in found:
            s = str(s).strip()
            if not s or len(s) > 32:
                continue
            if s not in out:
                out.append(s)
        return out
    except Exception:
        return []


def _fetch_shinny_brokers(*, timeout_s: float = 5.0) -> list[str]:
    url = "https://www.shinnytech.com/articles/reference/tqsdk-brokers"
    try:
        import urllib.request

        with urllib.request.urlopen(url, timeout=float(timeout_s)) as resp:  # nosec - expected public doc URL
            raw = resp.read()
        html = raw.decode("utf-8", errors="ignore")
        return _parse_brokers_from_shinny_html(html)
    except Exception:
        return []


def _get_supported_brokers(*, runs_dir: Path) -> tuple[list[str], str]:
    """
    Return (brokers, source) where source is one of: cache|fetched|fallback.
    """
    cache = _brokers_cache_path(runs_dir=runs_dir)
    cached = _read_json(cache) if cache.exists() else None
    if isinstance(cached, dict) and isinstance(cached.get("brokers"), list) and cached.get("brokers"):
        brokers = [str(x) for x in cached["brokers"] if str(x).strip()]
        if brokers:
            return brokers, "cache"

    # Avoid network in tests.
    if os.environ.get("PYTEST_CURRENT_TEST") or "pytest" in sys.modules:
        return _fallback_brokers(), "fallback"

    brokers = _fetch_shinny_brokers()
    if brokers:
        _write_json_atomic(
            cache,
            {
                "brokers": brokers,
                "fetched_at": _now_iso(),
                "source_url": "https://www.shinnytech.com/articles/reference/tqsdk-brokers",
            },
        )
        return brokers, "fetched"

    return _fallback_brokers(), "fallback"


def _argv_opt(argv: list[str], name: str) -> str | None:
    try:
        for i, t in enumerate(argv):
            if str(t) == str(name) and i + 1 < len(argv):
                return str(argv[i + 1])
    except Exception:
        return None
    return None


def _start_daily_update_scheduler(app: FastAPI) -> None:
    if getattr(app.state, "_daily_update_scheduler_started", False):
        return
    app.state._daily_update_scheduler_started = True

    def _loop() -> None:
        while True:
            try:
                _daily_update_tick(app=app)
            except Exception as e:
                log.warning("daily_update.tick_failed", error=str(e))
            time.sleep(float(os.environ.get("GHTRADER_DAILY_UPDATE_POLL_SECONDS", "60")))

    t = threading.Thread(target=_loop, name="daily-update-scheduler", daemon=True)
    t.start()


def _daily_update_tick(*, app: FastAPI) -> None:
    runs_dir = get_runs_dir()
    data_dir = get_data_dir()
    store: JobStore = app.state.job_store
    jm: JobManager = app.state.job_manager

    targets = _daily_update_targets_from_env()
    if not targets:
        return

    # Determine "today trading day" (UTC) so we enqueue at most once per trading day.
    from ghtrader.data.trading_calendar import get_trading_calendar

    # Dashboard scheduler: avoid network in control-plane threads unless explicitly requested elsewhere.
    cal = get_trading_calendar(data_dir=data_dir, refresh=False, allow_download=False)
    today = datetime.now(timezone.utc).date()
    today_trading = today
    if cal:
        # last <= today
        lo = 0
        hi = len(cal)
        while lo < hi:
            mid = (lo + hi) // 2
            if cal[mid] <= today:
                lo = mid + 1
            else:
                hi = mid
        idx = lo - 1
        today_trading = cal[idx] if idx >= 0 else today
    else:
        while today_trading.weekday() >= 5:
            today_trading -= timedelta(days=1)

    st_path = _daily_update_state_path(runs_dir=runs_dir)
    state = _read_json(st_path) or {}
    last_by_target: dict[str, str] = dict(state.get("last_enqueued_trading_day") or {})
    last_success_by_target: dict[str, str] = dict(state.get("last_successful_trading_day") or {})
    last_attempt_by_target: dict[str, str] = dict(state.get("last_attempt_at") or {})
    last_job_by_target: dict[str, str] = dict(state.get("last_job_id") or {})
    last_status_by_target: dict[str, str] = dict(state.get("last_job_status") or {})
    failure_count_by_target: dict[str, int] = {
        str(k): int(v or 0) for k, v in (state.get("failure_count") or {}).items()
    }

    active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=500)

    catalog_ttl_s = int(os.environ.get("GHTRADER_DAILY_UPDATE_CATALOG_TTL_SECONDS", "3600"))
    backoff_s = int(os.environ.get("GHTRADER_DAILY_UPDATE_RETRY_BACKOFF_SECONDS", "300"))
    max_catchup = int(os.environ.get("GHTRADER_DAILY_UPDATE_MAX_CATCHUP_DAYS", "0"))

    for ex, v in targets:
        key = f"{ex}:{v}"
        last_success = str(last_success_by_target.get(key) or "")
        last_attempt = str(last_attempt_by_target.get(key) or "")
        last_enqueued = str(last_by_target.get(key) or "")

        # Update status from the last job id (avoid double-counting).
        last_job_id = str(last_job_by_target.get(key) or "")
        if last_job_id:
            job = store.get_job(last_job_id)
            if job is not None and job.status:
                st = str(job.status).lower().strip()
                if st and st != str(last_status_by_target.get(key) or ""):
                    last_status_by_target[key] = st
                    if st == "succeeded":
                        last_success_by_target[key] = last_enqueued or today_trading.isoformat()
                        failure_count_by_target[key] = 0
                    elif st in {"failed", "cancelled"}:
                        failure_count_by_target[key] = int(failure_count_by_target.get(key, 0)) + 1

        # Determine which trading day to enqueue (today or catch-up).
        target_day = today_trading
        if max_catchup > 0 and last_success:
            try:
                last_dt = date.fromisoformat(last_success)
                if last_dt < today_trading:
                    # Catch up at most N trading days.
                    if cal:
                        idx_last = -1
                        for i, d in enumerate(cal):
                            if d <= last_dt:
                                idx_last = i
                            else:
                                break
                        idx_today = cal.index(today_trading) if today_trading in cal else (len(cal) - 1)
                        earliest = cal[max(0, idx_today - max_catchup + 1)]
                        next_day = cal[min(len(cal) - 1, idx_last + 1)] if idx_last >= 0 else earliest
                        target_day = next_day if next_day >= earliest else earliest
                    else:
                        earliest = today_trading - timedelta(days=max_catchup - 1)
                        target_day = max(last_dt + timedelta(days=1), earliest)
            except Exception:
                target_day = today_trading

        if last_enqueued == target_day.isoformat() and last_success == target_day.isoformat():
            continue

        # Skip if an update job for this target is already queued/running.
        already = False
        for j in active:
            sub = _job_subcommand(j.command) or ""
            if sub != "update":
                continue
            j_ex = _argv_opt(j.command, "--exchange")
            j_v = _argv_opt(j.command, "--var")
            if str(j_ex or "").upper().strip() == ex and str(j_v or "").lower().strip() == v:
                already = True
                break
        if already:
            continue

        # Backoff if last attempt failed recently.
        if last_attempt and int(failure_count_by_target.get(key, 0)) > 0:
            try:
                last_attempt_dt = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - last_attempt_dt).total_seconds() < float(backoff_s):
                    continue
            except Exception:
                pass

        recent_days = int(os.environ.get("GHTRADER_DAILY_UPDATE_RECENT_EXPIRED_DAYS", "10"))
        refresh_catalog = True
        if last_attempt and failure_count_by_target.get(key, 0) == 0:
            refresh_catalog = False
        if catalog_ttl_s > 0 and last_attempt:
            try:
                last_attempt_dt = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - last_attempt_dt).total_seconds() < float(catalog_ttl_s):
                    refresh_catalog = False
            except Exception:
                pass
        argv = python_module_argv(
            "ghtrader.cli",
            "update",
            "--exchange",
            ex,
            "--var",
            v,
            "--recent-expired-days",
            str(int(recent_days)),
            "--refresh-catalog" if refresh_catalog else "--no-refresh-catalog",
            "--catalog-ttl-seconds",
            str(int(catalog_ttl_s)),
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        title = f"daily-update {ex}.{v} {target_day.isoformat()}"
        rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        last_by_target[key] = target_day.isoformat()
        last_attempt_by_target[key] = _now_iso()
        last_job_by_target[key] = rec.id
        log.info(
            "daily_update.enqueued",
            exchange=ex,
            var=v,
            trading_day=target_day.isoformat(),
            refresh_catalog=refresh_catalog,
            job_id=rec.id,
        )

    state = {
        "updated_at": _now_iso(),
        "last_enqueued_trading_day": last_by_target,
        "last_successful_trading_day": last_success_by_target,
        "last_attempt_at": last_attempt_by_target,
        "last_job_id": last_job_by_target,
        "last_job_status": last_status_by_target,
        "failure_count": failure_count_by_target,
    }
    try:
        _write_json_atomic(st_path, state)
    except Exception:
        pass


def _start_tqsdk_scheduler(app: FastAPI, *, max_parallel_default: int = 4) -> None:
    if getattr(app.state, "_tqsdk_scheduler_started", False):
        return
    app.state._tqsdk_scheduler_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                max_parallel = int(os.environ.get("GHTRADER_MAX_PARALLEL_TQSDK_JOBS", str(max_parallel_default)))
                max_parallel = max(1, max_parallel)
                _tqsdk_scheduler_tick(store=store, jm=jm, max_parallel=max_parallel)
            except Exception as e:
                log.warning("tqsdk_scheduler.tick_failed", error=str(e))
            time.sleep(1.0)

    t = threading.Thread(target=_loop, name="tqsdk-job-scheduler", daemon=True)
    t.start()


def _tqsdk_scheduler_tick(*, store: JobStore, jm: JobManager, max_parallel: int) -> int:
    """
    Run one scheduling tick: start up to `max_parallel` TqSdk-heavy queued jobs.

    Exposed for unit tests.
    """
    active = [j for j in store.list_active_jobs() if _is_tqsdk_heavy_job(j.command)]
    slots = int(max_parallel) - int(len(active))
    if slots <= 0:
        return 0
    queued = [j for j in store.list_unstarted_queued_jobs(limit=5000) if _is_tqsdk_heavy_job(j.command)]
    started = 0
    for j in queued[:slots]:
        out = jm.start_queued_job(j.id)
        if out is not None and out.pid is not None:
            started += 1
    return started


def _gateway_supervisor_enabled() -> bool:
    """
    Whether the dashboard should supervise AccountGateway processes.

    Disabled by default during pytest to avoid background process management in unit tests.
    """
    if str(os.environ.get("GHTRADER_DISABLE_GATEWAY_SUPERVISOR", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_GATEWAY_SUPERVISOR_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def _is_gateway_job(argv: list[str]) -> bool:
    sub1, sub2 = _job_subcommand2(argv)
    return bool((sub1 or "") == "gateway" and (sub2 or "") in {"run"})


def _scan_gateway_desired(*, runs_dir: Path) -> dict[str, str]:
    """
    Return mapping {profile -> desired_mode} for gateway profiles.

    Source: runs/gateway/account=<profile>/desired.json
    """
    root = runs_dir / "gateway"
    out: dict[str, str] = {}
    try:
        if not root.exists():
            return out
        for d in [p for p in root.iterdir() if p.is_dir() and p.name.startswith("account=")]:
            prof = d.name.split("=", 1)[-1] if "=" in d.name else d.name
            desired = _read_json(d / "desired.json") or {}
            cfg = desired.get("desired") if isinstance(desired.get("desired"), dict) else desired
            mode = str((cfg or {}).get("mode") or "idle").strip().lower()
            if prof:
                out[str(prof)] = mode
    except Exception:
        return {}
    return out


def _strategy_supervisor_enabled() -> bool:
    """
    Whether the dashboard should supervise StrategyRunner processes.

    Disabled by default during pytest to avoid background process management in unit tests.
    """
    if str(os.environ.get("GHTRADER_DISABLE_STRATEGY_SUPERVISOR", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_STRATEGY_SUPERVISOR_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def _is_strategy_job(argv: list[str]) -> bool:
    sub1, sub2 = _job_subcommand2(argv)
    return bool((sub1 or "") == "strategy" and (sub2 or "") in {"run"})


def _scan_strategy_desired(*, runs_dir: Path) -> dict[str, dict[str, Any]]:
    """
    Return mapping {profile -> desired_config_dict} for strategies.

    Source: runs/strategy/account=<profile>/desired.json
    """
    root = runs_dir / "strategy"
    out: dict[str, dict[str, Any]] = {}
    try:
        if not root.exists():
            return out
        for d in [p for p in root.iterdir() if p.is_dir() and p.name.startswith("account=")]:
            prof = d.name.split("=", 1)[-1] if "=" in d.name else d.name
            desired = _read_json(d / "desired.json") or {}
            cfg = desired.get("desired") if isinstance(desired.get("desired"), dict) else desired
            cfg = cfg if isinstance(cfg, dict) else {}
            if prof:
                out[str(prof)] = dict(cfg)
    except Exception:
        return {}
    return out


def _strategy_supervisor_tick(*, store: JobStore, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one StrategySupervisor tick.

    Exposed for unit tests (pass fake `jm` to avoid launching subprocesses).
    """
    desired_by_profile = _scan_strategy_desired(runs_dir=runs_dir)

    active_jobs = store.list_active_jobs()
    active_strategy: dict[str, Any] = {}
    for j in active_jobs:
        if not _is_strategy_job(j.command):
            continue
        prof = _argv_opt(j.command, "--account") or ""
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(prof)
        except Exception:
            prof = str(prof).strip().lower() or "default"
        active_strategy[prof] = j

    stopped = 0
    started = 0

    # Stop strategies whose desired mode is explicitly idle.
    for prof, job in active_strategy.items():
        cfg = desired_by_profile.get(prof) or {}
        dm = str(cfg.get("mode") or "idle").strip().lower()
        if dm in {"", "idle"}:
            try:
                if bool(jm.cancel_job(job.id)):
                    stopped += 1
            except Exception:
                pass

    # Start at most one strategy per tick.
    for prof, cfg in desired_by_profile.items():
        dm = str(cfg.get("mode") or "idle").strip().lower()
        if dm in {"", "idle"}:
            continue
        if prof in active_strategy:
            continue

        symbols: list[str] = []
        raw_syms = cfg.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]
        if not symbols:
            continue

        model_name = str(cfg.get("model_name") or "xgboost").strip() or "xgboost"
        horizon = str(cfg.get("horizon") or "50").strip()
        threshold_up = str(cfg.get("threshold_up") or "0.6").strip()
        threshold_down = str(cfg.get("threshold_down") or "0.6").strip()
        position_size = str(cfg.get("position_size") or "1").strip()
        artifacts_dir = str(cfg.get("artifacts_dir") or "artifacts").strip()
        poll_interval_sec = str(cfg.get("poll_interval_sec") or "0.5").strip()

        argv = python_module_argv(
            "ghtrader.cli",
            "strategy",
            "run",
            "--account",
            prof,
            "--model",
            model_name,
            "--horizon",
            horizon,
            "--threshold-up",
            threshold_up,
            "--threshold-down",
            threshold_down,
            "--position-size",
            position_size,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            str(runs_dir),
            "--poll-interval-sec",
            poll_interval_sec,
        )
        for s in symbols:
            argv += ["--symbols", s]
        title = f"strategy {prof} {model_name} h={horizon}"
        try:
            jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            started += 1
        except Exception:
            pass
        break

    return {"started": int(started), "stopped": int(stopped)}


def _start_strategy_supervisor(app: FastAPI) -> None:
    if getattr(app.state, "_strategy_supervisor_started", False):
        return
    app.state._strategy_supervisor_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                runs_dir = get_runs_dir()
                _strategy_supervisor_tick(store=store, jm=jm, runs_dir=runs_dir)
            except Exception as e:
                log.warning("strategy_supervisor.tick_failed", error=str(e))
            time.sleep(float(os.environ.get("GHTRADER_STRATEGY_SUPERVISOR_POLL_SECONDS", "2.0")))

    t = threading.Thread(target=_loop, name="strategy-supervisor", daemon=True)
    t.start()


def _start_gateway_supervisor(app: FastAPI) -> None:
    if getattr(app.state, "_gateway_supervisor_started", False):
        return
    app.state._gateway_supervisor_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                runs_dir = get_runs_dir()
                desired_by_profile = _scan_gateway_desired(runs_dir=runs_dir)

                active_jobs = store.list_active_jobs()
                active_gateway: dict[str, Any] = {}
                for j in active_jobs:
                    if not _is_gateway_job(j.command):
                        continue
                    prof = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        prof = canonical_account_profile(prof)
                    except Exception:
                        prof = str(prof).strip().lower() or "default"
                    active_gateway[prof] = j

                # Stop gateways whose desired mode is explicitly idle.
                for prof, job in active_gateway.items():
                    dm = str(desired_by_profile.get(prof) or "").strip().lower()
                    if dm == "idle":
                        try:
                            jm.cancel_job(job.id)
                        except Exception:
                            pass

                # Start at most one gateway per tick to avoid a thundering herd.
                for prof, dm in desired_by_profile.items():
                    dm2 = str(dm or "").strip().lower()
                    if dm2 in {"", "idle"}:
                        continue
                    if prof in active_gateway:
                        continue
                    argv = python_module_argv(
                        "ghtrader.cli",
                        "gateway",
                        "run",
                        "--account",
                        prof,
                        "--runs-dir",
                        str(runs_dir),
                    )
                    title = f"gateway {prof}"
                    jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
                    break
            except Exception as e:
                log.warning("gateway_supervisor.tick_failed", error=str(e))
            time.sleep(float(os.environ.get("GHTRADER_GATEWAY_SUPERVISOR_POLL_SECONDS", "2.0")))

    t = threading.Thread(target=_loop, name="gateway-supervisor", daemon=True)
    t.start()


def _control_root(runs_dir: Path) -> Path:
    return runs_dir / "control"


def _jobs_db_path(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "jobs.db"


def _logs_dir(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "logs"


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []
        self.redis: redis.Redis | None = None
        self.pubsub: Any = None
        self.task: asyncio.Task | None = None

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)
        if not self.redis:
            await self._start_redis()

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str) -> None:
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

    async def _start_redis(self) -> None:
        try:
            self.redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            self.pubsub = self.redis.pubsub()
            await self.pubsub.psubscribe("ghtrader:*:updates:*")
            self.task = asyncio.create_task(self._redis_listener())
        except Exception as e:
            log.warning("websocket.redis_connect_failed", error=str(e))

    async def _redis_listener(self) -> None:
        if not self.pubsub:
            return
        try:
            async for message in self.pubsub.listen():
                if message["type"] == "pmessage":
                    channel = message["channel"]
                    data = message["data"]
                    try:
                        payload = json.loads(data)
                    except Exception:
                        payload = data
                    await self.broadcast(json.dumps({"channel": channel, "data": payload}, default=str))
        except Exception as e:
            log.warning("websocket.redis_listener_error", error=str(e))


def create_app() -> Any:
    runs_dir = get_runs_dir()
    store = JobStore(_jobs_db_path(runs_dir))
    jm = JobManager(store=store, logs_dir=_logs_dir(runs_dir))
    jm.reconcile()

    # Apply persisted dashboard settings before starting background schedulers.
    try:
        from ghtrader.control.settings import apply_tqsdk_scheduler_settings_from_disk

        apply_tqsdk_scheduler_settings_from_disk(runs_dir=runs_dir)
    except Exception:
        pass

    app = FastAPI(title="ghTrader Control", version="0.1.0")
    app.state.job_store = store
    app.state.job_manager = jm
    if _scheduler_enabled():
        _start_tqsdk_scheduler(app)
    if _daily_update_enabled():
        _start_daily_update_scheduler(app)
    if _gateway_supervisor_enabled():
        _start_gateway_supervisor(app)
    if _strategy_supervisor_enabled():
        _start_strategy_supervisor(app)

    # Static assets (CSS)
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # HTML pages
    app.include_router(build_router())
    app.include_router(build_root_router())

    # WebSocket Endpoint
    manager = ConnectionManager()

    @app.websocket("/ws/dashboard")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)

    # JSON API (kept small; UI uses HTML routes above)
    @app.get("/api/jobs", response_class=JSONResponse)
    def api_list_jobs(request: Request, limit: int = 200) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        jobs = store.list_jobs(limit=int(limit))
        return {"jobs": [asdict(j) for j in jobs]}

    @app.get("/api/data/coverage", response_class=JSONResponse)
    def api_data_coverage(
        request: Request, kind: str = "ticks", limit: int = 200, search: str = "", refresh: bool = False
    ) -> dict[str, Any]:
        """
        Lightweight coverage listing for Data Hub (lazy-loaded).

        kind: ticks|main_l5|features|labels
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = (kind, limit, search, refresh)
        raise HTTPException(status_code=410, detail="data coverage endpoint removed in current PRD phase")

    @app.get("/api/data/coverage/summary", response_class=JSONResponse)
    def api_data_coverage_summary(request: Request, refresh: bool = False) -> dict[str, Any]:
        """
        Aggregated coverage summary for Data Hub Overview KPIs.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = refresh
        raise HTTPException(status_code=410, detail="coverage summary endpoint removed in current PRD phase")

    @app.get("/api/data/quality-summary", response_class=JSONResponse)
    def api_data_quality_summary(
        request: Request,
        exchange: str = "SHFE",
        var: str = "cu",
        limit: int = 300,
        search: str = "",
        issues_only: bool = False,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """
        Return per-symbol data quality summary (QuestDB-first, cached).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        # Keep this as a best-effort read endpoint; unlike other deferred APIs it has
        # a complete implementation and does not enqueue removed CLI surfaces.

        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        lim = max(1, min(int(limit or 300), 500))
        q = str(search or "").strip().lower()
        only_issues = bool(issues_only)

        global _data_quality_at, _data_quality_cache_key, _data_quality_payload
        now = time.time()
        cache_key = f"{ex}|{v}|{lim}|{q}|{int(only_issues)}"
        with _data_quality_lock:
            if (
                isinstance(_data_quality_payload, dict)
                and _data_quality_cache_key == cache_key
                and (now - float(_data_quality_at)) <= float(_DATA_QUALITY_TTL_S)
                and not refresh
            ):
                return dict(_data_quality_payload)

        runs_dir = get_runs_dir()
        snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / f"contracts_exchange={ex}_var={v}.json"
        snap = _read_json(snap_path) if snap_path.exists() else None
        snap = dict(snap) if isinstance(snap, dict) else {}

        contracts = snap.get("contracts") if isinstance(snap.get("contracts"), list) else []
        comp_summary = None
        try:
            comp = snap.get("completeness") if isinstance(snap.get("completeness"), dict) else {}
            comp_summary = comp.get("summary") if isinstance(comp.get("summary"), dict) else None
        except Exception:
            comp_summary = None

        # Base rows (from cached snapshot; safe to load).
        base_rows: list[dict[str, Any]] = []
        freshness_max_age_sec: float | None = None
        for r in contracts:
            if not isinstance(r, dict):
                continue
            sym = str(r.get("symbol") or "").strip()
            if not sym:
                continue
            if q and q not in sym.lower():
                continue

            comp = r.get("completeness") if isinstance(r.get("completeness"), dict) else {}
            qc = r.get("questdb_coverage") if isinstance(r.get("questdb_coverage"), dict) else {}
            missing_days = comp.get("missing_days")
            expected_days = comp.get("expected_days")
            present_days = comp.get("present_days")
            status = str(r.get("status") or "").strip() or None
            last_tick_age_sec = r.get("last_tick_age_sec")
            try:
                if r.get("expired") is False and last_tick_age_sec is not None:
                    age = float(last_tick_age_sec)
                    if freshness_max_age_sec is None or age > freshness_max_age_sec:
                        freshness_max_age_sec = age
            except Exception:
                pass

            base_rows.append(
                {
                    "symbol": sym,
                    "status": status,
                    "last_tick_ts": str(qc.get("last_tick_ts") or "").strip() or None,
                    "last_tick_day": str(qc.get("last_tick_day") or "").strip() or None,
                    "missing_days": (int(missing_days) if missing_days is not None else None),
                    "expected_days": (int(expected_days) if expected_days is not None else None),
                    "present_days": (int(present_days) if present_days is not None else None),
                    "last_tick_age_sec": (float(last_tick_age_sec) if last_tick_age_sec is not None else None),
                }
            )
            if len(base_rows) >= lim:
                break

        # Join latest QuestDB quality ledgers when available.
        fq_by_sym: dict[str, dict[str, Any]] = {}
        gaps_by_sym: dict[str, dict[str, Any]] = {}
        fq_ok = False
        gaps_ok = False
        fq_error: str | None = None
        gaps_error: str | None = None
        questdb_error: str | None = None
        errors: list[str] = []
        try:
            from ghtrader.questdb.client import connect_pg as _connect, make_questdb_query_config_from_env

            cfg = make_questdb_query_config_from_env()
            syms = [r["symbol"] for r in base_rows if str(r.get("symbol") or "").strip()]
            if syms:
                placeholders = ", ".join(["%s"] * len(syms))
                fq_sql = (
                    "SELECT symbol, trading_day, rows_total, "
                    "bid_price1_null_rate, ask_price1_null_rate, last_price_null_rate, volume_null_rate, open_interest_null_rate, "
                    "l5_fields_null_rate, price_jump_outliers, volume_spike_outliers, spread_anomaly_outliers, updated_at "
                    "FROM ghtrader_field_quality_v2 "
                    f"WHERE symbol IN ({placeholders}) AND ticks_kind='main_l5' AND dataset_version='v2' "
                    "LATEST ON ts PARTITION BY symbol"
                )
                gaps_sql = (
                    "SELECT symbol, trading_day, tick_count_actual, tick_count_expected_min, tick_count_expected_max, "
                    "median_interval_ms, p95_interval_ms, p99_interval_ms, max_interval_ms, "
                    "abnormal_gaps_count, critical_gaps_count, largest_gap_duration_ms, updated_at "
                    "FROM ghtrader_tick_gaps_v2 "
                    f"WHERE symbol IN ({placeholders}) AND ticks_kind='main_l5' AND dataset_version='v2' "
                    "LATEST ON ts PARTITION BY symbol"
                )
                with _connect(cfg, connect_timeout_s=2) as conn:
                    with conn.cursor() as cur:
                        try:
                            cur.execute(fq_sql, syms)
                            for row in cur.fetchall() or []:
                                s = str(row[0] or "").strip()
                                if not s:
                                    continue
                                fq_by_sym[s] = {
                                    "trading_day": str(row[1] or "").strip() or None,
                                    "rows_total": (int(row[2]) if row[2] is not None else None),
                                    "bid_price1_null_rate": (float(row[3]) if row[3] is not None else None),
                                    "ask_price1_null_rate": (float(row[4]) if row[4] is not None else None),
                                    "last_price_null_rate": (float(row[5]) if row[5] is not None else None),
                                    "volume_null_rate": (float(row[6]) if row[6] is not None else None),
                                    "open_interest_null_rate": (float(row[7]) if row[7] is not None else None),
                                    "l5_fields_null_rate": (float(row[8]) if row[8] is not None else None),
                                    "price_jump_outliers": (int(row[9]) if row[9] is not None else 0),
                                    "volume_spike_outliers": (int(row[10]) if row[10] is not None else 0),
                                    "spread_anomaly_outliers": (int(row[11]) if row[11] is not None else 0),
                                    "updated_at": str(row[12] or "").strip() or None,
                                }
                            fq_ok = True
                        except Exception as e:
                            fq_error = str(e)
                            errors.append(f"field_quality: {fq_error}")
                            fq_ok = False
                        try:
                            cur.execute(gaps_sql, syms)
                            for row in cur.fetchall() or []:
                                s = str(row[0] or "").strip()
                                if not s:
                                    continue
                                gaps_by_sym[s] = {
                                    "trading_day": str(row[1] or "").strip() or None,
                                    "tick_count_actual": (int(row[2]) if row[2] is not None else None),
                                    "tick_count_expected_min": (int(row[3]) if row[3] is not None else None),
                                    "tick_count_expected_max": (int(row[4]) if row[4] is not None else None),
                                    "median_interval_ms": (int(row[5]) if row[5] is not None else None),
                                    "p95_interval_ms": (int(row[6]) if row[6] is not None else None),
                                    "p99_interval_ms": (int(row[7]) if row[7] is not None else None),
                                    "max_interval_ms": (int(row[8]) if row[8] is not None else None),
                                    "abnormal_gaps_count": (int(row[9]) if row[9] is not None else 0),
                                    "critical_gaps_count": (int(row[10]) if row[10] is not None else 0),
                                    "largest_gap_duration_ms": (int(row[11]) if row[11] is not None else None),
                                    "updated_at": str(row[12] or "").strip() or None,
                                }
                            gaps_ok = True
                        except Exception as e:
                            gaps_error = str(e)
                            errors.append(f"tick_gaps: {gaps_error}")
                            gaps_ok = False
        except Exception as e:
            questdb_error = str(e)
            errors.append(f"questdb: {questdb_error}")
            fq_ok = False
            gaps_ok = False

        # Compute derived metrics + per-symbol table.
        out_rows: list[dict[str, Any]] = []
        quality_scores: list[float] = []
        anomalies_total = 0
        for r in base_rows:
            sym = str(r.get("symbol") or "").strip()
            fq = fq_by_sym.get(sym) or {}
            gp = gaps_by_sym.get(sym) or {}

            rates = [
                fq.get("bid_price1_null_rate"),
                fq.get("ask_price1_null_rate"),
                fq.get("last_price_null_rate"),
                fq.get("volume_null_rate"),
                fq.get("open_interest_null_rate"),
            ]
            rates2 = [float(x) for x in rates if isinstance(x, (int, float))]
            score = None
            if rates2:
                m = float(sum(rates2)) / float(len(rates2))
                score = float(max(0.0, min(1.0, 1.0 - m)))
                quality_scores.append(score)

            outliers = int(fq.get("price_jump_outliers") or 0) + int(fq.get("volume_spike_outliers") or 0) + int(fq.get("spread_anomaly_outliers") or 0)
            gaps = int(gp.get("abnormal_gaps_count") or 0) + int(gp.get("critical_gaps_count") or 0)
            anomalies = int(outliers + gaps)
            anomalies_total += int(anomalies)

            # Simple status classification.
            st = "ok"
            try:
                if (fq.get("bid_price1_null_rate") is not None and float(fq.get("bid_price1_null_rate") or 0.0) > 0.01) or (
                    fq.get("ask_price1_null_rate") is not None and float(fq.get("ask_price1_null_rate") or 0.0) > 0.01
                ):
                    st = "error"
                elif int(gp.get("critical_gaps_count") or 0) > 0:
                    st = "warning"
                elif anomalies > 0:
                    st = "warning"
            except Exception:
                st = "unknown"

            if only_issues and st not in {"warning", "error"}:
                continue

            out_rows.append(
                {
                    "symbol": sym,
                    "status": st,
                    "last_tick_ts": r.get("last_tick_ts"),
                    "missing_days": r.get("missing_days"),
                    "expected_days": r.get("expected_days"),
                    "present_days": r.get("present_days"),
                    "field_quality_score": (round(score * 100.0, 2) if isinstance(score, (int, float)) else None),
                    "l1_null_rates": {k: fq.get(k) for k in ["bid_price1_null_rate", "ask_price1_null_rate", "last_price_null_rate", "volume_null_rate", "open_interest_null_rate"]},
                    "l5_fields_null_rate": fq.get("l5_fields_null_rate"),
                    "outliers": {
                        "price_jump": int(fq.get("price_jump_outliers") or 0),
                        "volume_spike": int(fq.get("volume_spike_outliers") or 0),
                        "spread_anomaly": int(fq.get("spread_anomaly_outliers") or 0),
                    },
                    "gaps": {
                        "abnormal": int(gp.get("abnormal_gaps_count") or 0),
                        "critical": int(gp.get("critical_gaps_count") or 0),
                        "largest_gap_duration_ms": gp.get("largest_gap_duration_ms"),
                        "p99_interval_ms": gp.get("p99_interval_ms"),
                        "max_interval_ms": gp.get("max_interval_ms"),
                    },
                    "ledger_days": {
                        "field_quality_day": fq.get("trading_day"),
                        "tick_gaps_day": gp.get("trading_day"),
                    },
                }
            )

        completeness_pct = None
        try:
            if isinstance(comp_summary, dict):
                total = float(comp_summary.get("symbols_total") or comp_summary.get("symbols") or 0.0)
                complete = float(comp_summary.get("symbols_complete") or 0.0)
                if total > 0:
                    completeness_pct = (complete / total) * 100.0
        except Exception:
            completeness_pct = None

        field_quality_pct = None
        try:
            if quality_scores:
                field_quality_pct = (float(sum(quality_scores)) / float(len(quality_scores))) * 100.0
        except Exception:
            field_quality_pct = None

        payload = {
            "ok": True,
            "exchange": ex,
            "var": v,
            "generated_at": _now_iso(),
            "kpis": {
                "completeness_pct": completeness_pct,
                "field_quality_pct": field_quality_pct,
                "anomalies_total": int(anomalies_total),
                "freshness_max_age_sec": freshness_max_age_sec,
            },
            "sources": {
                "contracts_snapshot": str(snap_path) if snap_path.exists() else None,
                "field_quality_ok": bool(fq_ok),
                "tick_gaps_ok": bool(gaps_ok),
                "field_quality_error": fq_error,
                "tick_gaps_error": gaps_error,
                "questdb_error": questdb_error,
            },
            "errors": errors,
            "count": int(len(out_rows)),
            "rows": out_rows,
        }
        if errors and not out_rows:
            payload["ok"] = False
            payload["error"] = errors[0]
        with _data_quality_lock:
            _data_quality_payload = dict(payload)
            _data_quality_cache_key = cache_key
            _data_quality_at = time.time()
        return payload

    @app.get("/api/data/quality-detail/{symbol}", response_class=JSONResponse)
    def api_data_quality_detail(request: Request, symbol: str, limit: int = 30) -> dict[str, Any]:
        """
        Return recent quality ledger rows for a symbol (best-effort).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = (symbol, limit)
        raise HTTPException(status_code=410, detail="data quality detail endpoint removed in current PRD phase")

    @app.post("/api/data/diagnose", response_class=JSONResponse)
    async def api_data_enqueue_diagnose(request: Request) -> dict[str, Any]:
        """
        Enqueue a data diagnose job (runs `ghtrader data diagnose ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = request
        raise HTTPException(status_code=410, detail="data diagnose endpoint removed in current PRD phase")

    @app.post("/api/data/repair", response_class=JSONResponse)
    async def api_data_enqueue_repair(request: Request) -> dict[str, Any]:
        """
        Enqueue a data repair job (runs `ghtrader data repair ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = request
        raise HTTPException(status_code=410, detail="data repair endpoint removed in current PRD phase")

    @app.post("/api/data/health", response_class=JSONResponse)
    async def api_data_enqueue_health(request: Request) -> dict[str, Any]:
        """
        Enqueue a data health job (runs `ghtrader data health ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = request
        raise HTTPException(status_code=410, detail="data health endpoint removed in current PRD phase")

    @app.post("/api/data/enqueue-l5-start", response_class=JSONResponse)
    async def api_data_enqueue_l5_start(request: Request) -> dict[str, Any]:
        """
        Enqueue an L5-start computation job (runs `ghtrader data l5-start ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = str(payload.get("var") or "cu").lower().strip()
        refresh_catalog = bool(payload.get("refresh_catalog", False))

        symbols: list[str] = []
        raw_syms = payload.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]

        argv = python_module_argv(
            "ghtrader.cli",
            "data",
            "l5-start",
            "--exchange",
            ex,
            "--var",
            v,
            "--refresh-catalog",
            ("1" if refresh_catalog else "0"),
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        for s in symbols:
            argv += ["--symbol", s]

        title = f"data-l5-start {ex}.{v}"
        store = request.app.state.job_store
        jm = request.app.state.job_manager

        # Dedupe: avoid duplicate l5-start jobs.
        try:
            active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
            for j in active:
                if str(j.title or "").startswith(title):
                    if j.pid is None and str(j.status or "") == "queued":
                        started = jm.start_queued_job(j.id) or j
                        return {"ok": True, "enqueued": [started.id], "count": 1, "deduped": True, "started": bool(started.pid)}
                    return {"ok": True, "enqueued": [j.id], "count": 1, "deduped": True}
        except Exception:
            pass

        # L5-start is index-backed and light: start immediately.
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return {"ok": True, "enqueued": [rec.id], "count": 1, "deduped": False}

    @app.post("/api/data/enqueue-main-l5-validate", response_class=JSONResponse)
    async def api_data_enqueue_main_l5_validate(request: Request) -> dict[str, Any]:
        """
        Enqueue a main_l5 validation job (runs `ghtrader data main-l5-validate ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = str(payload.get("var") or "cu").lower().strip()
        derived_symbol = str(payload.get("derived_symbol") or f"KQ.m@{ex}.{v}").strip()
        start = str(payload.get("start") or "").strip()
        end = str(payload.get("end") or "").strip()
        raw_check = payload.get("tqsdk_check", True)
        if isinstance(raw_check, str):
            tqsdk_check = raw_check.strip().lower() not in {"0", "false", "no", "off"}
        else:
            tqsdk_check = bool(raw_check)

        argv = python_module_argv(
            "ghtrader.cli",
            "data",
            "main-l5-validate",
            "--exchange",
            ex,
            "--var",
            v,
            "--symbol",
            derived_symbol,
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        if start:
            argv += ["--start", start]
        if end:
            argv += ["--end", end]
        argv += ["--tqsdk-check" if tqsdk_check else "--no-tqsdk-check"]

        for key, flag in [
            ("tqsdk_check_max_days", "--tqsdk-check-max-days"),
            ("tqsdk_check_max_segments", "--tqsdk-check-max-segments"),
            ("max_segments_per_day", "--max-segments-per-day"),
            ("gap_threshold_s", "--gap-threshold-s"),
            ("strict_ratio", "--strict-ratio"),
        ]:
            raw = payload.get(key)
            if raw not in (None, ""):
                argv += [flag, str(raw)]

        title = f"main-l5-validate {ex}.{v} {derived_symbol}"
        store = request.app.state.job_store
        jm = request.app.state.job_manager

        try:
            active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
            for j in active:
                if str(j.title or "").startswith(title):
                    if j.pid is None and str(j.status or "") == "queued":
                        started = jm.start_queued_job(j.id) or j
                        return {"ok": True, "enqueued": [started.id], "count": 1, "deduped": True, "started": bool(started.pid)}
                    return {"ok": True, "enqueued": [j.id], "count": 1, "deduped": True}
        except Exception:
            pass

        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return {"ok": True, "enqueued": [rec.id], "count": 1, "deduped": False}

    @app.get("/api/data/reports", response_class=JSONResponse)
    def api_data_reports(
        request: Request,
        kind: str = "diagnose",
        exchange: str = "SHFE",
        var: str = "cu",
        limit: int = 5,
    ) -> dict[str, Any]:
        """
        Return recent data reports written under runs/control/reports/.

        kind:
        - diagnose: runs/control/reports/data_diagnose/diagnose_exchange=..._var=..._*.json
        - repair: runs/control/reports/data_repair/repair_*.plan.json (+ optional .result.json)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        k = str(kind or "").strip().lower()
        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        lim = max(1, min(int(limit or 5), 50))

        rd = get_runs_dir()

        if k == "diagnose":
            rep_dir = rd / "control" / "reports" / "data_diagnose"
            if not rep_dir.exists():
                return {"ok": False, "error": "no_reports", "kind": k, "exchange": ex, "var": v, "reports": []}
            pat = f"diagnose_exchange={ex}_var={v}_*.json"
            paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
            reports: list[dict[str, Any]] = []
            for p in paths:
                obj = _read_json(p)
                if obj:
                    obj = dict(obj)
                    obj["_path"] = str(p)
                    reports.append(obj)
            return {"ok": True, "kind": k, "exchange": ex, "var": v, "count": int(len(reports)), "reports": reports}

        if k == "repair":
            rep_dir = rd / "control" / "reports" / "data_repair"
            if not rep_dir.exists():
                return {"ok": False, "error": "no_reports", "kind": k, "exchange": ex, "var": v, "reports": []}
            plan_paths = sorted(rep_dir.glob("repair_*.plan.json"), key=lambda p: p.stat().st_mtime_ns, reverse=True)
            reports2: list[dict[str, Any]] = []
            for p in plan_paths:
                plan = _read_json(p)
                if not isinstance(plan, dict):
                    continue
                if str(plan.get("exchange") or "").upper().strip() != ex:
                    continue
                if str(plan.get("var") or "").lower().strip() != v:
                    continue
                run_id = str(plan.get("run_id") or "").strip()
                res_path = rep_dir / f"repair_{run_id}.result.json" if run_id else None
                res_obj = _read_json(res_path) if (res_path and res_path.exists()) else None
                reports2.append(
                    {
                        "run_id": run_id or None,
                        "exchange": ex,
                        "var": v,
                        "plan": plan,
                        "result": res_obj,
                        "plan_path": str(p),
                        "result_path": (str(res_path) if res_path else None),
                    }
                )
                if len(reports2) >= lim:
                    break
            return {"ok": True, "kind": k, "exchange": ex, "var": v, "count": int(len(reports2)), "reports": reports2}

        raise HTTPException(status_code=400, detail="kind must be one of: diagnose, repair")

    @app.get("/api/data/l5-start", response_class=JSONResponse)
    def api_data_l5_start(request: Request, exchange: str = "SHFE", var: str = "cu", limit: int = 1) -> dict[str, Any]:
        """
        Return the most recent L5-start report(s) written under runs/control/reports/l5_start/.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        lim = max(1, min(int(limit or 1), 20))

        rd = get_runs_dir()
        rep_dir = rd / "control" / "reports" / "l5_start"
        if not rep_dir.exists():
            return {"ok": False, "error": "no_reports", "exchange": ex, "var": v, "reports": []}

        pat = f"l5_start_exchange={ex}_var={v}_*.json"
        paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
        reports: list[dict[str, Any]] = []
        for p in paths:
            obj = _read_json(p)
            if obj:
                obj = dict(obj)
                obj["_path"] = str(p)
                reports.append(obj)
        return {"ok": True, "exchange": ex, "var": v, "count": int(len(reports)), "reports": reports}

    @app.get("/api/data/main-l5-validate", response_class=JSONResponse)
    def api_data_main_l5_validate(
        request: Request, exchange: str = "SHFE", var: str = "cu", symbol: str = "", limit: int = 1
    ) -> dict[str, Any]:
        """
        Return recent main_l5 validation report(s) written under runs/control/reports/main_l5_validate/.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        derived_symbol = str(symbol or f"KQ.m@{ex}.{v}").strip()
        lim = max(1, min(int(limit or 1), 20))

        rd = get_runs_dir()
        rep_dir = rd / "control" / "reports" / "main_l5_validate"
        if not rep_dir.exists():
            return {"ok": False, "error": "no_reports", "exchange": ex, "var": v, "reports": []}

        sym = re.sub(r"[^A-Za-z0-9]+", "_", derived_symbol).strip("_").lower()
        pat = f"main_l5_validate_exchange={ex}_var={v}_symbol={sym}_*.json"
        paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
        reports: list[dict[str, Any]] = []
        for p in paths:
            obj = _read_json(p)
            if obj:
                obj = dict(obj)
                obj["_path"] = str(p)
                reports.append(obj)
        return {"ok": True, "exchange": ex, "var": v, "symbol": derived_symbol, "count": int(len(reports)), "reports": reports}

    @app.get("/api/data/main-l5-validate-summary", response_class=JSONResponse)
    def api_data_main_l5_validate_summary(
        request: Request, exchange: str = "SHFE", var: str = "cu", symbol: str = "", limit: int = 30
    ) -> dict[str, Any]:
        """
        Return QuestDB-backed validation summary rows + overview.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        derived_symbol = str(symbol or f"KQ.m@{ex}.{v}").strip()
        lim = max(1, min(int(limit or 30), 3650))
        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import (
                fetch_latest_main_l5_validate_summary,
                fetch_main_l5_validate_overview,
            )

            cfg = make_questdb_query_config_from_env()
            overview = fetch_main_l5_validate_overview(cfg=cfg, symbol=derived_symbol)
            rows = fetch_latest_main_l5_validate_summary(cfg=cfg, symbol=derived_symbol, limit=lim)
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "overview": overview,
                "count": int(len(rows)),
                "rows": rows,
            }
        except Exception as e:
            return {"ok": False, "error": str(e), "exchange": ex, "var": v, "symbol": derived_symbol, "rows": []}

    @app.get("/api/data/main-l5-validate-gaps", response_class=JSONResponse)
    def api_data_main_l5_validate_gaps(
        request: Request,
        exchange: str = "SHFE",
        var: str = "cu",
        symbol: str = "",
        trading_day: str = "",
        start: str = "",
        end: str = "",
        min_duration_s: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        """
        Return QuestDB-backed gap details for main_l5 validation.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        derived_symbol = str(symbol or f"KQ.m@{ex}.{v}").strip()
        lim = max(1, min(int(limit or 500), 10000))
        day = None
        start_day = None
        end_day = None
        if trading_day:
            try:
                day = date.fromisoformat(str(trading_day)[:10])
            except Exception:
                day = None
        if start:
            try:
                start_day = date.fromisoformat(str(start)[:10])
            except Exception:
                start_day = None
        if end:
            try:
                end_day = date.fromisoformat(str(end)[:10])
            except Exception:
                end_day = None
        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import list_main_l5_validate_gaps

            cfg = make_questdb_query_config_from_env()
            gaps = list_main_l5_validate_gaps(
                cfg=cfg,
                symbol=derived_symbol,
                trading_day=day,
                start_day=start_day,
                end_day=end_day,
                min_duration_s=(int(min_duration_s) if int(min_duration_s or 0) > 0 else None),
                limit=lim,
            )
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "count": int(len(gaps)),
                "gaps": gaps,
            }
        except Exception as e:
            return {"ok": False, "error": str(e), "exchange": ex, "var": v, "symbol": derived_symbol, "gaps": []}

    @app.get("/api/contracts", response_class=JSONResponse)
    def api_contracts(
        request: Request,
        exchange: str = "SHFE",
        var: str = "cu",
        refresh: str = "0",
    ) -> dict[str, Any]:
        """
        Contract explorer backend (QuestDB-first, snapshot-backed per PRD §5.11).

        The Contracts table is always served from a cached snapshot produced by a background
        job (`contracts-snapshot-build`). This keeps the request path fast and responsive.

        Query params:
        - exchange: SHFE
        - var: cu|au|ag
        - refresh: 1 to force refresh (enqueues snapshot rebuild job if stale/missing)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = (exchange, var, refresh)
        raise HTTPException(status_code=410, detail="contracts explorer endpoint removed in current PRD phase")

    @app.post("/api/contracts/enqueue-fill", response_class=JSONResponse)
    async def api_contracts_enqueue_fill(request: Request) -> dict[str, Any]:
        """
        Enqueue per-contract download jobs (runs `ghtrader download --symbol ...`).

        Payload:
        - symbols: optional list[str] (explicit)
        - scope: "missing" (default) | "all"
        - exchange/var: used when scope is set (to select contracts)
        - start/end: optional manual bounds (YYYY-MM-DD) used when active-ranges are missing
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = request
        raise HTTPException(status_code=410, detail="contracts fill endpoint removed in current PRD phase")

    @app.post("/api/contracts/enqueue-update", response_class=JSONResponse)
    async def api_contracts_enqueue_update(request: Request) -> dict[str, Any]:
        """
        Enqueue an Update job (runs `ghtrader update ...`) to check remote contract updates.

        Payload:
        - exchange/var: required for variety update
        - symbols: optional list[str] to update a subset
        - recent_expired_days: optional int (default 10)
        - refresh_catalog: optional bool (default true)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = request
        raise HTTPException(status_code=410, detail="contracts update endpoint removed in current PRD phase")

    @app.post("/api/contracts/enqueue-audit", response_class=JSONResponse)
    async def api_contracts_enqueue_audit(request: Request) -> dict[str, Any]:
        """
        Start per-contract audit jobs (ticks scope, symbol-filtered).

        Unlike download/probe, audits are local and start immediately.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        _ = request
        raise HTTPException(status_code=410, detail="contracts audit endpoint removed in current PRD phase")

    @app.post("/api/jobs", response_class=JSONResponse)
    async def api_create_job(request: Request) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        title = str(payload.get("title") or "job")
        argv = payload.get("argv")
        if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
            raise HTTPException(status_code=400, detail="argv must be a list[str]")

        cwd = Path(str(payload.get("cwd") or Path.cwd()))
        rec = jm.start_job(JobSpec(title=title, argv=list(argv), cwd=cwd))
        return {"job": asdict(rec)}

    @app.post("/api/jobs/{job_id}/cancel", response_class=JSONResponse)
    def api_cancel_job(request: Request, job_id: str) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ok = jm.cancel_job(job_id)
        return {"ok": bool(ok)}

    @app.post("/api/jobs/cancel-batch", response_class=JSONResponse)
    async def api_cancel_jobs_batch(request: Request) -> dict[str, Any]:
        """
        Batch cancel jobs by kind/status.

        Intended for dashboard UX (e.g. cancel all download jobs).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        kinds_raw = payload.get("kinds")
        statuses_raw = payload.get("statuses")
        include_unstarted_queued = bool(payload.get("include_unstarted_queued", False))

        if kinds_raw is None:
            kinds: set[str] = set()
        elif isinstance(kinds_raw, list) and all(isinstance(x, str) for x in kinds_raw):
            kinds = {str(x).strip() for x in kinds_raw if str(x).strip()}
        else:
            raise HTTPException(status_code=400, detail="kinds must be list[str]")

        if statuses_raw is None:
            statuses = {"running", "queued"}
        elif isinstance(statuses_raw, list) and all(isinstance(x, str) for x in statuses_raw):
            statuses = {str(x).strip().lower() for x in statuses_raw if str(x).strip()}
        else:
            raise HTTPException(status_code=400, detail="statuses must be list[str]")

        # Default: cancel Phase-0 data pipeline jobs.
        if not kinds:
            kinds = {"main_schedule", "main_l5"}

        def _job_kind_from_command(cmd: list[str] | None) -> str:
            parts = [str(p) for p in (cmd or [])]
            if not parts:
                return "unknown"
            if "ghtrader" in parts:
                i = parts.index("ghtrader")
                if i + 1 < len(parts):
                    return parts[i + 1].replace("-", "_")
            if "ghtrader.cli" in parts:
                i = parts.index("ghtrader.cli")
                if i + 1 < len(parts):
                    return parts[i + 1].replace("-", "_")
            if "-m" in parts:
                i = parts.index("-m")
                if i + 2 < len(parts) and parts[i + 1].endswith("ghtrader.cli"):
                    return parts[i + 2].replace("-", "_")
            return "unknown"

        matched: list[str] = []
        cancelled: list[str] = []
        failed: list[dict[str, Any]] = []

        # Consider the recent job window; active jobs should be near the top.
        jobs = store.list_jobs(limit=2000)
        for job in jobs:
            st = str(job.status or "").lower().strip()
            if st not in statuses:
                continue

            # Determine kind from argv; if unknown, skip.
            kind = _job_kind_from_command(list(job.command or []))
            if kind not in kinds:
                continue

            # Skip unstarted queued jobs unless explicitly requested.
            if st == "queued" and job.pid is None and not include_unstarted_queued:
                continue

            matched.append(job.id)

            ok = False
            try:
                ok = bool(jm.cancel_job(job.id))
            except Exception as e:
                ok = False
                failed.append({"job_id": job.id, "error": str(e), "kind": kind, "status": st})
            if ok:
                cancelled.append(job.id)
            else:
                failed.append({"job_id": job.id, "error": "cancel_failed", "kind": kind, "status": st})

        return {
            "ok": True,
            "kinds": sorted(list(kinds)),
            "statuses": sorted(list(statuses)),
            "matched": int(len(matched)),
            "cancelled": int(len(cancelled)),
            "failed": failed,
            "job_ids": {"matched": matched, "cancelled": cancelled},
        }

    @app.get("/api/jobs/{job_id}/log", response_class=PlainTextResponse)
    def api_job_log(request: Request, job_id: str, max_bytes: int = 64000) -> str:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return jm.read_log_tail(job_id, max_bytes=int(max_bytes))

    @app.get("/api/jobs/{job_id}/log/download")
    def api_job_log_download(request: Request, job_id: str) -> Any:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if not job.log_path:
            raise HTTPException(status_code=404, detail="No log path for job")

        p = Path(str(job.log_path)).resolve()
        if not p.exists():
            raise HTTPException(status_code=404, detail="Log file not found")

        logs_root = _logs_dir(get_runs_dir()).resolve()
        if logs_root not in p.parents:
            raise HTTPException(status_code=400, detail="Invalid log path")

        return FileResponse(path=str(p), filename=p.name, media_type="text/plain")


    @app.get("/api/jobs/{job_id}/progress", response_class=JSONResponse)
    def api_job_progress(request: Request, job_id: str) -> dict[str, Any]:
        """Get structured progress for a job."""
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")

        from ghtrader.control.progress import get_job_progress

        progress = get_job_progress(job_id=job.id, runs_dir=get_runs_dir())
        if progress is None:
            # No progress file - return minimal info
            return {
                "job_id": job.id,
                "job_status": job.status,
                "available": False,
                "message": "No progress tracking for this job",
            }

        # Attach job metadata
        progress["job_status"] = job.status
        progress["available"] = True
        return progress


    @app.post("/api/questdb/query", response_class=JSONResponse)
    async def api_questdb_query(request: Request) -> dict[str, Any]:
        """
        Read-only QuestDB query endpoint (guarded).

        Intended for local SSH-forwarded dashboard use only.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        query = str(payload.get("query") or "").strip()
        try:
            limit = int(payload.get("limit") or 200)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.queries import query_sql_read_only

            cfg = make_questdb_query_config_from_env()
            cols, rows = query_sql_read_only(cfg=cfg, query=query, limit=int(limit), connect_timeout_s=2)
            return {"ok": True, "columns": cols, "rows": rows}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except RuntimeError as e:
            # Missing dependency / config issues.
            raise HTTPException(status_code=500, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/api/dashboard/summary", response_class=JSONResponse)
    def api_dashboard_summary(request: Request) -> dict[str, Any]:
        """
        Aggregated KPIs for the dashboard home page.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        global _dash_summary_at, _dash_summary_payload
        now = time.time()
        with _dash_summary_lock:
            if _dash_summary_payload is not None and (now - float(_dash_summary_at)) <= float(_DASH_SUMMARY_TTL_S):
                return dict(_dash_summary_payload)

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()

        # Job counts
        jobs = store.list_jobs(limit=200)
        running_count = len([j for j in jobs if j.status == "running"])
        queued_count = len([j for j in jobs if j.status == "queued"])

        # QuestDB reachability
        questdb_ok = False
        try:
            from ghtrader.questdb.client import questdb_reachable_pg

            q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
            questdb_ok = bool(q.get("ok"))
        except Exception:
            questdb_ok = False

        # Data symbols counts (Phase-1/2 deferred)
        data_symbols = 0
        data_status = "deferred"

        # Model count (count actual model files, not arbitrary artifacts/ subdirs)
        model_files: list[dict[str, Any]] = []
        model_status = ""
        try:
            model_files = _scan_model_files(artifacts_dir)
            if model_files:
                latest = sorted(model_files, key=lambda x: float(x.get("mtime") or 0.0), reverse=True)[0]
                model_status = f"{latest.get('model_type')} {latest.get('symbol')} h{latest.get('horizon')}"
        except Exception:
            model_files = []
            model_status = "error"
        model_count = int(len({(m.get("namespace"), m.get("symbol"), m.get("model_type"), m.get("horizon")) for m in model_files}))

        # Trading status
        trading_mode = "idle"
        trading_status = "No active runs"
        try:
            trading_dir = runs_dir / "trading"
            if trading_dir.exists():
                runs = sorted([d for d in trading_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)
                if runs:
                    latest = runs[0]
                    # Check if there's recent activity (snapshots.jsonl updated in last 5 minutes)
                    snapshots_file = latest / "snapshots.jsonl"
                    if snapshots_file.exists():
                        mtime = snapshots_file.stat().st_mtime
                        import time as t
                        if t.time() - mtime < 300:  # 5 minutes
                            trading_mode = "active"
                            trading_status = f"Run: {latest.name[:8]}"
        except Exception:
            pass

        # Pipeline status (simplified)
        pipeline = {
            "ingest": {"state": "ok" if data_symbols > 0 else "warn", "text": f"{data_symbols} symbols"},
            "sync": {"state": "ok" if questdb_ok else "error", "text": "connected" if questdb_ok else "offline"},
            "schedule": {"state": "unknown", "text": "--"},
            "main_l5": {"state": "unknown", "text": "--"},
            "build": {"state": "unknown", "text": "--"},
            "train": {"state": "ok" if model_count > 0 else "warn", "text": f"{model_count} models"},
        }

        # Check schedule exists (QuestDB canonical table)
        try:
            if questdb_ok:
                from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
                from ghtrader.questdb.main_schedule import MAIN_SCHEDULE_TABLE_V2

                cfg = make_questdb_query_config_from_env()
                with connect_pg(cfg, connect_timeout_s=1) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT count() FROM {MAIN_SCHEDULE_TABLE_V2} WHERE exchange=%s AND variety=%s",
                            ["SHFE", "cu"],
                        )
                        n = int((cur.fetchone() or [0])[0] or 0)
                if n > 0:
                    pipeline["schedule"] = {"state": "ok", "text": "cu ready"}
        except Exception:
            pass

        # main_l5 status (Phase-1/2 deferred)
        pipeline["main_l5"] = {"state": "deferred", "text": "deferred"}

        # Check features/labels builds exist (QuestDB build tables)
        try:
            if questdb_ok:
                from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
                from ghtrader.questdb.features_labels import FEATURE_BUILDS_TABLE_V2

                cfg = make_questdb_query_config_from_env()
                with connect_pg(cfg, connect_timeout_s=1) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT count(DISTINCT symbol) FROM {FEATURE_BUILDS_TABLE_V2} WHERE dataset_version='v2'")
                        n = int((cur.fetchone() or [0])[0] or 0)
                if n > 0:
                    pipeline["build"] = {"state": "ok", "text": f"{n} symbols"}
        except Exception:
            pass

        payload = {
            "ok": True,
            "running_count": running_count,
            "queued_count": queued_count,
            "questdb_ok": questdb_ok,
            "data_symbols": data_symbols,
            "data_symbols_v2": int(data_symbols),
            "data_status": data_status,
            "model_count": model_count,
            "model_status": model_status,
            "trading_mode": trading_mode,
            "trading_status": trading_status,
            "pipeline": pipeline,
        }
        with _dash_summary_lock:
            _dash_summary_payload = dict(payload)
            _dash_summary_at = time.time()
        return payload

    @app.get("/api/ui/status", response_class=JSONResponse)
    def api_ui_status(request: Request) -> dict[str, Any]:
        """
        Lightweight status endpoint for global UI chrome (topbar).

        Cached to avoid doing QuestDB connection checks too frequently.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        global _ui_status_at, _ui_status_payload
        now = time.time()
        with _ui_status_lock:
            if _ui_status_payload is not None and (now - float(_ui_status_at)) <= float(_UI_STATUS_TTL_S):
                return dict(_ui_status_payload)

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()
        artifacts_dir = get_artifacts_dir()

        # Job counts
        jobs = store.list_jobs(limit=200)
        running_count = len([j for j in jobs if j.status == "running"])
        queued_count = len([j for j in jobs if j.status == "queued"])

        # GPU status (best-effort; comes from the same cached snapshot used by /api/system)
        gpu_status = "unknown"
        try:
            from ghtrader.control.system_info import system_snapshot

            snap = system_snapshot(
                data_dir=data_dir,
                runs_dir=runs_dir,
                artifacts_dir=artifacts_dir,
                include_dir_sizes=False,
                refresh="none",
            )
            gpu_status = str(((snap.get("gpu") or {}) if isinstance(snap.get("gpu"), dict) else {}).get("status") or "unknown")
        except Exception:
            gpu_status = "unknown"

        # QuestDB reachability (best-effort)
        questdb_ok = False
        try:
            from ghtrader.questdb.client import questdb_reachable_pg

            q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
            questdb_ok = bool(q.get("ok"))
        except Exception:
            questdb_ok = False

        payload = {
            "ok": True,
            "questdb_ok": bool(questdb_ok),
            "gpu_status": gpu_status,
            "running_count": int(running_count),
            "queued_count": int(queued_count),
        }
        with _ui_status_lock:
            _ui_status_payload = dict(payload)
            _ui_status_at = time.time()
        return payload

    @app.get("/api/models/inventory", response_class=JSONResponse)
    def api_models_inventory(request: Request) -> dict[str, Any]:
        """
        List trained model artifacts from artifacts/ directory.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        artifacts_dir = get_artifacts_dir()
        models: list[dict[str, Any]] = []

        try:
            if not artifacts_dir.exists():
                return {"ok": True, "models": []}

            def _human_size(size_bytes: int) -> str:
                for unit in ["B", "KB", "MB", "GB"]:
                    if size_bytes < 1024:
                        return f"{size_bytes:.1f} {unit}"
                    size_bytes /= 1024
                return f"{size_bytes:.1f} TB"

            from datetime import datetime, timezone

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

        return {"ok": True, "models": models}

    @app.get("/api/models/benchmarks", response_class=JSONResponse)
    def api_models_benchmarks(request: Request, limit: int = 20) -> dict[str, Any]:
        """
        List recent benchmark summaries from runs/benchmarks/**/<run_id>.json.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        global _benchmarks_at, _benchmarks_payload
        now = time.time()
        with _benchmarks_lock:
            if _benchmarks_payload is not None and (now - float(_benchmarks_at)) <= float(_BENCHMARKS_TTL_S):
                payload = dict(_benchmarks_payload)
                # Apply limit at read time (cheap)
                payload["benchmarks"] = list(payload.get("benchmarks") or [])[: max(1, int(limit or 20))]
                return payload

        runs_dir = get_runs_dir()
        root = runs_dir / "benchmarks"
        if not root.exists():
            return {"ok": True, "benchmarks": []}

        out: list[dict[str, Any]] = []
        from datetime import datetime, timezone

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
        payload2 = {"ok": True, "benchmarks": out[: max(1, int(limit or 20))]}
        with _benchmarks_lock:
            _benchmarks_payload = dict(payload2)
            _benchmarks_at = time.time()
        return payload2

    # ---------------------------------------------------------------------
    # Trading artifacts helpers
    # ---------------------------------------------------------------------

    def _read_json_file(path: Path) -> dict[str, Any] | None:
        try:
            if not path.exists():
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _read_redis_json(key: str) -> dict[str, Any] | None:
        """
        Best-effort sync Redis JSON read for hot state/desired keys.
        Falls back to file-based mirrors when Redis is unavailable.
        """
        try:
            import redis as redis_sync

            client = redis_sync.Redis(
                host="localhost",
                port=6379,
                db=0,
                decode_responses=True,
                socket_connect_timeout=0.2,
                socket_timeout=0.2,
            )
            raw = client.get(str(key))
            if not raw:
                return None
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def _read_jsonl_tail(path: Path, *, max_lines: int, max_bytes: int = 256 * 1024) -> list[dict[str, Any]]:
        """
        Best-effort tail reader for JSONL artifacts (snapshots/events).
        Returns parsed objects in chronological order (oldest -> newest).
        """
        try:
            if not path.exists() or int(max_lines) <= 0:
                return []
            with open(path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                offset = max(0, size - int(max_bytes))
                f.seek(offset)
                chunk = f.read().decode("utf-8", errors="ignore")
            lines = [ln for ln in chunk.splitlines() if ln.strip()]
            out: list[dict[str, Any]] = []
            for ln in lines[-int(max_lines) :]:
                try:
                    obj = json.loads(ln)
                    if isinstance(obj, dict):
                        out.append(obj)
                except Exception:
                    continue
            return out
        except Exception:
            return []

    def _read_last_jsonl_obj(path: Path) -> dict[str, Any] | None:
        out = _read_jsonl_tail(path, max_lines=1)
        return out[-1] if out else None

    @app.get("/api/accounts", response_class=JSONResponse)
    def api_accounts(request: Request) -> dict[str, Any]:
        """
        List broker account profiles (runs/control/accounts.env) + last verification + gateway/strategy summary.
        """
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

    @app.post("/api/accounts/upsert", response_class=JSONResponse)
    async def api_accounts_upsert(request: Request) -> dict[str, Any]:
        """
        Upsert a broker account profile into runs/control/accounts.env.

        Payload: {profile, broker_id, account_id, password}
        """
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

    @app.post("/api/accounts/delete", response_class=JSONResponse)
    async def api_accounts_delete(request: Request) -> dict[str, Any]:
        """
        Delete a broker account profile from runs/control/accounts.env.

        Payload: {profile}
        Also removes runs/control/cache/accounts/account=<profile>.json verify cache.
        """
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

        # Remove verify cache if present.
        verify_path = runs_dir / "control" / "cache" / "accounts" / f"account={p}.json"
        try:
            if verify_path.exists():
                verify_path.unlink()
        except Exception:
            pass

        return {"ok": True, "profile": p}

    @app.get("/api/brokers", response_class=JSONResponse)
    def api_brokers(request: Request) -> dict[str, Any]:
        """
        Return supported broker IDs for TqSdk (best-effort).

        Implementation: fetch + parse ShinnyTech list, cache to runs/control/cache/brokers.json,
        and fall back to a small built-in list when unavailable.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        runs_dir = get_runs_dir()
        brokers, source = _get_supported_brokers(runs_dir=runs_dir)
        return {"ok": True, "brokers": brokers, "source": source, "generated_at": _now_iso()}

    @app.post("/api/accounts/enqueue-verify", response_class=JSONResponse)
    async def api_accounts_enqueue_verify(request: Request) -> dict[str, Any]:
        """
        Enqueue a broker account verification job (runs `ghtrader account verify ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        prof = str(payload.get("account_profile") or "default").strip() or "default"

        argv = python_module_argv("ghtrader.cli", "account", "verify", "--account", prof, "--json")
        title = f"account-verify {prof}"
        jm = request.app.state.job_manager
        rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return {"ok": True, "enqueued": [rec.id], "count": 1}

    # ---------------------------------------------------------------------
    # Gateway (AccountGateway; OMS/EMS per account profile)
    # ---------------------------------------------------------------------

    @app.get("/api/gateway/status", response_class=JSONResponse)
    def api_gateway_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
        """
        Read gateway state for a broker account profile (Redis hot state first, file mirror fallback).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(str(account_profile or "default"))
        except Exception:
            prof = str(account_profile or "default").strip() or "default"

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

    @app.get("/api/gateway/list", response_class=JSONResponse)
    def api_gateway_list(request: Request, limit: int = 200) -> dict[str, Any]:
        """
        List gateway profiles + summary state.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

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

    @app.post("/api/gateway/desired", response_class=JSONResponse)
    async def api_gateway_desired(request: Request) -> dict[str, Any]:
        """
        Upsert gateway desired.json for a profile.

        Payload:
          {account_profile, desired: {mode, symbols, executor, sim_account, confirm_live, max_abs_position, ...}}
        """
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

    @app.post("/api/gateway/command", response_class=JSONResponse)
    async def api_gateway_command(request: Request) -> dict[str, Any]:
        """
        Append an operator command (Redis stream primary; file mirror for audit).

        Payload:
          {account_profile, type, params?}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"
        cmd_type = str(payload.get("type") or "").strip()
        if not cmd_type:
            raise HTTPException(status_code=400, detail="missing type")

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

            # Primary command bus: Redis stream.
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

    # ---------------------------------------------------------------------
    # Strategy (AI StrategyRunner; consumes gateway state, writes targets)
    # ---------------------------------------------------------------------

    def _artifact_age_sec(p: Path) -> float | None:
        try:
            return float(time.time() - float(p.stat().st_mtime))
        except Exception:
            return None

    def _health_error(state: Any) -> str:
        try:
            if isinstance(state, dict) and isinstance(state.get("health"), dict):
                return str(state["health"].get("error") or "")
        except Exception:
            return ""
        return ""

    def _status_from_desired_and_state(*, root_exists: bool, desired_mode: str, state_age: float | None) -> str:
        """
        Shared status mapping used by the Trading Console.

        Statuses: not_initialized | desired_idle | starting | running | degraded
        """
        if not bool(root_exists):
            return "not_initialized"
        dm = str(desired_mode or "").strip().lower()
        if dm in {"", "idle"}:
            if state_age is not None and float(state_age) < 15.0:
                return "running"
            return "desired_idle"
        # desired is non-idle
        if state_age is None:
            return "starting"
        return "running" if float(state_age) < 15.0 else "degraded"

    @app.get("/api/strategy/status", response_class=JSONResponse)
    def api_strategy_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
        """
        Read StrategyRunner stable state for a profile (Redis hot state first, file mirror fallback).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(str(account_profile or "default"))
        except Exception:
            prof = str(account_profile or "default").strip() or "default"

        root = runs_dir / "strategy" / f"account={prof}"
        st_path = root / "state.json"
        desired_path = root / "desired.json"
        state = _read_redis_json(f"ghtrader:strategy:state:{prof}") or (_read_json_file(st_path) if st_path.exists() else None)
        desired = _read_redis_json(f"ghtrader:strategy:desired:{prof}") or (_read_json_file(desired_path) if desired_path.exists() else None)
        targets = _read_json_file((runs_dir / "gateway" / f"account={prof}" / "targets.json"))  # strategy output surface

        age = _artifact_age_sec(st_path) if st_path.exists() else None
        desired_cfg = desired.get("desired") if isinstance(desired, dict) and isinstance(desired.get("desired"), dict) else (desired or {})
        desired_mode = str((desired_cfg or {}).get("mode") or "idle")
        status = _status_from_desired_and_state(root_exists=root.exists(), desired_mode=desired_mode, state_age=age)

        active_job_id = None
        try:
            store2: JobStore = request.app.state.job_store
            for j in store2.list_active_jobs():
                if _is_strategy_job(j.command):
                    ap = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap = canonical_account_profile(ap)
                    except Exception:
                        ap = str(ap).strip().lower() or "default"
                    if ap == prof:
                        active_job_id = j.id
                        break
        except Exception:
            active_job_id = None

        return {
            "ok": True,
            "account_profile": prof,
            "root": str(root),
            "exists": bool(root.exists()),
            "status": status,
            "state_age_sec": age,
            "error": _health_error(state),
            "active_job_id": active_job_id,
            "desired": desired,
            "state": state,
            "targets": targets,
            "generated_at": _now_iso(),
        }

    @app.post("/api/strategy/desired", response_class=JSONResponse)
    async def api_strategy_desired(request: Request) -> dict[str, Any]:
        """
        Upsert StrategyRunner desired.json for a profile.

        Payload:
          {account_profile, desired: {mode, symbols, model_name, horizon, threshold_up, threshold_down, position_size, artifacts_dir, poll_interval_sec}}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"
        desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else {}

        try:
            from ghtrader.trading.strategy_control import StrategyDesired, write_strategy_desired
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
            d = StrategyDesired(
                mode=str(desired_in.get("mode") or "idle").strip().lower() in {"run", "running", "active", "on"} and "run" or "idle",  # type: ignore[arg-type]
                symbols=list(desired_in.get("symbols")) if isinstance(desired_in.get("symbols"), list) else None,
                model_name=str(desired_in.get("model_name") or "xgboost").strip() or "xgboost",
                horizon=int(desired_in.get("horizon") or 50),
                threshold_up=float(desired_in.get("threshold_up") or 0.6),
                threshold_down=float(desired_in.get("threshold_down") or 0.6),
                position_size=int(desired_in.get("position_size") or 1),
                artifacts_dir=str(desired_in.get("artifacts_dir") or "artifacts").strip() or "artifacts",
                poll_interval_sec=float(desired_in.get("poll_interval_sec") or 0.5),
            )
            write_strategy_desired(runs_dir=get_runs_dir(), profile=prof, desired=d)
            return {"ok": True, "account_profile": prof}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"strategy_desired_failed: {e}")

    # ---------------------------------------------------------------------
    # Explicit Start/Stop endpoints for Gateway and Strategy
    # ---------------------------------------------------------------------

    @app.post("/api/gateway/start", response_class=JSONResponse)
    async def api_gateway_start(request: Request) -> dict[str, Any]:
        """
        Start a gateway job for the selected profile.

        Payload: {account_profile, mode?, symbols?, confirm_live?, ...}

        If mode/symbols/confirm_live are provided, desired.json is updated first.
        Then starts `ghtrader gateway run --account <profile>` as a job.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        runs_dir = get_runs_dir()

        # Optionally update desired state if mode/symbols provided
        desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else None
        if desired_in:
            try:
                from ghtrader.tq.gateway import GatewayDesired, write_gateway_desired

                d = GatewayDesired(
                    mode=str(desired_in.get("mode") or "sim").strip(),  # type: ignore[arg-type]
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
                write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=d)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"gateway_desired_failed: {e}")

        # Check if already running
        store2: JobStore = request.app.state.job_store
        for j in store2.list_active_jobs():
            if _is_gateway_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    return {"ok": False, "error": "already_running", "job_id": j.id, "message": f"Gateway for profile '{prof}' is already running."}

        # Start gateway job
        argv = python_module_argv("ghtrader.cli", "gateway", "run", "--account", prof, "--runs-dir", str(runs_dir))
        title = f"gateway {prof}"
        jm2: JobManager = request.app.state.job_manager
        rec = jm2.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return {"ok": True, "account_profile": prof, "job_id": rec.id}

    @app.post("/api/gateway/stop", response_class=JSONResponse)
    async def api_gateway_stop(request: Request) -> dict[str, Any]:
        """
        Stop the running gateway job for the selected profile.

        Payload: {account_profile}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        store2: JobStore = request.app.state.job_store
        jm2: JobManager = request.app.state.job_manager
        cancelled = False
        job_id = None

        for j in store2.list_active_jobs():
            if _is_gateway_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    job_id = j.id
                    cancelled = bool(jm2.cancel_job(j.id))
                    break

        # Also set desired mode to idle so supervisor doesn't restart
        runs_dir = get_runs_dir()
        try:
            from ghtrader.tq.gateway import GatewayDesired, read_gateway_desired, write_gateway_desired

            existing = read_gateway_desired(runs_dir=runs_dir, profile=prof)
            if existing and existing.mode != "idle":
                existing.mode = "idle"  # type: ignore[assignment]
                write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=existing)
        except Exception:
            pass

        if job_id:
            return {"ok": True, "account_profile": prof, "job_id": job_id, "cancelled": cancelled}
        return {"ok": False, "error": "not_running", "message": f"No running gateway job for profile '{prof}'."}

    @app.post("/api/strategy/start", response_class=JSONResponse)
    async def api_strategy_start(request: Request) -> dict[str, Any]:
        """
        Start a strategy job for the selected profile.

        Payload: {account_profile, desired?: {mode, symbols, model_name, horizon, ...}}

        If desired is provided, desired.json is updated first.
        Then starts `ghtrader strategy run --account <profile> ...` as a job.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        runs_dir = get_runs_dir()

        # Optionally update desired state
        desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else None
        if desired_in:
            try:
                from ghtrader.trading.strategy_control import StrategyDesired, write_strategy_desired

                d = StrategyDesired(
                    mode="run",  # type: ignore[arg-type]
                    symbols=list(desired_in.get("symbols")) if isinstance(desired_in.get("symbols"), list) else None,
                    model_name=str(desired_in.get("model_name") or "xgboost").strip() or "xgboost",
                    horizon=int(desired_in.get("horizon") or 50),
                    threshold_up=float(desired_in.get("threshold_up") or 0.6),
                    threshold_down=float(desired_in.get("threshold_down") or 0.6),
                    position_size=int(desired_in.get("position_size") or 1),
                    artifacts_dir=str(desired_in.get("artifacts_dir") or "artifacts").strip() or "artifacts",
                    poll_interval_sec=float(desired_in.get("poll_interval_sec") or 0.5),
                )
                write_strategy_desired(runs_dir=runs_dir, profile=prof, desired=d)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"strategy_desired_failed: {e}")

        # Read desired config to build argv
        try:
            from ghtrader.trading.strategy_control import read_strategy_desired

            cfg = read_strategy_desired(runs_dir=runs_dir, profile=prof)
        except Exception:
            cfg = None

        if not cfg:
            return {"ok": False, "error": "no_desired", "message": "No strategy desired.json found. Please set desired state first."}

        symbols = cfg.symbols or []
        if not symbols:
            return {"ok": False, "error": "no_symbols", "message": "Symbols required for strategy start."}

        # Check if already running
        store2: JobStore = request.app.state.job_store
        for j in store2.list_active_jobs():
            if _is_strategy_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    return {"ok": False, "error": "already_running", "job_id": j.id, "message": f"Strategy for profile '{prof}' is already running."}

        # Build argv matching _strategy_supervisor_tick
        model_name = cfg.model_name or "xgboost"
        horizon = str(cfg.horizon or 50)
        threshold_up = str(cfg.threshold_up or 0.6)
        threshold_down = str(cfg.threshold_down or 0.6)
        position_size = str(cfg.position_size or 1)
        artifacts_dir = cfg.artifacts_dir or "artifacts"
        poll_interval_sec = str(cfg.poll_interval_sec or 0.5)

        argv = python_module_argv(
            "ghtrader.cli",
            "strategy",
            "run",
            "--account",
            prof,
            "--model",
            model_name,
            "--horizon",
            horizon,
            "--threshold-up",
            threshold_up,
            "--threshold-down",
            threshold_down,
            "--position-size",
            position_size,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            str(runs_dir),
            "--poll-interval-sec",
            poll_interval_sec,
        )
        for s in symbols:
            argv += ["--symbols", str(s)]

        title = f"strategy {prof} {model_name} h={horizon}"
        jm2: JobManager = request.app.state.job_manager
        rec = jm2.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return {"ok": True, "account_profile": prof, "job_id": rec.id}

    @app.post("/api/strategy/stop", response_class=JSONResponse)
    async def api_strategy_stop(request: Request) -> dict[str, Any]:
        """
        Stop the running strategy job for the selected profile.

        Payload: {account_profile}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        store2: JobStore = request.app.state.job_store
        jm2: JobManager = request.app.state.job_manager
        cancelled = False
        job_id = None

        for j in store2.list_active_jobs():
            if _is_strategy_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    job_id = j.id
                    cancelled = bool(jm2.cancel_job(j.id))
                    break

        # Also set desired mode to idle so supervisor doesn't restart
        runs_dir = get_runs_dir()
        try:
            from ghtrader.trading.strategy_control import read_strategy_desired, write_strategy_desired

            existing = read_strategy_desired(runs_dir=runs_dir, profile=prof)
            if existing and existing.mode != "idle":
                existing.mode = "idle"  # type: ignore[assignment]
                write_strategy_desired(runs_dir=runs_dir, profile=prof, desired=existing)
        except Exception:
            pass

        if job_id:
            return {"ok": True, "account_profile": prof, "job_id": job_id, "cancelled": cancelled}
        return {"ok": False, "error": "not_running", "message": f"No running strategy job for profile '{prof}'."}

    @app.get("/api/strategy/runs", response_class=JSONResponse)
    def api_strategy_runs(request: Request, account_profile: str = "", limit: int = 50) -> dict[str, Any]:
        """
        List recent strategy runs under runs/strategy/<run_id>/ (excludes account=<profile> stable dirs).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        root = runs_dir / "strategy"
        lim = max(1, min(int(limit or 50), 200))

        profile_filter = ""
        if str(account_profile or "").strip():
            try:
                from ghtrader.tq.runtime import canonical_account_profile

                profile_filter = canonical_account_profile(str(account_profile))
            except Exception:
                profile_filter = str(account_profile).strip().lower()

        if not root.exists():
            return {"ok": True, "runs": []}

        out: list[dict[str, Any]] = []
        try:
            dirs = [p for p in root.iterdir() if p.is_dir() and not p.name.startswith("account=")]
            # Newest first (run_id is timestamp-prefixed in current implementation).
            dirs = sorted(dirs, key=lambda x: x.name, reverse=True)[:lim]
            for d in dirs:
                cfg = _read_json_file(d / "run_config.json") or {}
                ap = str(cfg.get("account_profile") or "").strip()
                if ap:
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap = canonical_account_profile(ap)
                    except Exception:
                        ap = ap.strip().lower()
                if profile_filter and ap != profile_filter:
                    continue
                last_ev = _read_last_jsonl_obj(d / "events.jsonl") if (d / "events.jsonl").exists() else None
                out.append(
                    {
                        "run_id": d.name,
                        "account_profile": ap or None,
                        "symbols": cfg.get("symbols"),
                        "model_name": cfg.get("model_name"),
                        "horizon": cfg.get("horizon"),
                        "position_size": cfg.get("position_size"),
                        "threshold_up": cfg.get("threshold_up"),
                        "threshold_down": cfg.get("threshold_down"),
                        "created_at": cfg.get("created_at"),
                        "last_event_ts": (last_ev or {}).get("ts") if isinstance(last_ev, dict) else None,
                    }
                )
        except Exception as e:
            return {"ok": False, "error": str(e), "runs": []}

        return {"ok": True, "runs": out}

    @app.get("/api/trading/console/status", response_class=JSONResponse)
    def api_trading_console_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
        """
        Unified Trading Console status (Gateway-first):
        - gateway: runs/gateway/account=<profile>/...
        - strategy: runs/strategy/account=<profile>/...
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(str(account_profile or "default"))
        except Exception:
            prof = str(account_profile or "default").strip() or "default"

        live_enabled = False
        try:
            from ghtrader.config import is_live_enabled

            live_enabled = bool(is_live_enabled())
        except Exception:
            live_enabled = False

        # Compose from existing endpoints (keep behavior consistent and minimize duplication).
        gw = api_gateway_status(request, account_profile=prof)
        st = api_strategy_status(request, account_profile=prof)

        # Add derived component statuses for clarity.
        try:
            gw_root = Path(str(gw.get("root") or ""))
            gw_state_path = gw_root / "state.json" if str(gw.get("root") or "") else None
            gw_age = _artifact_age_sec(gw_state_path) if (gw_state_path and gw_state_path.exists()) else None
            gw_desired = gw.get("desired") if isinstance(gw.get("desired"), dict) else {}
            gw_cfg = gw_desired.get("desired") if isinstance(gw_desired.get("desired"), dict) else gw_desired
            gw_mode = str((gw_cfg or {}).get("mode") or "idle")
            gw_status = _status_from_desired_and_state(root_exists=bool(gw.get("exists")), desired_mode=gw_mode, state_age=gw_age)
        except Exception:
            gw_status = "unknown"
            gw_age = None

        # Find active gateway/strategy jobs for this profile
        gateway_job: dict[str, Any] | None = None
        strategy_job: dict[str, Any] | None = None
        try:
            store2: JobStore = request.app.state.job_store
            for j in store2.list_active_jobs():
                if _is_gateway_job(j.command):
                    ap2 = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap2 = canonical_account_profile(ap2)
                    except Exception:
                        ap2 = str(ap2).strip().lower() or "default"
                    if ap2 == prof and gateway_job is None:
                        gateway_job = {"job_id": j.id, "status": j.status, "started_at": j.started_at}
                elif _is_strategy_job(j.command):
                    ap2 = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap2 = canonical_account_profile(ap2)
                    except Exception:
                        ap2 = str(ap2).strip().lower() or "default"
                    if ap2 == prof and strategy_job is None:
                        strategy_job = {"job_id": j.id, "status": j.status, "started_at": j.started_at}
        except Exception:
            pass

        # Compute stale flags: state_age_sec > 30 and desired mode is not idle
        STALE_THRESHOLD_SEC = 30.0
        gw_stale = False
        st_stale = False
        try:
            if gw_age is not None and float(gw_age) > STALE_THRESHOLD_SEC and gw_mode not in {"", "idle"}:
                gw_stale = True
        except Exception:
            pass
        try:
            st_age = st.get("state_age_sec")
            st_desired = st.get("desired") if isinstance(st.get("desired"), dict) else {}
            st_cfg = st_desired.get("desired") if isinstance(st_desired.get("desired"), dict) else st_desired
            st_mode = str((st_cfg or {}).get("mode") or "idle")
            if st_age is not None and float(st_age) > STALE_THRESHOLD_SEC and st_mode not in {"", "idle"}:
                st_stale = True
        except Exception:
            pass

        return {
            "ok": True,
            "account_profile": prof,
            "live_enabled": bool(live_enabled),
            "gateway": {**gw, "component_status": gw_status, "state_age_sec": gw_age, "stale": gw_stale},
            "strategy": {**st, "stale": st_stale},
            "gateway_job": gateway_job,
            "strategy_job": strategy_job,
            "generated_at": _now_iso(),
        }

    return app


app = create_app()
