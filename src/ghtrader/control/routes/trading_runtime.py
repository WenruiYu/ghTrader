from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.control.db import JobStore
from ghtrader.control.jobs import JobManager, JobSpec, python_module_argv
from ghtrader.control.routes.query_budget import bounded_limit


def mount_trading_runtime_routes(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
    get_runs_dir: Callable[[], Path],
    now_iso: Callable[[], str],
    read_state_with_revision: Callable[..., tuple[Any, Any]],
    read_redis_json: Callable[[str], Any],
    read_json_file: Callable[[Path], Any],
    read_last_jsonl_obj: Callable[[Path], Any],
    artifact_age_sec: Callable[[Path], float | None],
    health_error: Callable[[Any], Any],
    status_from_desired_and_state: Callable[..., str],
    is_gateway_job: Callable[[list[str]], bool],
    is_strategy_job: Callable[[list[str]], bool],
    argv_opt: Callable[[list[str], str], str | None],
    normalize_variety_for_api: Callable[..., str],
    symbol_matches_variety: Callable[[str | None, str | None], bool],
    gateway_status_payload: Callable[..., dict[str, Any]],
) -> None:
    @app.get("/api/strategy/status", response_class=JSONResponse)
    def api_strategy_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
        """
        Read StrategyRunner stable state for a profile (Redis hot state first, file mirror fallback).
        """
        if not is_authorized(request):
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
        state, state_source = read_state_with_revision(
            redis_key=f"ghtrader:strategy:state:{prof}",
            file_path=st_path,
        )
        desired = read_redis_json(f"ghtrader:strategy:desired:{prof}") or (read_json_file(desired_path) if desired_path.exists() else None)
        targets = read_json_file((runs_dir / "gateway" / f"account={prof}" / "targets.json"))  # strategy output surface

        age = artifact_age_sec(st_path) if st_path.exists() else None
        desired_cfg = desired.get("desired") if isinstance(desired, dict) and isinstance(desired.get("desired"), dict) else (desired or {})
        desired_mode = str((desired_cfg or {}).get("mode") or "idle")
        status = status_from_desired_and_state(root_exists=root.exists(), desired_mode=desired_mode, state_age=age)

        active_job_id = None
        try:
            store2: JobStore = request.app.state.job_store
            for j in store2.list_active_jobs():
                if is_strategy_job(j.command):
                    ap = argv_opt(j.command, "--account") or ""
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
            "error": health_error(state),
            "active_job_id": active_job_id,
            "desired": desired,
            "state": state,
            "state_source": state_source,
            "targets": targets,
            "generated_at": now_iso(),
        }

    @app.post("/api/strategy/desired", response_class=JSONResponse)
    async def api_strategy_desired(request: Request) -> dict[str, Any]:
        """
        Upsert StrategyRunner desired.json for a profile.

        Payload:
          {account_profile, desired: {mode, symbols, model_name, horizon, threshold_up, threshold_down, position_size, artifacts_dir, poll_interval_sec}}
        """
        if not is_authorized(request):
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
        if not is_authorized(request):
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
            if is_gateway_job(j.command):
                ap2 = argv_opt(j.command, "--account") or ""
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
        gw_symbols = []
        if desired_in and isinstance(desired_in.get("symbols"), list):
            gw_symbols = [str(s).strip() for s in list(desired_in.get("symbols") or []) if str(s).strip()]
        rec = jm2.start_job(
            JobSpec(
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata={
                    "kind": "gateway",
                    "account_profile": prof,
                    "symbols": gw_symbols,
                },
            )
        )
        return {"ok": True, "account_profile": prof, "job_id": rec.id}

    @app.post("/api/gateway/stop", response_class=JSONResponse)
    async def api_gateway_stop(request: Request) -> dict[str, Any]:
        """
        Stop the running gateway job for the selected profile.

        Payload: {account_profile}
        """
        if not is_authorized(request):
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
            if is_gateway_job(j.command):
                ap2 = argv_opt(j.command, "--account") or ""
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
            from dataclasses import replace
            from ghtrader.tq.gateway import read_gateway_desired, write_gateway_desired

            existing = read_gateway_desired(runs_dir=runs_dir, profile=prof)
            if existing and existing.mode != "idle":
                write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=replace(existing, mode="idle"))
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
        if not is_authorized(request):
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
            if is_strategy_job(j.command):
                ap2 = argv_opt(j.command, "--account") or ""
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
        rec = jm2.start_job(
            JobSpec(
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata={
                    "kind": "strategy",
                    "account_profile": prof,
                    "model": model_name,
                    "horizon": str(cfg.horizon or 50),
                    "symbols": [str(s).strip() for s in symbols if str(s).strip()],
                },
            )
        )
        return {"ok": True, "account_profile": prof, "job_id": rec.id}

    @app.post("/api/strategy/stop", response_class=JSONResponse)
    async def api_strategy_stop(request: Request) -> dict[str, Any]:
        """
        Stop the running strategy job for the selected profile.

        Payload: {account_profile}
        """
        if not is_authorized(request):
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
            if is_strategy_job(j.command):
                ap2 = argv_opt(j.command, "--account") or ""
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
            from dataclasses import replace
            from ghtrader.trading.strategy_control import read_strategy_desired, write_strategy_desired

            existing = read_strategy_desired(runs_dir=runs_dir, profile=prof)
            if existing and existing.mode != "idle":
                write_strategy_desired(runs_dir=runs_dir, profile=prof, desired=replace(existing, mode="idle"))
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
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        root = runs_dir / "strategy"
        lim = bounded_limit(limit, default=50, max_limit=200)

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
                cfg = read_json_file(d / "run_config.json") or {}
                ap = str(cfg.get("account_profile") or "").strip()
                if ap:
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap = canonical_account_profile(ap)
                    except Exception:
                        ap = ap.strip().lower()
                if profile_filter and ap != profile_filter:
                    continue
                last_ev = read_last_jsonl_obj(d / "events.jsonl") if (d / "events.jsonl").exists() else None
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
    def api_trading_console_status(request: Request, account_profile: str = "default", var: str = "") -> dict[str, Any]:
        """
        Unified Trading Console status (Gateway-first):
        - gateway: runs/gateway/account=<profile>/...
        - strategy: runs/strategy/account=<profile>/...
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        var_filter = normalize_variety_for_api(var) if str(var or "").strip() else ""
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
        gw = gateway_status_payload(account_profile=prof)
        st = api_strategy_status(request, account_profile=prof)

        # Add derived component statuses for clarity.
        try:
            gw_root = Path(str(gw.get("root") or ""))
            gw_state_path = gw_root / "state.json" if str(gw.get("root") or "") else None
            gw_age = artifact_age_sec(gw_state_path) if (gw_state_path and gw_state_path.exists()) else None
            gw_desired = gw.get("desired") if isinstance(gw.get("desired"), dict) else {}
            gw_cfg = gw_desired.get("desired") if isinstance(gw_desired.get("desired"), dict) else gw_desired
            gw_mode = str((gw_cfg or {}).get("mode") or "idle")
            gw_status = status_from_desired_and_state(root_exists=bool(gw.get("exists")), desired_mode=gw_mode, state_age=gw_age)
        except Exception:
            gw_status = "unknown"
            gw_age = None

        # Find active gateway/strategy jobs for this profile
        gateway_job: dict[str, Any] | None = None
        strategy_job: dict[str, Any] | None = None
        try:
            store2: JobStore = request.app.state.job_store
            for j in store2.list_active_jobs():
                if is_gateway_job(j.command):
                    ap2 = argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap2 = canonical_account_profile(ap2)
                    except Exception:
                        ap2 = str(ap2).strip().lower() or "default"
                    if ap2 == prof and gateway_job is None:
                        gateway_job = {"job_id": j.id, "status": j.status, "started_at": j.started_at}
                elif is_strategy_job(j.command):
                    ap2 = argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap2 = canonical_account_profile(ap2)
                    except Exception:
                        ap2 = str(ap2).strip().lower() or "default"
                    if ap2 == prof and strategy_job is None:
                        strategy_job = {"job_id": j.id, "status": j.status, "started_at": j.started_at}
        except Exception:
            pass

        supervisor_telemetry: dict[str, Any] = {}
        supervisor_tick_failures_total = 0
        try:
            telemetry_getter = getattr(request.app.state, "supervisor_telemetry_getter", None)
            if callable(telemetry_getter):
                raw = telemetry_getter()
                if isinstance(raw, dict):
                    supervisor_telemetry = dict(raw)
                    sup_root = raw.get("supervisors") if isinstance(raw.get("supervisors"), dict) else {}
                    for name in ("strategy", "gateway", "scheduler"):
                        bucket = sup_root.get(name) if isinstance(sup_root.get(name), dict) else {}
                        supervisor_tick_failures_total += int(bucket.get("tick_failures_total") or 0)
        except Exception:
            supervisor_telemetry = {}
            supervisor_tick_failures_total = 0

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

        warm_path_reasons: list[str] = []
        risk_kill_active = False
        risk_kill_reason = ""
        try:
            gw_state = gw.get("state") if isinstance(gw.get("state"), dict) else {}
            gw_eff = gw_state.get("effective") if isinstance(gw_state.get("effective"), dict) else {}
            if bool(gw_eff.get("warm_path_degraded")):
                gw_r = str(gw_eff.get("warm_path_reason") or "").strip()
                warm_path_reasons.append(f"gateway:{gw_r}" if gw_r else "gateway")
            risk_kill_active = bool(gw_eff.get("risk_kill_active"))
            risk_kill_reason = str(gw_eff.get("risk_kill_reason") or "")
        except Exception:
            pass
        try:
            st_state = st.get("state") if isinstance(st.get("state"), dict) else {}
            st_eff = st_state.get("effective") if isinstance(st_state.get("effective"), dict) else {}
            if bool(st_eff.get("warm_path_degraded")):
                st_r = str(st_eff.get("warm_path_reason") or "").strip()
                warm_path_reasons.append(f"strategy:{st_r}" if st_r else "strategy")
        except Exception:
            pass
        warm_path_degraded = bool(warm_path_reasons)
        state_notes: list[str] = []
        if gw_stale or st_stale:
            state_notes.append("state stale")
        if warm_path_degraded:
            state_notes.append("warm path degraded")
        if risk_kill_active:
            state_notes.append("risk kill active")
        if int(supervisor_tick_failures_total) > 0:
            state_notes.append("supervisor tick failures")

        if var_filter:
            # Gateway desired/effective symbols and snapshots filtered for current variety view.
            gw_desired = gw.get("desired") if isinstance(gw.get("desired"), dict) else None
            if isinstance(gw_desired, dict):
                gw_desired2 = dict(gw_desired)
                inner = gw_desired2.get("desired") if isinstance(gw_desired2.get("desired"), dict) else None
                if isinstance(inner, dict):
                    inner2 = dict(inner)
                    if isinstance(inner2.get("symbols"), list):
                        inner2["symbols"] = [s for s in inner2.get("symbols") or [] if symbol_matches_variety(str(s), var_filter)]
                    gw_desired2["desired"] = inner2
                elif isinstance(gw_desired2.get("symbols"), list):
                    gw_desired2["symbols"] = [s for s in gw_desired2.get("symbols") or [] if symbol_matches_variety(str(s), var_filter)]
                gw["desired"] = gw_desired2

            gw_state = gw.get("state") if isinstance(gw.get("state"), dict) else None
            if isinstance(gw_state, dict):
                gw_state2 = dict(gw_state)
                eff = gw_state2.get("effective") if isinstance(gw_state2.get("effective"), dict) else None
                if isinstance(eff, dict):
                    eff2 = dict(eff)
                    if isinstance(eff2.get("symbols"), list):
                        eff2["symbols"] = [s for s in eff2.get("symbols") or [] if symbol_matches_variety(str(s), var_filter)]
                    gw_state2["effective"] = eff2
                snap = gw_state2.get("last_snapshot") if isinstance(gw_state2.get("last_snapshot"), dict) else None
                if isinstance(snap, dict):
                    snap2 = dict(snap)
                    pos = snap2.get("positions")
                    if isinstance(pos, dict):
                        snap2["positions"] = {k: v for k, v in pos.items() if symbol_matches_variety(str(k), var_filter)}
                    orders_alive = snap2.get("orders_alive")
                    if isinstance(orders_alive, list):
                        snap2["orders_alive"] = [
                            o
                            for o in orders_alive
                            if isinstance(o, dict) and symbol_matches_variety(str(o.get("symbol") or ""), var_filter)
                        ]
                    gw_state2["last_snapshot"] = snap2
                gw["state"] = gw_state2

            st_desired = st.get("desired") if isinstance(st.get("desired"), dict) else None
            if isinstance(st_desired, dict):
                st_desired2 = dict(st_desired)
                inner = st_desired2.get("desired") if isinstance(st_desired2.get("desired"), dict) else None
                if isinstance(inner, dict):
                    inner2 = dict(inner)
                    if isinstance(inner2.get("symbols"), list):
                        inner2["symbols"] = [s for s in inner2.get("symbols") or [] if symbol_matches_variety(str(s), var_filter)]
                    st_desired2["desired"] = inner2
                elif isinstance(st_desired2.get("symbols"), list):
                    st_desired2["symbols"] = [s for s in st_desired2.get("symbols") or [] if symbol_matches_variety(str(s), var_filter)]
                st["desired"] = st_desired2

            st_state = st.get("state") if isinstance(st.get("state"), dict) else None
            if isinstance(st_state, dict):
                st_state2 = dict(st_state)
                last_targets = st_state2.get("last_targets")
                if isinstance(last_targets, dict):
                    st_state2["last_targets"] = {
                        k: v for k, v in last_targets.items() if symbol_matches_variety(str(k), var_filter)
                    }
                events = st_state2.get("recent_events")
                if isinstance(events, list):
                    filtered_events: list[Any] = []
                    for e in events:
                        if not isinstance(e, dict):
                            continue
                        e2 = dict(e)
                        targets = e2.get("targets")
                        if isinstance(targets, dict):
                            targets2 = {k: v for k, v in targets.items() if symbol_matches_variety(str(k), var_filter)}
                            if targets2:
                                e2["targets"] = targets2
                                filtered_events.append(e2)
                                continue
                        sym = str(e2.get("symbol") or "")
                        if sym and not symbol_matches_variety(sym, var_filter):
                            continue
                        filtered_events.append(e2)
                    st_state2["recent_events"] = filtered_events
                st["state"] = st_state2

        payload = {
            "ok": True,
            "account_profile": prof,
            "live_enabled": bool(live_enabled),
            "state": ("warn" if state_notes else "ok"),
            "text": ("; ".join(state_notes) if state_notes else "healthy"),
            "error": "",
            "stale": bool(gw_stale or st_stale),
            "warm_path_degraded": bool(warm_path_degraded),
            "warm_path_reasons": list(warm_path_reasons),
            "risk_kill_active": bool(risk_kill_active),
            "risk_kill_reason": str(risk_kill_reason),
            "updated_at": now_iso(),
            "gateway": {**gw, "component_status": gw_status, "state_age_sec": gw_age, "stale": gw_stale},
            "strategy": {**st, "stale": st_stale},
            "gateway_job": gateway_job,
            "strategy_job": strategy_job,
            "supervisor_telemetry": supervisor_telemetry,
            "generated_at": now_iso(),
        }
        if var_filter:
            payload["var"] = var_filter
        return payload
