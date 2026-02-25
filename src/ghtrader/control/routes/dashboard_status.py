from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Protocol

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse


class _CacheSlot(Protocol):
    def get(self, *, ttl_s: float, key: str = "", now: float | None = None) -> Any: ...
    def set(self, val: dict[str, Any], *, key: str = "", now: float | None = None) -> None: ...


def mount_dashboard_status_routes(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
    normalize_variety_for_api: Callable[..., str],
    get_data_dir: Callable[[], Path],
    get_runs_dir: Callable[[], Path],
    get_artifacts_dir: Callable[[], Path],
    now_iso: Callable[[], str],
    job_matches_variety: Callable[[Any, str], bool],
    symbol_matches_variety: Callable[[str | None, str | None], bool],
    scan_model_files: Callable[[Path], list[dict[str, Any]]],
    ui_state_payload: Callable[..., dict[str, Any]],
    dashboard_guardrails_context: Callable[[], dict[str, Any]],
    derived_symbol_for_variety: Callable[..., str],
    dash_summary_cache: _CacheSlot,
    dash_summary_ttl_s: float,
    ui_status_cache: _CacheSlot,
    ui_status_ttl_s: float,
) -> None:
    @app.get("/api/dashboard/summary", response_class=JSONResponse)
    def api_dashboard_summary(request: Request, var: str = "") -> dict[str, Any]:
        """
        Aggregated KPIs for the dashboard home page.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        v = normalize_variety_for_api(var, allow_legacy_default=True)
        now = time.time()
        cached = dash_summary_cache.get(ttl_s=dash_summary_ttl_s, key=v, now=now)
        if isinstance(cached, dict):
            return dict(cached)

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()
        _ = data_dir

        # Job counts
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=200)
        jobs_filtered = [j for j in jobs if job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if j.status == "running"])
        queued_count = len([j for j in jobs_filtered if j.status == "queued"])

        # QuestDB reachability
        questdb_ok = False
        try:
            from ghtrader.questdb.client import questdb_reachable_pg

            q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
            questdb_ok = bool(q.get("ok"))
        except Exception:
            questdb_ok = False

        # Data symbols / status derived from canonical main_l5 table.
        data_symbols = 0
        data_status = "offline" if not questdb_ok else "unknown"

        # Model count (count actual model files, not arbitrary artifacts/ subdirs)
        model_files: list[dict[str, Any]] = []
        model_status = ""
        try:
            model_files_all = scan_model_files(artifacts_dir)
            model_files = [m for m in model_files_all if symbol_matches_variety(str(m.get("symbol") or ""), v)]
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

        summary_updated_at = now_iso()
        validation_rows = 0

        # Pipeline status (simplified)
        pipeline = {
            "ingest": ui_state_payload(
                state=("ok" if data_symbols > 0 else "warn"),
                text=f"{data_symbols} symbols",
                updated_at=summary_updated_at,
            ),
            "sync": ui_state_payload(
                state=("ok" if questdb_ok else "error"),
                text=("connected" if questdb_ok else "offline"),
                updated_at=summary_updated_at,
            ),
            "schedule": ui_state_payload(state="unknown", text="--", updated_at=summary_updated_at),
            "main_l5": ui_state_payload(state="unknown", text="--", updated_at=summary_updated_at),
            "validation": ui_state_payload(state="warn", text="not run", updated_at=summary_updated_at),
            "build": ui_state_payload(state="unknown", text="--", updated_at=summary_updated_at),
            "train": ui_state_payload(
                state=("ok" if model_count > 0 else "warn"),
                text=f"{model_count} models",
                updated_at=summary_updated_at,
            ),
        }

        # Check schedule + main_l5 availability (QuestDB canonical tables).
        try:
            if questdb_ok:
                from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
                from ghtrader.questdb.main_l5_daily_agg import query_main_l5_symbol_count_for_variety
                from ghtrader.questdb.main_schedule import MAIN_SCHEDULE_TABLE_V2

                cfg = make_questdb_query_config_from_env()
                try:
                    data_symbols = int(
                        query_main_l5_symbol_count_for_variety(
                            cfg=cfg,
                            variety=v,
                            dataset_version="v2",
                            ticks_kind="main_l5",
                        )
                    )
                except Exception:
                    data_symbols = 0
                with connect_pg(cfg, connect_timeout_s=1) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT count() FROM {MAIN_SCHEDULE_TABLE_V2} WHERE exchange=%s AND variety=%s",
                            ["SHFE", v],
                        )
                        n = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            "SELECT count() FROM ghtrader_main_l5_validate_summary_v2 "
                            "WHERE lower(symbol) LIKE %s",
                            [f"%shfe.{v}%"],
                        )
                        validation_rows = int((cur.fetchone() or [0])[0] or 0)
                if n > 0:
                    pipeline["schedule"] = ui_state_payload(state="ok", text=f"{v} ready", updated_at=summary_updated_at)
                if data_symbols > 0:
                    pipeline["ingest"] = ui_state_payload(
                        state="ok",
                        text=f"{data_symbols} symbols",
                        updated_at=summary_updated_at,
                    )
                    pipeline["main_l5"] = ui_state_payload(
                        state="ok",
                        text=f"{data_symbols} symbols",
                        updated_at=summary_updated_at,
                    )
                    data_status = "ready"
                else:
                    pipeline["ingest"] = ui_state_payload(state="warn", text="0 symbols", updated_at=summary_updated_at)
                    pipeline["main_l5"] = ui_state_payload(state="warn", text="empty", updated_at=summary_updated_at)
                    data_status = "empty"
                validation_state = ("ok" if validation_rows > 0 else "warn")
                validation_text = (f"{validation_rows} rows" if validation_rows > 0 else "not run")
                try:
                    from ghtrader.data.main_l5_validation import read_latest_validation_report

                    rep = read_latest_validation_report(
                        runs_dir=runs_dir,
                        exchange="SHFE",
                        variety=v,
                        derived_symbol=derived_symbol_for_variety(v, exchange="SHFE"),
                    )
                    if isinstance(rep, dict):
                        rep_state = str(rep.get("state") or "").strip().lower()
                        if rep_state == "error":
                            validation_state = "error"
                            validation_text = "error"
                        elif rep_state == "warn":
                            validation_state = "warn"
                            validation_text = "warn"
                        elif rep_state == "noop":
                            validation_state = "ok"
                            validation_text = "up-to-date"
                        elif rep_state == "ok":
                            validation_state = "ok"
                            validation_text = "ok"
                except Exception:
                    pass
                pipeline["validation"] = ui_state_payload(
                    state=validation_state,
                    text=validation_text,
                    updated_at=summary_updated_at,
                )
        except Exception:
            if questdb_ok:
                pipeline["main_l5"] = ui_state_payload(
                    state="error",
                    text="query error",
                    error="main_l5 query failed",
                    updated_at=summary_updated_at,
                )
                pipeline["validation"] = ui_state_payload(
                    state="warn",
                    text="query unavailable",
                    error="validation summary unavailable",
                    updated_at=summary_updated_at,
                )
                data_status = "error"

        # Check features/labels builds exist (QuestDB build tables)
        try:
            if questdb_ok:
                from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
                from ghtrader.questdb.features_labels import FEATURE_BUILDS_TABLE_V2

                cfg = make_questdb_query_config_from_env()
                with connect_pg(cfg, connect_timeout_s=1) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT count(DISTINCT symbol) FROM {FEATURE_BUILDS_TABLE_V2} "
                            "WHERE dataset_version='v2' AND lower(symbol) LIKE %s",
                            [f"%shfe.{v}%"],
                        )
                        n = int((cur.fetchone() or [0])[0] or 0)
                if n > 0:
                    pipeline["build"] = ui_state_payload(state="ok", text=f"{n} symbols", updated_at=summary_updated_at)
        except Exception:
            pass

        guardrails = dashboard_guardrails_context()

        payload = {
            "ok": True,
            "var": v,
            "state": "ok",
            "text": "summary ready",
            "error": "",
            "stale": False,
            "updated_at": summary_updated_at,
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
            "guardrails": guardrails,
        }
        dash_summary_cache.set(dict(payload), key=v, now=time.time())
        return payload

    @app.get("/api/ui/status", response_class=JSONResponse)
    def api_ui_status(request: Request) -> dict[str, Any]:
        """
        Lightweight status endpoint for global UI chrome (topbar).

        Cached to avoid doing QuestDB connection checks too frequently.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        now = time.time()
        cached = ui_status_cache.get(ttl_s=ui_status_ttl_s, now=now)
        if isinstance(cached, dict):
            return dict(cached)

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()
        artifacts_dir = get_artifacts_dir()

        # Job counts
        store = request.app.state.job_store
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

        status_updated_at = now_iso()
        payload = {
            "ok": True,
            "state": ("ok" if questdb_ok else "warn"),
            "text": ("QuestDB connected" if questdb_ok else "QuestDB offline"),
            "error": ("" if questdb_ok else "questdb_unreachable"),
            "stale": False,
            "updated_at": status_updated_at,
            "questdb_ok": bool(questdb_ok),
            "gpu_status": gpu_status,
            "running_count": int(running_count),
            "queued_count": int(queued_count),
        }
        ui_status_cache.set(dict(payload), now=time.time())
        return payload
