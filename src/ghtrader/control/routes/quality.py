from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.control.routes.query_budget import bounded_limit


def mount_quality_routes(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
    normalize_variety_for_api: Callable[[str | None], str],
    derived_symbol_for_variety: Callable[..., str],
    get_runs_dir: Callable[[], Path],
) -> None:
    @app.get("/api/data/quality/readiness", response_class=JSONResponse)
    def api_data_quality_readiness(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        symbol: str = "",
    ) -> dict[str, Any]:
        """Return layered readiness summary for one variety/symbol."""
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        derived_symbol = str(symbol or derived_symbol_for_variety(v, exchange=ex)).strip()
        report = None
        report_error = ""
        overview: dict[str, Any] = {}
        try:
            from ghtrader.data.main_l5_validation import read_latest_validation_report
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import fetch_main_l5_validate_overview

            report = read_latest_validation_report(
                runs_dir=get_runs_dir(),
                exchange=ex,
                variety=v,
                derived_symbol=derived_symbol,
            )
            schedule_hash_filter = str((report or {}).get("schedule_hash") or "").strip() or None
            cfg = make_questdb_query_config_from_env()
            overview = fetch_main_l5_validate_overview(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=schedule_hash_filter,
            )
        except Exception as e:
            report_error = str(e)

        rep = dict(report or {})
        engineering_state = str(rep.get("engineering_state") or "warn")
        source_state = str(rep.get("source_state") or "warn")
        policy_state = str(rep.get("policy_state") or "warn")
        overall_state = str(rep.get("overall_state") or rep.get("state") or "warn")
        checked_days = int(rep.get("checked_days") or 0)
        source_missing_days_count = int(rep.get("source_missing_days_count") or rep.get("missing_days") or 0)
        provider_missing_day_rate = (
            float(source_missing_days_count) / float(checked_days) if checked_days > 0 else 0.0
        )

        # SLO-style metrics for operations dashboarding.
        store = request.app.state.job_store
        main_l5_jobs = [j for j in store.list_jobs(limit=400) if str((j.metadata or {}).get("kind") or "") == "main_l5"]
        succ = len([j for j in main_l5_jobs if str(j.status or "") == "succeeded"])
        total = len(main_l5_jobs)
        main_l5_build_success_rate = (float(succ) / float(total) if total > 0 else None)

        return {
            "ok": bool(not report_error),
            "exchange": ex,
            "var": v,
            "symbol": derived_symbol,
            "overall_state": overall_state,
            "engineering_state": engineering_state,
            "source_state": source_state,
            "policy_state": policy_state,
            "reason_code": str(rep.get("reason_code") or ""),
            "action_hint": str(rep.get("action_hint") or ""),
            "updated_at": str(rep.get("created_at") or ""),
            "stale": False,
            "slo_metrics": {
                "provider_missing_day_rate": provider_missing_day_rate,
                "outside_session_seconds": int(rep.get("ticks_outside_sessions_seconds_total") or 0),
                "gap_count_gt_30": int(rep.get("gap_count_gt_30s") or 0),
                "validation_runtime_p95": None,
                "main_l5_build_success_rate": main_l5_build_success_rate,
            },
            "overview": overview,
            "report_error": report_error or None,
        }

    @app.get("/api/data/quality/anomalies", response_class=JSONResponse)
    def api_data_quality_anomalies(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        symbol: str = "",
        limit: int = 50,
    ) -> dict[str, Any]:
        """Return compact anomaly list from latest validation report."""
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        derived_symbol = str(symbol or derived_symbol_for_variety(v, exchange=ex)).strip()
        lim = bounded_limit(limit, default=50, max_limit=200)
        try:
            from ghtrader.data.main_l5_validation import read_latest_validation_report

            rep = read_latest_validation_report(
                runs_dir=get_runs_dir(),
                exchange=ex,
                variety=v,
                derived_symbol=derived_symbol,
            ) or {}
            issues = list(rep.get("issues") or [])[:lim]
            rows: list[dict[str, Any]] = []
            for it in issues:
                if not isinstance(it, dict):
                    continue
                rows.append(
                    {
                        "trading_day": it.get("trading_day"),
                        "missing_segments_total": int(it.get("missing_segments_total") or 0),
                        "missing_half_seconds": int(it.get("missing_half_seconds") or 0),
                        "ticks_outside_sessions_seconds": int(it.get("ticks_outside_sessions_seconds") or 0),
                        "max_gap_s": int(it.get("max_gap_s") or 0),
                    }
                )
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "count": int(len(rows)),
                "rows": rows,
                "missing_days_sample": list(rep.get("missing_days_sample") or [])[:20],
                "source_missing_days_count": int(rep.get("source_missing_days_count") or rep.get("missing_days") or 0),
            }
        except Exception as e:
            return {"ok": False, "error": str(e), "exchange": ex, "var": v, "symbol": derived_symbol, "rows": []}

    @app.get("/api/data/quality/profiles", response_class=JSONResponse)
    def api_data_quality_profiles(request: Request, var: str = "") -> dict[str, Any]:
        """Return effective validation profile values and source mapping."""
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        v = normalize_variety_for_api(var)
        try:
            from ghtrader.data.main_l5_validation import resolve_validation_policy_preview

            profile = resolve_validation_policy_preview(variety=v)
            return {"ok": True, "var": v, "profile": profile}
        except Exception as e:
            return {"ok": False, "var": v, "error": str(e), "profile": {}}

