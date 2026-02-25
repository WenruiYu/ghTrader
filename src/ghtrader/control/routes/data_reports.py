from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.control.routes.query_budget import bounded_limit


def mount_data_report_routes(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
    normalize_variety_for_api: Callable[[str | None], str],
    get_runs_dir: Callable[[], Path],
    read_json: Callable[[Path], dict[str, Any] | None],
) -> None:
    @app.get("/api/data/reports", response_class=JSONResponse)
    def api_data_reports(
        request: Request,
        kind: str = "diagnose",
        exchange: str = "SHFE",
        var: str = "",
        limit: int = 5,
    ) -> dict[str, Any]:
        """
        Return recent data reports written under runs/control/reports/.

        kind:
        - diagnose: runs/control/reports/data_diagnose/diagnose_exchange=..._var=..._*.json
        - repair: runs/control/reports/data_repair/repair_*.plan.json (+ optional .result.json)
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        k = str(kind or "").strip().lower()
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        lim = bounded_limit(limit, default=5, max_limit=50)

        rd = get_runs_dir()

        if k == "diagnose":
            rep_dir = rd / "control" / "reports" / "data_diagnose"
            if not rep_dir.exists():
                return {"ok": False, "error": "no_reports", "kind": k, "exchange": ex, "var": v, "reports": []}
            pat = f"diagnose_exchange={ex}_var={v}_*.json"
            paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
            reports: list[dict[str, Any]] = []
            for p in paths:
                obj = read_json(p)
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
                plan = read_json(p)
                if not isinstance(plan, dict):
                    continue
                if str(plan.get("exchange") or "").upper().strip() != ex:
                    continue
                if str(plan.get("var") or "").lower().strip() != v:
                    continue
                run_id = str(plan.get("run_id") or "").strip()
                res_path = rep_dir / f"repair_{run_id}.result.json" if run_id else None
                res_obj = read_json(res_path) if (res_path and res_path.exists()) else None
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
    def api_data_l5_start(request: Request, exchange: str = "SHFE", var: str = "", limit: int = 1) -> dict[str, Any]:
        """
        Return the most recent L5-start report(s) written under runs/control/reports/l5_start/.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        lim = bounded_limit(limit, default=1, max_limit=20)

        rd = get_runs_dir()
        rep_dir = rd / "control" / "reports" / "l5_start"
        if not rep_dir.exists():
            return {"ok": False, "error": "no_reports", "exchange": ex, "var": v, "reports": []}

        pat = f"l5_start_exchange={ex}_var={v}_*.json"
        paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
        reports: list[dict[str, Any]] = []
        for p in paths:
            obj = read_json(p)
            if obj:
                obj = dict(obj)
                obj["_path"] = str(p)
                reports.append(obj)
        return {"ok": True, "exchange": ex, "var": v, "count": int(len(reports)), "reports": reports}
