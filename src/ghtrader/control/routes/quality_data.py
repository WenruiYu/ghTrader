from __future__ import annotations

import re
import time
from datetime import date
from pathlib import Path
from typing import Any, Callable, Protocol

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.control.routes.query_budget import bounded_limit


class _QualityCache(Protocol):
    def get(self, *, ttl_s: float, key: str, now: float | None = None) -> Any: ...
    def set(self, val: dict[str, Any], *, key: str, now: float | None = None) -> None: ...


def mount_quality_data_routes(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
    normalize_variety_for_api: Callable[[str | None], str],
    derived_symbol_for_variety: Callable[..., str],
    get_runs_dir: Callable[[], Path],
    read_json: Callable[[Path], dict[str, Any] | None],
    now_iso: Callable[[], str],
    data_quality_cache: _QualityCache,
    data_quality_ttl_s: float,
) -> None:
    @app.get("/api/data/quality-summary", response_class=JSONResponse)
    def api_data_quality_summary(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        limit: int = 300,
        search: str = "",
        issues_only: bool = False,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """
        Return per-symbol data quality summary (QuestDB-first, cached).
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        # Keep this as a best-effort read endpoint; unlike other deferred APIs it has
        # a complete implementation and does not enqueue removed CLI surfaces.

        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        lim = bounded_limit(limit, default=300, max_limit=500)
        q = str(search or "").strip().lower()
        only_issues = bool(issues_only)

        now = time.time()
        cache_key = f"{ex}|{v}|{lim}|{q}|{int(only_issues)}"
        cached = data_quality_cache.get(ttl_s=data_quality_ttl_s, key=cache_key, now=now)
        if isinstance(cached, dict) and not refresh:
            return dict(cached)

        runs_dir = get_runs_dir()
        snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / f"contracts_exchange={ex}_var={v}.json"
        snap = read_json(snap_path) if snap_path.exists() else None
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

            outliers = int(fq.get("price_jump_outliers") or 0) + int(fq.get("volume_spike_outliers") or 0) + int(
                fq.get("spread_anomaly_outliers") or 0
            )
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
                    "l1_null_rates": {
                        k: fq.get(k)
                        for k in [
                            "bid_price1_null_rate",
                            "ask_price1_null_rate",
                            "last_price_null_rate",
                            "volume_null_rate",
                            "open_interest_null_rate",
                        ]
                    },
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
            "generated_at": now_iso(),
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
        data_quality_cache.set(dict(payload), key=cache_key, now=time.time())
        return payload

    @app.get("/api/data/main-l5-validate", response_class=JSONResponse)
    def api_data_main_l5_validate(
        request: Request, exchange: str = "SHFE", var: str = "", symbol: str = "", limit: int = 1
    ) -> dict[str, Any]:
        """
        Return recent main_l5 validation report(s) written under runs/control/reports/main_l5_validate/.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        derived_symbol = str(symbol or derived_symbol_for_variety(v, exchange=ex)).strip()
        lim = bounded_limit(limit, default=1, max_limit=20)

        rd = get_runs_dir()
        rep_dir = rd / "control" / "reports" / "main_l5_validate"
        if not rep_dir.exists():
            return {"ok": False, "error": "no_reports", "exchange": ex, "var": v, "reports": []}

        sym = re.sub(r"[^A-Za-z0-9]+", "_", derived_symbol).strip("_").lower()
        pat = f"main_l5_validate_exchange={ex}_var={v}_symbol={sym}_*.json"
        paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
        reports: list[dict[str, Any]] = []
        for p in paths:
            obj = read_json(p)
            if obj:
                obj = dict(obj)
                obj["_path"] = str(p)
                reports.append(obj)
        return {"ok": True, "exchange": ex, "var": v, "symbol": derived_symbol, "count": int(len(reports)), "reports": reports}

    @app.get("/api/data/main-l5-validate-summary", response_class=JSONResponse)
    def api_data_main_l5_validate_summary(
        request: Request, exchange: str = "SHFE", var: str = "", symbol: str = "", schedule_hash: str = "", limit: int = 30
    ) -> dict[str, Any]:
        """
        Return QuestDB-backed validation summary rows + overview.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        derived_symbol = str(symbol or derived_symbol_for_variety(v, exchange=ex)).strip()
        sh = str(schedule_hash or "").strip()
        lim = bounded_limit(limit, default=30, max_limit=3650)
        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import (
                fetch_latest_main_l5_validate_summary,
                fetch_main_l5_validate_overview,
            )

            cfg = make_questdb_query_config_from_env()
            overview = fetch_main_l5_validate_overview(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=(sh or None),
            )
            rows = fetch_latest_main_l5_validate_summary(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=(sh or None),
                limit=lim,
            )
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "overview": overview,
                "count": int(len(rows)),
                "rows": rows,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "rows": [],
            }

    @app.get("/api/data/main-l5-validate-gaps", response_class=JSONResponse)
    def api_data_main_l5_validate_gaps(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        symbol: str = "",
        schedule_hash: str = "",
        trading_day: str = "",
        start: str = "",
        end: str = "",
        min_duration_s: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        """
        Return QuestDB-backed gap details for main_l5 validation.
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = normalize_variety_for_api(var)
        derived_symbol = str(symbol or derived_symbol_for_variety(v, exchange=ex)).strip()
        sh = str(schedule_hash or "").strip()
        lim = bounded_limit(limit, default=500, max_limit=10000)
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
                schedule_hash=(sh or None),
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
                "schedule_hash": (sh or None),
                "count": int(len(gaps)),
                "gaps": gaps,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "gaps": [],
            }
