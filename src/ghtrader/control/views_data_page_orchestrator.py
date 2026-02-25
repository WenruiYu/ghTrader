from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

import structlog

from ghtrader.control.views_data_page_loaders import (
    load_coverage_snapshot as _load_coverage_snapshot,
    load_questdb_status_snapshot as _load_questdb_status_snapshot,
    load_reports_snapshot as _load_reports_snapshot,
    load_validation_snapshot as _load_validation_snapshot,
)

log = structlog.get_logger()

CacheGet = Callable[[str], Any | None]
CacheSet = Callable[[str, Any], None]


def _payload_error(payload: dict[str, Any], *, preferred_key: str) -> str:
    preferred = payload.get(preferred_key)
    if preferred:
        return str(preferred)
    generic = payload.get("error")
    return str(generic) if generic else ""


def _normalize_questdb_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"ok": False, "error": "questdb unavailable"}
    out = dict(payload)
    out["ok"] = bool(out.get("ok"))
    if not out["ok"] and not str(out.get("error") or "").strip():
        out["error"] = "questdb unavailable"
    return out


def _normalize_coverage_payload(payload: Any) -> dict[str, Any]:
    out = dict(payload) if isinstance(payload, dict) else {}
    out["main_schedule_coverage"] = dict(out.get("main_schedule_coverage") or {})
    out["main_l5_coverage"] = dict(out.get("main_l5_coverage") or {})
    out["coverage_error"] = _payload_error(out, preferred_key="coverage_error")
    return out


def _normalize_validation_payload(payload: Any) -> dict[str, Any]:
    out = dict(payload) if isinstance(payload, dict) else {}
    out["main_l5_validation"] = dict(out.get("main_l5_validation") or {})
    out["validation_error"] = _payload_error(out, preferred_key="validation_error")
    return out


def _make_qdb_cfg() -> Any | None:
    try:
        from ghtrader.questdb.client import make_questdb_query_config_from_env

        return make_questdb_query_config_from_env()
    except Exception:
        return None


def resolve_data_page_snapshot(
    *,
    runs_dir: Path,
    coverage_var: str,
    cache_get: CacheGet,
    cache_set: CacheSet,
) -> dict[str, Any]:
    # Audit reports / QuestDB / coverage / validation can be cached + parallelized.
    t0 = time.time()
    reports = cache_get("data_page:audit_reports")
    questdb = cache_get("data_page:questdb_status")

    # Coverage watermarks (best-effort, variety-scoped)
    main_l5_coverage: dict[str, Any] = {}
    main_schedule_coverage: dict[str, Any] = {}
    coverage_error = ""
    main_l5_validation: dict[str, Any] = {}
    validation_error = ""
    coverage_symbol = f"KQ.m@SHFE.{coverage_var}"

    coverage_payload = cache_get(f"data_page:coverage:{coverage_symbol}")
    validation_payload = cache_get(f"data_page:validation:{coverage_symbol}")
    cache_hits = int(
        sum(
            1
            for hit in (
                reports is not None,
                questdb is not None,
                coverage_payload is not None,
                validation_payload is not None,
            )
            if hit
        )
    )

    tasks: dict[str, Any] = {}
    cfg = None
    if coverage_payload is None or validation_payload is None:
        cfg = _make_qdb_cfg()

    if reports is None:
        tasks["reports"] = lambda: _load_reports_snapshot(runs_dir=runs_dir)
    if questdb is None:
        tasks["questdb"] = _load_questdb_status_snapshot
    if coverage_payload is None:
        tasks["coverage"] = lambda: _load_coverage_snapshot(
            cfg=cfg,
            coverage_var=coverage_var,
            coverage_symbol=coverage_symbol,
        )
    if validation_payload is None:
        tasks["validation"] = lambda: _load_validation_snapshot(
            cfg=cfg,
            runs_dir=runs_dir,
            coverage_var=coverage_var,
            coverage_symbol=coverage_symbol,
        )

    if tasks:
        with ThreadPoolExecutor(max_workers=min(4, len(tasks))) as executor:
            future_map = {executor.submit(fn): name for name, fn in tasks.items()}
            for fut in as_completed(future_map):
                name = future_map[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    result = {"error": str(e)}
                if name == "reports":
                    reports = result if isinstance(result, list) else []
                    cache_set("data_page:audit_reports", reports)
                elif name == "questdb":
                    questdb = _normalize_questdb_payload(result)
                    cache_set("data_page:questdb_status", questdb)
                elif name == "coverage":
                    coverage_payload = _normalize_coverage_payload(result)
                    cache_set(f"data_page:coverage:{coverage_symbol}", coverage_payload)
                elif name == "validation":
                    validation_payload = _normalize_validation_payload(result)
                    cache_set(f"data_page:validation:{coverage_symbol}", validation_payload)

    if reports is None:
        reports = []
    questdb = _normalize_questdb_payload(questdb)
    if coverage_payload is not None:
        coverage_payload = _normalize_coverage_payload(coverage_payload)
        main_schedule_coverage = dict((coverage_payload or {}).get("main_schedule_coverage") or {})
        main_l5_coverage = dict((coverage_payload or {}).get("main_l5_coverage") or {})
        coverage_error = str((coverage_payload or {}).get("coverage_error") or "")
    if validation_payload is not None:
        validation_payload = _normalize_validation_payload(validation_payload)
        main_l5_validation = dict((validation_payload or {}).get("main_l5_validation") or {})
        validation_error = str((validation_payload or {}).get("validation_error") or "")

    try:
        log.debug(
            "data_page.timing",
            coverage_symbol=coverage_symbol,
            cache_hits=cache_hits,
            ms=int((time.time() - t0) * 1000),
        )
    except Exception:
        pass

    return {
        "reports": reports,
        "questdb": questdb,
        "coverage_var": coverage_var,
        "coverage_symbol": coverage_symbol,
        "main_schedule_coverage": main_schedule_coverage,
        "main_l5_coverage": main_l5_coverage,
        "coverage_error": coverage_error,
        "main_l5_validation": main_l5_validation,
        "validation_error": validation_error,
    }
