from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.json_io import write_json_atomic as _write_json_atomic
from ghtrader.trading_calendar import get_trading_calendar

log = structlog.get_logger()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_date_prefix(v: Any) -> date | None:
    """
    Parse YYYY-MM-DD from the first 10 chars of a value (datetime or date string).
    """
    if v in (None, ""):
        return None
    try:
        s = str(v).strip()
    except Exception:
        return None
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _last_trading_day_leq(cal: list[date], today: date) -> date:
    if not cal:
        # fallback approximation: last weekday
        d = today
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d
    # calendar is sorted
    lo = 0
    hi = len(cal)
    while lo < hi:
        mid = (lo + hi) // 2
        if cal[mid] <= today:
            lo = mid + 1
        else:
            hi = mid
    idx = lo - 1
    return cal[idx] if idx >= 0 else today


def _recent_cutoff_trading_day(cal: list[date], today_trading: date, n_trading_days: int) -> date:
    n = max(1, int(n_trading_days))
    if not cal:
        return today_trading - timedelta(days=max(1, n * 2))
    # Find today_trading in calendar and step back (n-1) trading days.
    # Use a bounded scan in case calendar doesn't include today_trading due to missing holidays data.
    try:
        idx = max(0, min(len(cal) - 1, cal.index(today_trading)))
    except Exception:
        idx = 0
        for i, d in enumerate(cal):
            if d <= today_trading:
                idx = i
            else:
                break
    j = max(0, idx - (n - 1))
    return cal[j]


@dataclass(frozen=True)
class UpdateResult:
    symbol: str
    expected_first: str | None
    expected_last: str | None
    status_before: str | None
    action: str  # updated|skipped|failed
    error: str | None = None


def run_update(
    *,
    exchange: str,
    var: str,
    data_dir: Path,
    runs_dir: Path,
    symbols: list[str] | None = None,
    recent_expired_trading_days: int = 10,
    refresh_catalog: bool = True,
    chunk_days: int = 5,
) -> tuple[Path, dict[str, Any]]:
    """
    Remote-aware daily Update:
    - Refresh contract catalog (network) by default.
    - Select active contracts + recently expired contracts (within last N trading days).
    - For each candidate, download missing trading days into QuestDB (canonical) via `tq_ingest`.
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    only = {str(s).strip() for s in (symbols or []) if str(s).strip()} or None

    from ghtrader.tqsdk_catalog import get_contract_catalog

    t0 = time.time()
    cat = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=bool(refresh_catalog))
    if not bool(cat.get("ok", False)):
        report = {
            "run_id": uuid.uuid4().hex[:12],
            "created_at": _now_iso(),
            "exchange": ex,
            "var": v,
            "data_dir": str(data_dir),
            "runs_dir": str(runs_dir),
            "ok": False,
            "error": str(cat.get("error") or "tqsdk_catalog_failed"),
            "results": [],
        }
        out_path = runs_dir / "update" / f"{report['run_id']}.json"
        _write_json_atomic(out_path, report)
        return out_path, report

    contracts = list(cat.get("contracts") or [])

    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    today = datetime.now(timezone.utc).date()
    today_trading = _last_trading_day_leq(cal, today)
    cutoff = _recent_cutoff_trading_day(cal, today_trading, int(recent_expired_trading_days))

    # Candidate selection based on remote metadata (active + recently expired).
    candidates: list[dict[str, Any]] = []
    for c in contracts:
        try:
            sym = str(c.get("symbol") or "").strip()
        except Exception:
            sym = ""
        if not sym:
            continue
        if only is not None and sym not in only:
            continue

        exp = c.get("expired")
        expired_b = bool(exp) if exp is not None else None

        if only is not None:
            candidates.append(c)
            continue

        if expired_b is False:
            candidates.append(c)
            continue

        if expired_b is True:
            exp_day = _parse_iso_date_prefix(c.get("expire_datetime"))
            if exp_day is not None and exp_day >= cutoff:
                candidates.append(c)
            continue

    # PRD: QuestDB-first - fetch coverage from QuestDB index before computing statuses.
    from ghtrader.control.contract_status import compute_contract_statuses

    syms = [str(c.get("symbol") or "").strip() for c in candidates if str(c.get("symbol") or "").strip()]
    questdb_cov: dict[str, dict[str, Any]] | None = None
    try:
        from ghtrader.questdb_client import make_questdb_query_config_from_env
        from ghtrader.questdb_index import INDEX_TABLE_V2, ensure_index_tables, query_contract_coverage_from_index

        cfg = make_questdb_query_config_from_env()
        ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
        questdb_cov = query_contract_coverage_from_index(
            cfg=cfg, symbols=syms, lake_version="v2", ticks_lake="raw", index_table=INDEX_TABLE_V2
        )
        log.info("update.questdb_coverage_ok", n_symbols=len(questdb_cov or {}))
    except Exception as e:
        log.warning("update.questdb_coverage_failed", error=str(e))
        questdb_cov = None

    st = compute_contract_statuses(
        exchange=ex,
        var=v,
        data_dir=data_dir,
        contracts=candidates,
        questdb_cov_by_symbol=questdb_cov,
    )
    rows = list(st.get("contracts") or [])

    # Select contracts that need action (PRD status taxonomy: unindexed, not-downloaded, incomplete, stale).
    needs = [r for r in rows if str(r.get("status") or "") in {"unindexed", "not-downloaded", "incomplete", "stale"}] if only is None else rows

    from ghtrader.tq_ingest import download_historical_ticks

    results: list[dict[str, Any]] = []
    n_failed = 0
    n_skipped = 0
    n_updated = 0
    for r in needs:
        sym = str(r.get("symbol") or "").strip()
        exp0 = str(r.get("expected_first") or "").strip() or None
        exp1 = str(r.get("expected_last") or "").strip() or None
        st0 = str(r.get("status") or "").strip() or None
        if not sym or not exp0 or not exp1:
            n_skipped += 1
            results.append(
                {
                    "symbol": sym,
                    "expected_first": exp0,
                    "expected_last": exp1,
                    "status_before": st0,
                    "action": "skipped",
                    "error": "missing_expected_range",
                }
            )
            continue
        try:
            d0 = date.fromisoformat(exp0)
            d1 = date.fromisoformat(exp1)
        except Exception:
            n_skipped += 1
            results.append(
                {
                    "symbol": sym,
                    "expected_first": exp0,
                    "expected_last": exp1,
                    "status_before": st0,
                    "action": "skipped",
                    "error": "invalid_expected_range",
                }
            )
            continue

        try:
            log.info("update.contract_start", symbol=sym, expected_first=exp0, expected_last=exp1, status=st0)
            download_historical_ticks(
                symbol=sym,
                start_date=d0,
                end_date=d1,
                data_dir=data_dir,
                chunk_days=int(chunk_days),
            )
            n_updated += 1
            results.append(
                {
                    "symbol": sym,
                    "expected_first": exp0,
                    "expected_last": exp1,
                    "status_before": st0,
                    "action": "updated",
                    "error": None,
                }
            )
        except Exception as e:
            n_failed += 1
            results.append(
                {
                    "symbol": sym,
                    "expected_first": exp0,
                    "expected_last": exp1,
                    "status_before": st0,
                    "action": "failed",
                    "error": str(e),
                }
            )

    run_id = uuid.uuid4().hex[:12]
    report = {
        "run_id": run_id,
        "created_at": _now_iso(),
        "exchange": ex,
        "var": v,
        "data_dir": str(data_dir),
        "runs_dir": str(runs_dir),
        "ok": True,
        "catalog_refresh": bool(refresh_catalog),
        "recent_expired_trading_days": int(recent_expired_trading_days),
        "today_trading": today_trading.isoformat(),
        "recent_expired_cutoff": cutoff.isoformat(),
        "candidates": int(len(candidates)),
        "needs_action": int(len(needs)),
        "summary": {"updated": int(n_updated), "skipped": int(n_skipped), "failed": int(n_failed)},
        "seconds": float(time.time() - t0),
        "results": results,
    }
    out_path = runs_dir / "update" / f"{run_id}.json"
    _write_json_atomic(out_path, report)
    return out_path, report

