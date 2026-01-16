"""
Contract status computation for Data Hub.

PRD: QuestDB is the canonical data source. This module must not depend on local file-based tick
storage scans for status/coverage.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.trading_calendar import get_trading_calendar

log = structlog.get_logger()


def infer_contract_date_range(symbol: str) -> tuple[date | None, date | None]:
    """
    Infer approximate trading date range from SHFE contract symbol.

    SHFE futures contracts have a YYMM suffix indicating expiry month.
    Trading typically starts ~12-13 months before expiry and ends on the
    15th of the expiry month (last trading day).
    """
    import re

    if not symbol:
        return None, None

    match = re.match(r"^(?:[A-Z]+\.)?([a-zA-Z]+)(\d{4})$", str(symbol).strip())
    if not match:
        return None, None

    yymm = match.group(2)
    if len(yymm) != 4:
        return None, None
    try:
        yy = int(yymm[:2])
        mm = int(yymm[2:])
    except ValueError:
        return None, None
    if mm < 1 or mm > 12:
        return None, None

    year = (1900 + yy) if yy >= 90 else (2000 + yy)
    try:
        end_date = date(year, mm, 15)
    except ValueError:
        return None, None

    try:
        start_date = date(year - 1, mm, 1)
    except ValueError:
        return None, None

    return start_date, end_date


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _last_trading_day_leq(cal: list[date], today: date) -> date:
    if not cal:
        d = today
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d
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


def _count_trading_days_between(*, cal: list[date], start: date, end: date) -> int:
    if end < start:
        return 0
    if not cal:
        # Weekday fallback (best-effort)
        n = 0
        d = start
        while d <= end:
            if d.weekday() < 5:
                n += 1
            d += timedelta(days=1)
        return n
    # Binary search in a sorted calendar list.
    from bisect import bisect_left, bisect_right

    i0 = bisect_left(cal, start)
    i1 = bisect_right(cal, end)
    return int(max(0, i1 - i0))


def _raw_contract_from_symbol(*, symbol: str, var: str) -> str | None:
    s = str(symbol).strip()
    if not s:
        return None
    if "." in s:
        tail = s.split(".", 1)[1].strip()
    else:
        tail = s
    tail = tail.strip()
    if not tail:
        return None
    # Best-effort: ensure it matches variety prefix.
    v = str(var).lower().strip()
    if v and not tail.lower().startswith(v):
        return None
    return tail


def compute_contract_statuses(
    *,
    exchange: str,
    var: str,
    data_dir: Path,
    contracts: list[dict[str, Any]],
    questdb_cov_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Compute per-contract status for the Data Hub.

    QuestDB-first:
    - Coverage and bounds come from `ghtrader_symbol_day_index_v2`.
    - No filesystem tick-store scans are performed.
    """
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    lv = "v2"

    cal = get_trading_calendar(data_dir=data_dir, refresh=False, allow_download=False)
    today = datetime.now(timezone.utc).date()
    today_trading = _last_trading_day_leq(cal, today)
    checked_at = _now_iso()
    now_s = time.time()

    try:
        lag_min = float(os.environ.get("GHTRADER_CONTRACTS_TICK_LAG_STALE_MINUTES", "120") or "120")
    except Exception:
        lag_min = 120.0
    lag_threshold_sec = float(max(0.0, lag_min * 60.0))

    # If caller did not provide coverage, best-effort fetch from QuestDB index.
    cov = questdb_cov_by_symbol
    if cov is None:
        try:
            from ghtrader.questdb_client import make_questdb_query_config_from_env
            from ghtrader.questdb_index import INDEX_TABLE_V2, ensure_index_tables, query_contract_coverage_from_index

            cfg = make_questdb_query_config_from_env()
            ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
            syms = [str((r or {}).get("symbol") or "").strip() for r in contracts if str((r or {}).get("symbol") or "").strip()]
            cov = query_contract_coverage_from_index(cfg=cfg, symbols=syms, lake_version=lv, ticks_lake="raw", index_table=INDEX_TABLE_V2)
        except Exception:
            cov = None

    out: list[dict[str, Any]] = []
    for c in contracts:
        sym = str((c or {}).get("symbol") or "").strip()
        if not sym:
            continue

        expired = (c or {}).get("expired")
        expired_b = bool(expired) if expired is not None else None
        exp_dt = (c or {}).get("expire_datetime")
        exp_dt_s = str(exp_dt).strip() if exp_dt not in (None, "") else None
        open_dt = (c or {}).get("open_date")
        open_dt_s = str(open_dt).strip() if open_dt not in (None, "") else None

        raw = _raw_contract_from_symbol(symbol=sym, var=v)

        qc = cov.get(sym) if isinstance(cov, dict) else None
        present_dates_raw = qc.get("present_dates") if isinstance(qc, dict) else None
        downloaded_set: set[str] = set()
        if isinstance(present_dates_raw, set):
            downloaded_set = {str(d) for d in present_dates_raw}

        db_min = str(qc.get("first_tick_day") or "").strip() if isinstance(qc, dict) else ""
        db_max = str(qc.get("last_tick_day") or "").strip() if isinstance(qc, dict) else ""

        db_first_tick_ns = int(qc.get("first_tick_ns")) if isinstance(qc, dict) and qc.get("first_tick_ns") is not None else None
        db_last_tick_ns = int(qc.get("last_tick_ns")) if isinstance(qc, dict) and qc.get("last_tick_ns") is not None else None
        db_first_tick_ts = str(qc.get("first_tick_ts") or "").strip() if isinstance(qc, dict) else ""
        db_last_tick_ts = str(qc.get("last_tick_ts") or "").strip() if isinstance(qc, dict) else ""

        db_last_tick_age_sec: float | None = None
        if db_last_tick_ns is not None and db_last_tick_ns > 0:
            try:
                db_last_tick_age_sec = float(max(0.0, now_s - (float(db_last_tick_ns) / 1_000_000_000.0)))
            except Exception:
                db_last_tick_age_sec = None

        # Expected window (prefer catalog, fall back to QuestDB bounds, then symbol inference).
        expected_first: date | None = None
        if open_dt_s:
            try:
                expected_first = date.fromisoformat(open_dt_s[:10])
            except Exception:
                expected_first = None
        if expected_first is None and db_min:
            try:
                expected_first = date.fromisoformat(db_min[:10])
            except Exception:
                expected_first = None
        if expected_first is None:
            a0, _ = infer_contract_date_range(sym)
            expected_first = a0

        expire_trading: date | None = None
        if exp_dt_s:
            try:
                exp_day = date.fromisoformat(exp_dt_s[:10])
                expire_trading = _last_trading_day_leq(cal, exp_day)
            except Exception:
                expire_trading = None

        expected_last: date | None = None
        if expired_b is False:
            expected_last = today_trading if expected_first else None
        else:
            expected_last = expire_trading if expire_trading else None
            if expected_last is None and db_max and expired_b is not False:
                try:
                    expected_last = date.fromisoformat(db_max[:10])
                except Exception:
                    expected_last = None
            if expected_last is None:
                _, b0 = infer_contract_date_range(sym)
                expected_last = b0

        days_expected: int | None = None
        days_done: int | None = None
        pct: float | None = None

        if expected_first and expected_last and expected_last >= expected_first:
            expected_iso0 = expected_first.isoformat()
            expected_iso1 = expected_last.isoformat()
            days_expected = _count_trading_days_between(cal=cal, start=expected_first, end=expected_last)

            downloaded_in_range = {d for d in downloaded_set if expected_iso0 <= d <= expected_iso1}

            # No-data markers count as done (QuestDB-only).
            no_data_days: set[date] = set()
            try:
                from ghtrader.questdb_client import make_questdb_query_config_from_env
                from ghtrader.questdb_index import list_no_data_trading_days

                cfg = make_questdb_query_config_from_env()
                no_data_days = set(
                    list_no_data_trading_days(
                        cfg=cfg,
                        symbol=sym,
                        start_day=expected_first,
                        end_day=expected_last,
                        lake_version=lv,
                        ticks_lake="raw",
                    )
                )
            except Exception:
                no_data_days = set()

            # Active contracts: do not treat today's trading day as no-data.
            if expired_b is False and expected_last == today_trading and no_data_days:
                no_data_days = {d for d in no_data_days if d != today_trading}

            no_data_effective = 0
            for d in no_data_days:
                ds = d.isoformat()
                if ds in downloaded_in_range:
                    continue
                no_data_effective += 1

            done = int(len(downloaded_in_range) + int(no_data_effective))
            days_done = int(min(done, days_expected) if days_expected and days_expected > 0 else done)
            pct = float(days_done / days_expected) if days_expected and days_expected > 0 else 0.0
            pct = float(max(0.0, min(1.0, pct)))

        # Status taxonomy (QuestDB-first).
        status = "unknown"
        stale_reason: str | None = None
        index_missing = (cov is not None and qc is None)
        if cov is not None and qc is None:
            status = "unindexed"
        elif not downloaded_set:
            status = "not-downloaded"
        elif days_expected is None or days_done is None:
            status = "unknown"
        else:
            if expired_b is False:
                if pct is not None and pct >= 1.0:
                    status = "complete"
                elif db_max and db_max < today_trading.isoformat():
                    status = "stale"
                    stale_reason = "missing_trading_day"
                else:
                    status = "incomplete"
            else:
                status = "complete" if pct is not None and pct >= 1.0 else "incomplete"

        if expired_b is False and status == "complete":
            if db_max and db_max == today_trading.isoformat():
                if db_last_tick_age_sec is not None and db_last_tick_age_sec > lag_threshold_sec:
                    status = "stale"
                    stale_reason = "tick_lag"

        out.append(
            {
                "symbol": sym,
                "raw_contract": raw,
                "catalog_source": str((c or {}).get("catalog_source") or ""),
                "expired": expired_b,
                "expire_datetime": exp_dt_s,
                "open_date": open_dt_s,
                "days_present": int(len(downloaded_set)),
                "db_min": db_min or None,
                "db_max": db_max or None,
                "db_first_tick_ns": db_first_tick_ns,
                "db_last_tick_ns": db_last_tick_ns,
                "db_first_tick_ts": (db_first_tick_ts or None),
                "db_last_tick_ts": (db_last_tick_ts or None),
                "db_last_tick_age_sec": db_last_tick_age_sec,
                "checked_at": checked_at,
                "stale_reason": stale_reason,
                "tick_lag_stale_threshold_sec": (lag_threshold_sec if expired_b is False else None),
                "days_expected": days_expected,
                "days_done": days_done,
                "pct": pct,
                "status": status,
                "index_missing": bool(index_missing),
                "expected_first": expected_first.isoformat() if expected_first else None,
                "expected_last": expected_last.isoformat() if expected_last else None,
                # QuestDB coverage summary (used by Data Hub table rendering).
                "questdb_coverage": qc if isinstance(qc, dict) else {},
            }
        )

    out.sort(key=lambda r: str(r.get("symbol") or ""))

    # Summary (QuestDB-first)
    symbols_total = int(len(out))
    symbols_indexed = int(sum(1 for r in out if r.get("index_missing") is not True))
    symbols_unindexed = int(symbols_total - symbols_indexed)
    symbols_complete = int(sum(1 for r in out if r.get("status") == "complete"))
    symbols_missing = int(sum(1 for r in out if r.get("status") in ("incomplete", "missing", "not-downloaded")))
    symbols_stale = int(sum(1 for r in out if r.get("status") == "stale"))

    index_healthy = bool(symbols_unindexed == 0) if symbols_total > 0 else True
    index_coverage_ratio = float(symbols_indexed / symbols_total) if symbols_total > 0 else 1.0

    return {
        "ok": True,
        "exchange": ex,
        "var": v,
        "lake_version": lv,
        "generated_at": checked_at,
        "local_checked_at": checked_at,
        "contracts": out,
        "active_ranges_available": False,
        "completeness": {
            "summary": {
                "symbols_total": symbols_total,
                "symbols_indexed": symbols_indexed,
                "symbols_unindexed": symbols_unindexed,
                "symbols_complete": symbols_complete,
                "symbols_missing": symbols_missing,
                "symbols_stale": symbols_stale,
                "index_healthy": index_healthy,
                "index_coverage_ratio": index_coverage_ratio,
                "bootstrap_recommended": (not index_healthy and symbols_unindexed > 0),
            }
        },
    }

