from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

from ghtrader.json_io import write_json_atomic

log = structlog.get_logger()

Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class Finding:
    severity: Severity
    code: str
    message: str
    path: str | None = None
    extra: dict[str, Any] | None = None


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


def _qdb_cfg():
    from ghtrader.questdb_client import make_questdb_query_config_from_env

    return make_questdb_query_config_from_env()


def _l5_condition_sql() -> str:
    from ghtrader.l5_detection import l5_sql_condition

    return l5_sql_condition()


def _chunks(xs: list[str], n: int) -> list[list[str]]:
    nn = max(1, int(n))
    return [xs[i : i + nn] for i in range(0, len(xs), nn)]


def _fetch_index_rows(
    *,
    cfg,
    symbols: list[str],
    ticks_lake: str,
    lake_version: str,
    index_table: str,
    connect_timeout_s: int = 2,
) -> dict[tuple[str, str], dict[str, Any]]:
    from ghtrader.questdb_client import connect_pg

    idx = str(index_table).strip()
    tl = str(ticks_lake).lower().strip()
    lv = str(lake_version).lower().strip()
    syms = [str(s).strip() for s in symbols if str(s).strip()]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    if not idx or not syms:
        return out

    cols = [
        "symbol",
        "cast(trading_day as string) AS trading_day",
        "rows_total",
        "first_datetime_ns",
        "last_datetime_ns",
        "l5_present",
        "row_hash_min",
        "row_hash_max",
        "row_hash_sum",
        "row_hash_sum_abs",
    ]
    sql_base = f"SELECT {', '.join(cols)} FROM {idx} WHERE symbol IN ({{ph}}) AND ticks_lake=%s AND lake_version=%s"
    for batch in _chunks(syms, 500):
        ph = ", ".join(["%s"] * len(batch))
        sql = sql_base.format(ph=ph)
        params: list[Any] = list(batch) + [tl, lv]
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for r in cur.fetchall():
                    try:
                        sym = str(r[0])
                        td = str(r[1])
                        out[(sym, td)] = {
                            "rows_total": (int(r[2]) if r[2] is not None else None),
                            "first_datetime_ns": (int(r[3]) if r[3] is not None else None),
                            "last_datetime_ns": (int(r[4]) if r[4] is not None else None),
                            "l5_present": bool(r[5]) if r[5] is not None else False,
                            "row_hash_min": (int(r[6]) if r[6] is not None else None),
                            "row_hash_max": (int(r[7]) if r[7] is not None else None),
                            "row_hash_sum": (int(r[8]) if r[8] is not None else None),
                            "row_hash_sum_abs": (int(r[9]) if r[9] is not None else None),
                        }
                    except Exception:
                        continue
    return out


def _compute_day_aggregates_from_ticks_table(
    *,
    cfg,
    ticks_table: str,
    symbols: list[str],
    ticks_lake: str,
    lake_version: str,
    connect_timeout_s: int = 2,
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Compute per-(symbol,trading_day) aggregates from the canonical ticks table.

    Returns a dict keyed by (symbol, trading_day ISO) with:
      rows_total, first_datetime_ns, last_datetime_ns, l5_present,
      row_hash_min/max/sum/sum_abs (best-effort; may be None if `row_hash` absent).
    """
    from ghtrader.questdb_client import connect_pg

    tbl = str(ticks_table).strip()
    tl = str(ticks_lake).lower().strip()
    lv = str(lake_version).lower().strip()
    syms = [str(s).strip() for s in symbols if str(s).strip()]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    if not tbl or not syms:
        return out

    l5_i = f"max(CASE WHEN {_l5_condition_sql()} THEN 1 ELSE 0 END) AS l5_present_i"
    for batch in _chunks(syms, 200):
        placeholders = ", ".join(["%s"] * len(batch))
        where = [f"symbol IN ({placeholders})", "ticks_lake = %s", "lake_version = %s"]
        params: list[Any] = list(batch) + [tl, lv]

        sql = (
            "SELECT symbol, cast(trading_day as string) AS trading_day, "
            "count() AS rows_total, min(datetime_ns) AS first_ns, max(datetime_ns) AS last_ns, "
            f"{l5_i}, "
            "min(row_hash) AS row_hash_min, max(row_hash) AS row_hash_max, "
            "sum(row_hash) AS row_hash_sum, sum(abs(row_hash)) AS row_hash_sum_abs "
            f"FROM {tbl} "
            f"WHERE {' AND '.join(where)} "
            "GROUP BY symbol, trading_day"
        )
        sql_legacy = (
            "SELECT symbol, cast(trading_day as string) AS trading_day, "
            "count() AS rows_total, min(datetime_ns) AS first_ns, max(datetime_ns) AS last_ns, "
            f"{l5_i} "
            f"FROM {tbl} "
            f"WHERE {' AND '.join(where)} "
            "GROUP BY symbol, trading_day"
        )

        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                has_row_hash = True
                try:
                    cur.execute(sql, params)
                except Exception:
                    has_row_hash = False
                    cur.execute(sql_legacy, params)

                for row in cur.fetchall():
                    try:
                        sym = str(row[0])
                        td = str(row[1])
                        rows_total = int(row[2]) if row[2] is not None else 0
                        first_ns = int(row[3]) if row[3] is not None else None
                        last_ns = int(row[4]) if row[4] is not None else None
                        l5_present = bool(int(row[5] or 0) > 0)
                        rh_min = int(row[6]) if has_row_hash and row[6] is not None else None
                        rh_max = int(row[7]) if has_row_hash and row[7] is not None else None
                        rh_sum = int(row[8]) if has_row_hash and row[8] is not None else None
                        rh_sum_abs = int(row[9]) if has_row_hash and row[9] is not None else None
                        out[(sym, td)] = {
                            "rows_total": int(rows_total),
                            "first_datetime_ns": first_ns,
                            "last_datetime_ns": last_ns,
                            "l5_present": bool(l5_present),
                            "row_hash_min": rh_min,
                            "row_hash_max": rh_max,
                            "row_hash_sum": rh_sum,
                            "row_hash_sum_abs": rh_sum_abs,
                        }
                    except Exception:
                        continue

    return out


def audit_ticks_index_vs_table(
    *,
    ticks_table: str,
    ticks_lake: str,
    lake_version: str = "v2",
    symbols: list[str] | None = None,
    index_table: str = "ghtrader_symbol_day_index_v2",
    connect_timeout_s: int = 2,
) -> list[Finding]:
    """
    Verify that `ghtrader_symbol_day_index_v2` matches aggregates computed from the canonical ticks table.
    """
    from ghtrader.questdb_index import ensure_index_tables, list_symbols_from_index

    cfg = _qdb_cfg()
    ensure_index_tables(cfg=cfg, index_table=index_table, connect_timeout_s=connect_timeout_s)

    tl = str(ticks_lake).lower().strip()
    lv = str(lake_version).lower().strip() or "v2"

    if symbols:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
    else:
        # Prefer index-backed symbol listing; if index is empty, we cannot audit without an explicit symbol list.
        syms = list_symbols_from_index(cfg=cfg, lake_version=lv, ticks_lake=tl, index_table=index_table, limit=200000)

    if not syms:
        return [Finding("warning", "no_symbols", f"No symbols to audit for ticks_lake={tl}", extra={"ticks_lake": tl})]

    computed = _compute_day_aggregates_from_ticks_table(
        cfg=cfg,
        ticks_table=ticks_table,
        symbols=syms,
        ticks_lake=tl,
        lake_version=lv,
        connect_timeout_s=connect_timeout_s,
    )

    index_rows = _fetch_index_rows(
        cfg=cfg,
        symbols=syms,
        ticks_lake=tl,
        lake_version=lv,
        index_table=index_table,
        connect_timeout_s=connect_timeout_s,
    )

    findings: list[Finding] = []
    for (sym, td), c in computed.items():
        i = index_rows.get((sym, td))
        if i is None:
            findings.append(
                Finding(
                    "error",
                    "index_missing",
                    "Missing index row for symbol/day",
                    extra={"symbol": sym, "trading_day": td, "ticks_lake": tl, "computed": c},
                )
            )
            continue

        for k in ["rows_total", "first_datetime_ns", "last_datetime_ns", "l5_present"]:
            if c.get(k) != i.get(k):
                findings.append(
                    Finding(
                        "error",
                        "index_mismatch",
                        f"Index mismatch for {k}",
                        extra={"symbol": sym, "trading_day": td, "ticks_lake": tl, "field": k, "index": i.get(k), "computed": c.get(k)},
                    )
                )

        # Checksum aggregates are best-effort: only compare when both sides are non-null.
        for k in ["row_hash_min", "row_hash_max", "row_hash_sum", "row_hash_sum_abs"]:
            cv = c.get(k)
            iv = i.get(k)
            if cv is None or iv is None:
                continue
            if int(cv) != int(iv):
                findings.append(
                    Finding(
                        "error",
                        "checksum_mismatch",
                        f"Index checksum mismatch for {k}",
                        extra={"symbol": sym, "trading_day": td, "ticks_lake": tl, "field": k, "index": int(iv), "computed": int(cv)},
                    )
                )

        # Basic sanity
        try:
            if i.get("first_datetime_ns") is not None and i.get("last_datetime_ns") is not None:
                if int(i["first_datetime_ns"]) > int(i["last_datetime_ns"]):
                    findings.append(
                        Finding(
                            "error",
                            "bounds_invalid",
                            "first_datetime_ns > last_datetime_ns",
                            extra={"symbol": sym, "trading_day": td, "ticks_lake": tl},
                        )
                    )
        except Exception:
            pass

    # Orphan rows: index has entry but computed has no rows.
    for (sym, td), i in index_rows.items():
        if (sym, td) in computed:
            continue
        if int(i.get("rows_total") or 0) > 0:
            findings.append(
                Finding(
                    "warning",
                    "index_orphan",
                    "Index row present but no rows found in ticks table for symbol/day (possible deletion?)",
                    extra={"symbol": sym, "trading_day": td, "ticks_lake": tl, "index": i},
                )
            )

    return findings


def audit_completeness(
    *,
    data_dir: Path,
    runs_dir: Path,
    lake_version: str = "v2",
    symbols: list[str] | None = None,
    exchange: str | None = None,
    var: str | None = None,
    refresh_catalog: bool = False,
) -> list[Finding]:
    """
    Completeness audit (QuestDB-first):
    - Uses TqSdk catalog metadata (open_date / expire_datetime) when available.
    - Computes expected trading days and compares QuestDB present_dates vs expected.
    - Treats `ghtrader_no_data_days_v2` as explicit exclusions.
    """
    from ghtrader.questdb_index import INDEX_TABLE_V2, list_no_data_trading_days, query_contract_coverage_from_index
    from ghtrader.tqsdk_catalog import get_contract_catalog
    from ghtrader.trading_calendar import get_trading_calendar, get_trading_days

    _ = lake_version  # v2-only

    ex_filter = str(exchange).upper().strip() if exchange else None
    v_filter = str(var).lower().strip() if var else None

    cal = get_trading_calendar(data_dir=data_dir, refresh=False, allow_download=False)
    today = datetime.now(timezone.utc).date()
    today_trading = _last_trading_day_leq(cal, today)

    # Determine target symbols via inputs or catalog.
    target_syms: set[str] = set()
    if symbols:
        target_syms = {str(s).strip() for s in symbols if str(s).strip()}
    elif ex_filter and v_filter:
        cat = get_contract_catalog(
            exchange=ex_filter,
            var=v_filter,
            runs_dir=runs_dir,
            ttl_s=10**9,
            refresh=bool(refresh_catalog),
        )
        target_syms = {str(r.get("symbol") or "").strip() for r in (cat.get("contracts") or []) if str(r.get("symbol") or "").strip()}
    else:
        return [Finding("warning", "missing_scope", "Completeness audit requires symbols or (exchange,var) filters")]

    if not target_syms:
        return [Finding("warning", "no_symbols", "No symbols selected for completeness audit")]

    cfg = _qdb_cfg()
    cov = query_contract_coverage_from_index(cfg=cfg, symbols=sorted(target_syms), lake_version="v2", ticks_lake="raw", index_table=INDEX_TABLE_V2)

    findings: list[Finding] = []
    for sym in sorted(target_syms):
        meta: dict[str, Any] = {}
        if ex_filter and v_filter:
            # Reuse the same cached catalog blob; find this symbol row.
            try:
                cat = get_contract_catalog(
                    exchange=ex_filter,
                    var=v_filter,
                    runs_dir=runs_dir,
                    ttl_s=10**9,
                    refresh=False,
                )
                for r in cat.get("contracts") or []:
                    if str(r.get("symbol") or "").strip() == sym:
                        meta = dict(r)
                        break
            except Exception:
                meta = {}

        open_date_s = str(meta.get("open_date") or "").strip() or None
        exp_dt_s = str(meta.get("expire_datetime") or "").strip() or None
        if not open_date_s:
            findings.append(Finding("warning", "missing_open_date", "Missing open_date in catalog metadata", extra={"symbol": sym}))
            continue

        try:
            start = date.fromisoformat(open_date_s)
        except Exception:
            findings.append(Finding("warning", "bad_open_date", "Invalid open_date", extra={"symbol": sym, "open_date": open_date_s}))
            continue

        def _parse_expire_date(s: str) -> date | None:
            ss = str(s).strip()
            if not ss:
                return None
            try:
                # Handle common 'Z' suffix.
                if ss.endswith("Z"):
                    ss = ss[:-1] + "+00:00"
                return datetime.fromisoformat(ss).date()
            except Exception:
                return None

        # End at min(expire_date, today_trading) when expire is known; else today_trading.
        end = today_trading
        if exp_dt_s:
            exp_d = _parse_expire_date(exp_dt_s)
            if exp_d is not None:
                end = min(end, exp_d)

        if end < start:
            continue

        expected_days = set(get_trading_days(market=ex_filter, start=start, end=end, data_dir=data_dir, refresh=False))
        present = set((cov.get(sym) or {}).get("present_dates") or set())
        present_days = {date.fromisoformat(s) for s in present if str(s).strip()}
        no_data = set()
        try:
            no_data = set(
                list_no_data_trading_days(
                    cfg=cfg,
                    symbol=sym,
                    start_day=start,
                    end_day=end,
                    lake_version="v2",
                    ticks_lake="raw",
                )
            )
        except Exception:
            no_data = set()

        missing = sorted([d for d in expected_days if d not in present_days and d not in no_data])
        if missing:
            findings.append(
                Finding(
                    "error",
                    "missing_days",
                    "Missing expected trading days in QuestDB index",
                    extra={
                        "symbol": sym,
                        "missing_days": [d.isoformat() for d in missing[:200]],
                        "missing_count": int(len(missing)),
                        "expected_count": int(len(expected_days)),
                        "present_count": int(len(present_days)),
                        "no_data_count": int(len(no_data)),
                    },
                )
            )

    return findings


def write_audit_report(*, runs_dir: Path, report: dict[str, Any]) -> Path:
    out_dir = runs_dir / "audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = report.get("run_id") or uuid.uuid4().hex[:12]
    out_path = out_dir / f"{run_id}.json"
    write_json_atomic(out_path, report)
    return out_path


def run_audit(
    *,
    data_dir: Path,
    runs_dir: Path,
    scopes: list[str],
    lake_version: str = "v2",
    symbols: list[str] | None = None,
    exchange: str | None = None,
    var: str | None = None,
    refresh_catalog: bool = False,
) -> tuple[Path, dict[str, Any]]:
    run_id = uuid.uuid4().hex[:12]
    findings: list[Finding] = []

    _ = data_dir  # unused except for completeness + calendar cache

    # Canonical tick table names (v2-only)
    ticks_raw_table = "ghtrader_ticks_raw_v2"
    ticks_main_l5_table = "ghtrader_ticks_main_l5_v2"

    only = [str(s).strip() for s in (symbols or []) if str(s).strip()] or None

    if "ticks" in scopes or "all" in scopes:
        findings.extend(
            audit_ticks_index_vs_table(
                ticks_table=ticks_raw_table,
                ticks_lake="raw",
                lake_version=str(lake_version),
                symbols=only,
            )
        )

    if "main_l5" in scopes or "all" in scopes:
        findings.extend(
            audit_ticks_index_vs_table(
                ticks_table=ticks_main_l5_table,
                ticks_lake="main_l5",
                lake_version=str(lake_version),
                symbols=only,
            )
        )

    # Feature/label audits are intentionally lightweight (QuestDB-only).
    if "features" in scopes or "all" in scopes:
        try:
            from ghtrader.questdb_client import connect_pg
            from ghtrader.questdb_features_labels import FEATURE_BUILDS_TABLE_V2

            cfg = _qdb_cfg()
            with connect_pg(cfg, connect_timeout_s=2) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT count() FROM {FEATURE_BUILDS_TABLE_V2}")
                    _ = cur.fetchone()
        except Exception as e:
            findings.append(Finding("warning", "features_check_failed", "Features table check failed", extra={"error": str(e)}))

    if "labels" in scopes or "all" in scopes:
        try:
            from ghtrader.questdb_client import connect_pg
            from ghtrader.questdb_features_labels import LABEL_BUILDS_TABLE_V2

            cfg = _qdb_cfg()
            with connect_pg(cfg, connect_timeout_s=2) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT count() FROM {LABEL_BUILDS_TABLE_V2}")
                    _ = cur.fetchone()
        except Exception as e:
            findings.append(Finding("warning", "labels_check_failed", "Labels table check failed", extra={"error": str(e)}))

    if "completeness" in scopes:
        findings.extend(
            audit_completeness(
                data_dir=data_dir,
                runs_dir=runs_dir,
                lake_version=str(lake_version),
                symbols=only,
                exchange=exchange,
                var=var,
                refresh_catalog=bool(refresh_catalog),
            )
        )

    n_errors = sum(1 for f in findings if f.severity == "error")
    n_warnings = sum(1 for f in findings if f.severity == "warning")

    report = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(data_dir),
        "scopes": scopes,
        "lake_version": str(lake_version),
        "completeness_exchange": str(exchange).upper().strip() if exchange else "",
        "completeness_var": str(var).lower().strip() if var else "",
        "completeness_refresh_catalog": bool(refresh_catalog),
        "summary": {"errors": int(n_errors), "warnings": int(n_warnings), "total": int(len(findings))},
        "findings": [
            {"severity": f.severity, "code": f.code, "message": f.message, "path": f.path, "extra": f.extra or {}}
            for f in findings
        ],
    }
    out_path = write_audit_report(runs_dir=runs_dir, report=report)
    return out_path, report

