from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

from ghtrader.util.json_io import write_json_atomic

log = structlog.get_logger()

Severity = Literal["error", "warning"]

_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Finding:
    severity: Severity
    code: str
    message: str
    path: str | None = None
    extra: dict[str, Any] | None = None


from ghtrader.data.trading_calendar import last_trading_day_leq as _last_trading_day_leq_opt


def _last_trading_day_leq(cal: list[date], today: date) -> date:
    """Wrapper that returns today (not None) when calendar is empty."""
    result = _last_trading_day_leq_opt(cal, today)
    return result if result is not None else today


def _qdb_cfg():
    from ghtrader.questdb.client import make_questdb_query_config_from_env

    return make_questdb_query_config_from_env()


def _l5_condition_sql() -> str:
    from ghtrader.util.l5_detection import l5_sql_condition

    return l5_sql_condition()


def _chunks(xs: list[str], n: int) -> list[list[str]]:
    nn = max(1, int(n))
    return [xs[i : i + nn] for i in range(0, len(xs), nn)]


def _safe_ident(name: str) -> str:
    s = str(name or "").strip()
    if not _SAFE_IDENT_RE.match(s):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return s


def _table_columns(*, cfg, table: str, connect_timeout_s: int = 2) -> dict[str, str]:
    """
    Return a best-effort {column_name: type} mapping for a QuestDB table.
    """
    from ghtrader.questdb.client import connect_pg

    tbl = _safe_ident(table)
    # Prefer QuestDB-native table_columns() when available.
    for sql in [
        f"SELECT column, type FROM table_columns('{tbl}')",
        f"SELECT name, type FROM table_columns('{tbl}')",
    ]:
        try:
            with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall() or []
            out: dict[str, str] = {}
            for r in rows:
                try:
                    out[str(r[0])] = str(r[1])
                except Exception:
                    continue
            if out:
                return out
        except Exception:
            continue

    # Fallback: information_schema (PG-style).
    try:
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=%s",
                    (tbl,),
                )
                rows = cur.fetchall() or []
        out = {}
        for r in rows:
            try:
                out[str(r[0])] = str(r[1])
            except Exception:
                continue
        return out
    except Exception:
        return {}


def audit_schema(
    *,
    connect_timeout_s: int = 2,
) -> list[Finding]:
    """
    PRD-required schema conformity check for core QuestDB tables.
    """
    from ghtrader.questdb.index import INDEX_TABLE_V2, NO_DATA_TABLE_V2
    from ghtrader.data.ticks_schema import TICK_NUMERIC_COLUMNS

    cfg = _qdb_cfg()
    findings: list[Finding] = []

    ticks_required = {"symbol", "ts", "datetime_ns", "trading_day", "row_hash", "dataset_version", "ticks_kind"} | set(TICK_NUMERIC_COLUMNS)
    ticks_main_required = ticks_required | {"underlying_contract", "segment_id", "schedule_hash"}
    index_required = {
        "ts",
        "symbol",
        "trading_day",
        "ticks_kind",
        "dataset_version",
        "rows_total",
        "first_datetime_ns",
        "last_datetime_ns",
        "l5_present",
        "row_hash_min",
        "row_hash_max",
        "row_hash_sum",
        "row_hash_sum_abs",
        "updated_at",
    }
    no_data_required = {"ts", "symbol", "trading_day", "ticks_kind", "dataset_version", "reason", "created_at"}

    checks = [
        ("ghtrader_ticks_raw_v2", ticks_required),
        ("ghtrader_ticks_main_l5_v2", ticks_main_required),
        (INDEX_TABLE_V2, index_required),
        (NO_DATA_TABLE_V2, no_data_required),
    ]

    for table, required in checks:
        cols = _table_columns(cfg=cfg, table=str(table), connect_timeout_s=connect_timeout_s)
        if not cols:
            findings.append(
                Finding(
                    "warning",
                    "schema_columns_unavailable",
                    "Could not query table schema (QuestDB metadata unavailable)",
                    extra={"table": str(table)},
                )
            )
            continue
        missing = sorted([c for c in required if c not in cols])
        if missing:
            findings.append(
                Finding(
                    "error",
                    "schema_missing_columns",
                    "Missing required columns in QuestDB table",
                    extra={"table": str(table), "missing": missing},
                )
            )

    return findings


def _fetch_index_row(
    *,
    cfg,
    symbol: str,
    trading_day: str,
    ticks_kind: str,
    dataset_version: str,
    index_table: str,
    connect_timeout_s: int = 2,
) -> dict[str, Any] | None:
    from ghtrader.questdb.client import connect_pg

    idx = str(index_table).strip()
    if not idx:
        return None
    sym = str(symbol).strip()
    td = str(trading_day).strip()
    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip()
    if not sym or not td:
        return None
    sql = (
        f"SELECT rows_total, first_datetime_ns, last_datetime_ns, l5_present, "
        "row_hash_min, row_hash_max, row_hash_sum, row_hash_sum_abs "
        f"FROM {idx} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s "
        "LIMIT 1"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (sym, td, tk, dv))
            r = cur.fetchone()
    if not r:
        return None
    return {
        "rows_total": (int(r[0]) if r[0] is not None else None),
        "first_datetime_ns": (int(r[1]) if r[1] is not None else None),
        "last_datetime_ns": (int(r[2]) if r[2] is not None else None),
        "l5_present": bool(r[3]) if r[3] is not None else False,
        "row_hash_min": (int(r[4]) if r[4] is not None else None),
        "row_hash_max": (int(r[5]) if r[5] is not None else None),
        "row_hash_sum": (int(r[6]) if r[6] is not None else None),
        "row_hash_sum_abs": (int(r[7]) if r[7] is not None else None),
    }


def _sample_index_partitions(
    *,
    cfg,
    ticks_kind: str,
    dataset_version: str,
    index_table: str,
    symbols: list[str] | None = None,
    limit: int = 50,
    connect_timeout_s: int = 2,
) -> list[tuple[str, str]]:
    """
    Return a deterministic sample of (symbol, trading_day) keys from the index table.
    """
    from ghtrader.questdb.client import connect_pg

    idx = str(index_table).strip()
    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip()
    lim = max(1, min(int(limit or 50), 5000))

    sym_list = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    sym_where = ""
    params: list[Any] = [tk, dv]
    if sym_list:
        ph = ", ".join(["%s"] * len(sym_list))
        sym_where = f" AND symbol IN ({ph})"
        params.extend(sym_list)

    sql = (
        f"SELECT symbol, cast(trading_day as string) AS trading_day "
        f"FROM {idx} "
        f"WHERE ticks_kind=%s AND dataset_version=%s{sym_where} "
        "ORDER BY ts DESC "
        f"LIMIT {lim}"
    )
    out: list[tuple[str, str]] = []
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for r in cur.fetchall() or []:
                try:
                    sym = str(r[0]).strip()
                    td = str(r[1]).strip()
                except Exception:
                    continue
                if sym and td:
                    out.append((sym, td))
    return out


def _compute_partition_aggregates_from_ticks_table(
    *,
    cfg,
    ticks_table: str,
    symbol: str,
    trading_day: str,
    ticks_kind: str,
    dataset_version: str,
    connect_timeout_s: int = 2,
) -> dict[str, Any] | None:
    """
    Compute per-(symbol,trading_day) aggregates from a ticks table.
    """
    from ghtrader.questdb.client import connect_pg

    tbl = str(ticks_table).strip()
    if not tbl:
        return None
    sym = str(symbol).strip()
    td = str(trading_day).strip()
    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip()
    if not sym or not td:
        return None

    l5_i = f"max(CASE WHEN {_l5_condition_sql()} THEN 1 ELSE 0 END) AS l5_present_i"
    sql = (
        "SELECT "
        "count() AS rows_total, min(datetime_ns) AS first_ns, max(datetime_ns) AS last_ns, "
        f"{l5_i}, "
        "min(row_hash) AS row_hash_min, max(row_hash) AS row_hash_max, "
        "sum(row_hash) AS row_hash_sum, sum(abs(row_hash)) AS row_hash_sum_abs "
        f"FROM {tbl} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s"
    )
    sql_legacy = (
        "SELECT "
        "count() AS rows_total, min(datetime_ns) AS first_ns, max(datetime_ns) AS last_ns, "
        f"{l5_i} "
        f"FROM {tbl} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s"
    )
    params = (sym, td, tk, dv)

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            has_row_hash = True
            try:
                cur.execute(sql, params)
            except Exception:
                has_row_hash = False
                cur.execute(sql_legacy, params)
            r = cur.fetchone()
    if not r:
        return None

    rows_total = int(r[0] or 0)
    first_ns = int(r[1]) if r[1] is not None else None
    last_ns = int(r[2]) if r[2] is not None else None
    l5_present = bool(int(r[3] or 0) > 0)
    rh_min = int(r[4]) if has_row_hash and r[4] is not None else None
    rh_max = int(r[5]) if has_row_hash and r[5] is not None else None
    rh_sum = int(r[6]) if has_row_hash and r[6] is not None else None
    rh_sum_abs = int(r[7]) if has_row_hash and r[7] is not None else None
    return {
        "rows_total": int(rows_total),
        "first_datetime_ns": first_ns,
        "last_datetime_ns": last_ns,
        "l5_present": bool(l5_present),
        "row_hash_min": rh_min,
        "row_hash_max": rh_max,
        "row_hash_sum": rh_sum,
        "row_hash_sum_abs": rh_sum_abs,
    }


def audit_ticks_index_vs_table_sampled(
    *,
    ticks_table: str,
    ticks_kind: str,
    dataset_version: str = "v2",
    symbols: list[str] | None = None,
    index_table: str = "ghtrader_symbol_day_index_v2",
    sample_partitions: int = 50,
    connect_timeout_s: int = 2,
) -> list[Finding]:
    """
    PRD-required index consistency check (sampled partitions by default).

    If `symbols` is provided, this falls back to a full comparison for those symbols
    (all days), which is appropriate for targeted audits.
    """
    from ghtrader.questdb.index import ensure_index_tables

    cfg = _qdb_cfg()
    ensure_index_tables(cfg=cfg, index_table=index_table, connect_timeout_s=connect_timeout_s)

    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip() or "v2"

    findings: list[Finding] = []

    # Full check for explicitly provided symbols (targeted).
    if symbols:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
        computed = _compute_day_aggregates_from_ticks_table(
            cfg=cfg,
            ticks_table=ticks_table,
            symbols=syms,
            ticks_kind=tk,
            dataset_version=dv,
            connect_timeout_s=connect_timeout_s,
        )
        index_rows = _fetch_index_rows(
            cfg=cfg,
            symbols=syms,
            ticks_kind=tk,
            dataset_version=dv,
            index_table=index_table,
            connect_timeout_s=connect_timeout_s,
        )
        for (sym, td), c in computed.items():
            i = index_rows.get((sym, td))
            if i is None:
                findings.append(
                    Finding(
                        "error",
                        "index_missing",
                        "Missing index row for symbol/day",
                        extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv, "computed": c},
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
                            extra={
                                "symbol": sym,
                                "trading_day": td,
                                "ticks_kind": tk,
                                "dataset_version": dv,
                                "field": k,
                                "index": i.get(k),
                                "computed": c.get(k),
                            },
                        )
                    )

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
                            extra={
                                "symbol": sym,
                                "trading_day": td,
                                "ticks_kind": tk,
                                "dataset_version": dv,
                                "field": k,
                                "index": int(iv),
                                "computed": int(cv),
                            },
                        )
                    )
        return findings

    # Sampled check by (symbol,trading_day) partitions.
    parts = _sample_index_partitions(
        cfg=cfg,
        ticks_kind=tk,
        dataset_version=dv,
        index_table=index_table,
        symbols=None,
        limit=int(sample_partitions),
        connect_timeout_s=connect_timeout_s,
    )
    if not parts:
        return [
            Finding(
                "warning",
                "no_partitions",
                f"No index partitions to audit for ticks_kind={tk}",
                extra={"ticks_kind": tk, "dataset_version": dv},
            )
        ]

    for sym, td in parts:
        computed = _compute_partition_aggregates_from_ticks_table(
            cfg=cfg,
            ticks_table=ticks_table,
            symbol=sym,
            trading_day=td,
            ticks_kind=tk,
            dataset_version=dv,
            connect_timeout_s=connect_timeout_s,
        )
        index_row = _fetch_index_row(
            cfg=cfg,
            symbol=sym,
            trading_day=td,
            ticks_kind=tk,
            dataset_version=dv,
            index_table=index_table,
            connect_timeout_s=connect_timeout_s,
        )
        if index_row is None:
            findings.append(
                Finding(
                    "error",
                    "index_missing",
                    "Missing index row for sampled partition",
                    extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv},
                )
            )
            continue
        if computed is None:
            # Index row exists but no tick rows found.
            if int(index_row.get("rows_total") or 0) > 0:
                findings.append(
                    Finding(
                        "warning",
                        "index_orphan",
                        "Index row present but no rows found in ticks table for sampled partition",
                        extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv, "index": index_row},
                    )
                )
            continue

        for k in ["rows_total", "first_datetime_ns", "last_datetime_ns", "l5_present"]:
            if computed.get(k) != index_row.get(k):
                findings.append(
                    Finding(
                        "error",
                        "index_mismatch",
                        f"Index mismatch for {k}",
                        extra={
                            "symbol": sym,
                            "trading_day": td,
                            "ticks_kind": tk,
                            "dataset_version": dv,
                            "field": k,
                            "index": index_row.get(k),
                            "computed": computed.get(k),
                        },
                    )
                )

        for k in ["row_hash_min", "row_hash_max", "row_hash_sum", "row_hash_sum_abs"]:
            cv = computed.get(k)
            iv = index_row.get(k)
            if cv is None or iv is None:
                continue
            if int(cv) != int(iv):
                findings.append(
                    Finding(
                        "error",
                        "checksum_mismatch",
                        f"Index checksum mismatch for {k}",
                        extra={
                            "symbol": sym,
                            "trading_day": td,
                            "ticks_kind": tk,
                            "dataset_version": dv,
                            "field": k,
                            "index": int(iv),
                            "computed": int(cv),
                        },
                    )
                )

        try:
            if index_row.get("first_datetime_ns") is not None and index_row.get("last_datetime_ns") is not None:
                if int(index_row["first_datetime_ns"]) > int(index_row["last_datetime_ns"]):
                    findings.append(
                        Finding(
                            "error",
                            "bounds_invalid",
                            "first_datetime_ns > last_datetime_ns",
                            extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv},
                        )
                    )
        except Exception:
            pass

    return findings


def audit_ticks_quality_sampled(
    *,
    ticks_table: str,
    ticks_kind: str,
    dataset_version: str = "v2",
    index_table: str = "ghtrader_symbol_day_index_v2",
    symbols: list[str] | None = None,
    sample_partitions: int = 50,
    connect_timeout_s: int = 2,
) -> list[Finding]:
    """
    PRD-required datetime/duplicate-rate + basic sanity checks (sampled partitions).
    """
    from ghtrader.questdb.client import connect_pg

    cfg = _qdb_cfg()

    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip() or "v2"
    parts = _sample_index_partitions(
        cfg=cfg,
        ticks_kind=tk,
        dataset_version=dv,
        index_table=index_table,
        symbols=symbols,
        limit=int(sample_partitions),
        connect_timeout_s=connect_timeout_s,
    )
    if not parts:
        return [
            Finding(
                "warning",
                "no_partitions",
                f"No index partitions to audit for ticks_kind={tk}",
                extra={"ticks_kind": tk, "dataset_version": dv},
            )
        ]

    tbl = str(ticks_table).strip()
    findings: list[Finding] = []
    sql = (
        "SELECT "
        "count() AS rows_total, "
        "count(DISTINCT datetime_ns) AS distinct_dt, "
        "sum(CASE WHEN datetime_ns <= 0 THEN 1 ELSE 0 END) AS bad_datetime, "
        "sum(CASE WHEN volume < 0 THEN 1 ELSE 0 END) AS bad_volume, "
        "sum(CASE WHEN open_interest < 0 THEN 1 ELSE 0 END) AS bad_open_interest, "
        "sum(CASE WHEN last_price < 0 OR bid_price1 < 0 OR ask_price1 < 0 THEN 1 ELSE 0 END) AS bad_prices, "
        "sum(CASE WHEN ask_price1 > 0 AND bid_price1 > 0 AND ask_price1 < bid_price1 THEN 1 ELSE 0 END) AS crossed "
        f"FROM {tbl} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s"
    )

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            for sym, td in parts:
                cur.execute(sql, (sym, td, tk, dv))
                r = cur.fetchone()
                if not r:
                    continue
                rows_total = int(r[0] or 0)
                distinct_dt = int(r[1] or 0)
                bad_datetime = int(r[2] or 0)
                bad_volume = int(r[3] or 0)
                bad_oi = int(r[4] or 0)
                bad_prices = int(r[5] or 0)
                crossed = int(r[6] or 0)

                if rows_total <= 0:
                    continue
                dup = max(0, rows_total - distinct_dt)
                dup_rate = float(dup) / float(rows_total) if rows_total > 0 else 0.0

                if dup > 0:
                    findings.append(
                        Finding(
                            "warning",
                            "duplicate_datetime_ns",
                            "Duplicate datetime_ns rows detected (sampled)",
                            extra={
                                "symbol": sym,
                                "trading_day": td,
                                "ticks_kind": tk,
                                "dataset_version": dv,
                                "duplicates": int(dup),
                                "rows_total": int(rows_total),
                                "dup_rate": dup_rate,
                            },
                        )
                    )
                if bad_datetime > 0:
                    findings.append(
                        Finding(
                            "error",
                            "bad_datetime_ns",
                            "Invalid datetime_ns values detected (<=0)",
                            extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv, "bad_rows": int(bad_datetime)},
                        )
                    )
                if bad_volume > 0 or bad_oi > 0 or bad_prices > 0 or crossed > 0:
                    findings.append(
                        Finding(
                            "error",
                            "tick_sanity_failed",
                            "Tick sanity checks failed (sampled)",
                            extra={
                                "symbol": sym,
                                "trading_day": td,
                                "ticks_kind": tk,
                                "dataset_version": dv,
                                "bad_volume_rows": int(bad_volume),
                                "bad_open_interest_rows": int(bad_oi),
                                "bad_price_rows": int(bad_prices),
                                "crossed_book_rows": int(crossed),
                            },
                        )
                    )

    return findings


def audit_main_l5_equivalence_sampled(
    *,
    raw_ticks_table: str,
    main_l5_table: str,
    dataset_version: str = "v2",
    index_table: str = "ghtrader_symbol_day_index_v2",
    symbols: list[str] | None = None,
    sample_partitions: int = 20,
    connect_timeout_s: int = 2,
) -> list[Finding]:
    """
    PRD-required derived-vs-raw equivalence checks for main_l5 (sampled partitions).
    """
    from ghtrader.questdb.client import connect_pg
    from ghtrader.data.ticks_schema import TICK_NUMERIC_COLUMNS

    cfg = _qdb_cfg()
    ds_v = str(dataset_version).lower().strip() or "v2"
    parts = _sample_index_partitions(
        cfg=cfg,
        ticks_kind="main_l5",
        dataset_version=ds_v,
        index_table=index_table,
        symbols=symbols,
        limit=int(sample_partitions),
        connect_timeout_s=connect_timeout_s,
    )
    if not parts:
        return [
            Finding(
                "warning",
                "no_partitions",
                "No main_l5 partitions to audit",
                extra={"ticks_kind": "main_l5", "dataset_version": ds_v},
            )
        ]

    raw_tbl = str(raw_ticks_table).strip()
    main_tbl = str(main_l5_table).strip()
    cols = [c for c in TICK_NUMERIC_COLUMNS if c in {"last_price", "volume", "open_interest", "bid_price1", "ask_price1", "bid_volume1", "ask_volume1"}] or [
        "last_price",
        "volume",
        "open_interest",
        "bid_price1",
        "ask_price1",
        "bid_volume1",
        "ask_volume1",
    ]
    select_cols = ", ".join(["datetime_ns", "row_hash"] + cols)

    findings: list[Finding] = []
    sql_meta = (
        f"SELECT underlying_contract, schedule_hash, segment_id "
        f"FROM {main_tbl} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind='main_l5' AND dataset_version=%s "
        "LIMIT 1"
    )
    sql_rows = (
        f"SELECT {select_cols} "
        f"FROM {{tbl}} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s "
        "ORDER BY datetime_ns "
        "LIMIT 200"
    )

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            for derived_sym, td in parts:
                cur.execute(sql_meta, (derived_sym, td, dv))
                m = cur.fetchone()
                if not m:
                    continue
                underlying = str(m[0] or "").strip()
                schedule_hash = str(m[1] or "").strip()
                segment_id = m[2]
                if not underlying or not schedule_hash:
                    findings.append(
                        Finding(
                            "error",
                            "main_l5_missing_provenance",
                            "main_l5 row missing provenance fields",
                            extra={"symbol": derived_sym, "trading_day": td, "underlying_contract": underlying, "schedule_hash": schedule_hash},
                        )
                    )
                    continue

                # Aggregate equivalence:
                # - Hard requirements: row count + datetime bounds must match.
                # - Row-hash aggregates are best-effort (they may differ if historical data used
                #   a different row_hash definition; the value-column spot-check below is authoritative).
                a_der = _compute_partition_aggregates_from_ticks_table(
                    cfg=cfg,
                    ticks_table=main_tbl,
                    symbol=derived_sym,
                    trading_day=td,
                    ticks_kind="main_l5",
                    dataset_version=ds_v,
                    connect_timeout_s=connect_timeout_s,
                )
                a_raw = _compute_partition_aggregates_from_ticks_table(
                    cfg=cfg,
                    ticks_table=raw_tbl,
                    symbol=underlying,
                    trading_day=td,
                    ticks_kind="raw",
                    dataset_version=ds_v,
                    connect_timeout_s=connect_timeout_s,
                )
                if a_der is None or a_raw is None:
                    continue

                for k in ["rows_total", "first_datetime_ns", "last_datetime_ns"]:
                    der_v = a_der.get(k)
                    raw_v = a_raw.get(k)
                    if der_v is None or raw_v is None:
                        continue
                    if int(der_v) != int(raw_v):
                        findings.append(
                            Finding(
                                "error",
                                "main_l5_equivalence_failed",
                                "main_l5 count/bounds do not match underlying raw day",
                                extra={
                                    "symbol": derived_sym,
                                    "trading_day": td,
                                    "underlying_contract": underlying,
                                    "field": k,
                                    "main_l5": der_v,
                                    "raw": raw_v,
                                    "schedule_hash": schedule_hash,
                                    "segment_id": (int(segment_id) if segment_id is not None else None),
                                },
                            )
                        )
                        break

                for k in ["row_hash_min", "row_hash_max", "row_hash_sum", "row_hash_sum_abs"]:
                    der_v = a_der.get(k)
                    raw_v = a_raw.get(k)
                    if der_v is None or raw_v is None:
                        continue
                    if int(der_v) != int(raw_v):
                        findings.append(
                            Finding(
                                "warning",
                                "main_l5_row_hash_mismatch",
                                "main_l5 row_hash aggregates differ from raw (best-effort)",
                                extra={
                                    "symbol": derived_sym,
                                    "trading_day": td,
                                    "underlying_contract": underlying,
                                    "field": k,
                                    "main_l5": der_v,
                                    "raw": raw_v,
                                },
                            )
                        )

                # Spot-check a small sample of tick value columns.
                cur.execute(sql_rows.format(tbl=main_tbl), (derived_sym, td, "main_l5", ds_v))
                der_rows = cur.fetchall() or []
                cur.execute(sql_rows.format(tbl=raw_tbl), (underlying, td, "raw", ds_v))
                raw_rows = cur.fetchall() or []
                if not der_rows or not raw_rows:
                    continue
                # Compare in time order (ORDER BY datetime_ns). This avoids relying on row_hash parity.
                n = min(len(der_rows), len(raw_rows), 100)
                mismatched = 0
                for i in range(n):
                    dv = der_rows[i][2:]
                    rv = raw_rows[i][2:]
                    if len(dv) != len(rv):
                        mismatched += 1
                        break
                    for a, b in zip(dv, rv):
                        # Treat NaN==NaN as equal and allow small float error.
                        try:
                            fa = float(a) if a is not None else float("nan")
                            fb = float(b) if b is not None else float("nan")
                            if fa != fa and fb != fb:
                                continue
                            if abs(fa - fb) > 1e-9:
                                mismatched += 1
                                break
                        except Exception:
                            if str(a) != str(b):
                                mismatched += 1
                                break
                    if mismatched > 0:
                        break
                if mismatched > 0:
                    findings.append(
                        Finding(
                            "error",
                            "main_l5_equivalence_failed",
                            "main_l5 tick value columns mismatch raw (sampled rows)",
                            extra={"symbol": derived_sym, "trading_day": td, "underlying_contract": underlying, "columns_checked": cols},
                        )
                    )

    return findings


def _fetch_index_rows(
    *,
    cfg,
    symbols: list[str],
    ticks_kind: str,
    dataset_version: str,
    index_table: str,
    connect_timeout_s: int = 2,
) -> dict[tuple[str, str], dict[str, Any]]:
    from ghtrader.questdb.client import connect_pg

    idx = str(index_table).strip()
    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip()
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
    sql_base = f"SELECT {', '.join(cols)} FROM {idx} WHERE symbol IN ({{ph}}) AND ticks_kind=%s AND dataset_version=%s"
    for batch in _chunks(syms, 500):
        ph = ", ".join(["%s"] * len(batch))
        sql = sql_base.format(ph=ph)
        params: list[Any] = list(batch) + [tk, dv]
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
    ticks_kind: str,
    dataset_version: str,
    connect_timeout_s: int = 2,
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Compute per-(symbol,trading_day) aggregates from the canonical ticks table.

    Returns a dict keyed by (symbol, trading_day ISO) with:
      rows_total, first_datetime_ns, last_datetime_ns, l5_present,
      row_hash_min/max/sum/sum_abs (best-effort; may be None if `row_hash` absent).
    """
    from ghtrader.questdb.client import connect_pg

    tbl = str(ticks_table).strip()
    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip()
    syms = [str(s).strip() for s in symbols if str(s).strip()]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    if not tbl or not syms:
        return out

    l5_i = f"max(CASE WHEN {_l5_condition_sql()} THEN 1 ELSE 0 END) AS l5_present_i"
    for batch in _chunks(syms, 200):
        placeholders = ", ".join(["%s"] * len(batch))
        where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s"]
        params: list[Any] = list(batch) + [tk, dv]

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
    ticks_kind: str,
    dataset_version: str = "v2",
    symbols: list[str] | None = None,
    index_table: str = "ghtrader_symbol_day_index_v2",
    connect_timeout_s: int = 2,
) -> list[Finding]:
    """
    Verify that `ghtrader_symbol_day_index_v2` matches aggregates computed from the canonical ticks table.
    """
    from ghtrader.questdb.index import ensure_index_tables, list_symbols_from_index

    cfg = _qdb_cfg()
    ensure_index_tables(cfg=cfg, index_table=index_table, connect_timeout_s=connect_timeout_s)

    tk = str(ticks_kind).lower().strip()
    dv = str(dataset_version).lower().strip() or "v2"

    if symbols:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
    else:
        # Prefer index-backed symbol listing; if index is empty, we cannot audit without an explicit symbol list.
        syms = list_symbols_from_index(cfg=cfg, dataset_version=dv, ticks_kind=tk, index_table=index_table, limit=200000)

    if not syms:
        return [Finding("warning", "no_symbols", f"No symbols to audit for ticks_kind={tk}", extra={"ticks_kind": tk, "dataset_version": dv})]

    computed = _compute_day_aggregates_from_ticks_table(
        cfg=cfg,
        ticks_table=ticks_table,
        symbols=syms,
        ticks_kind=tk,
        dataset_version=dv,
        connect_timeout_s=connect_timeout_s,
    )

    index_rows = _fetch_index_rows(
        cfg=cfg,
        symbols=syms,
        ticks_kind=tk,
        dataset_version=dv,
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
                    extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv, "computed": c},
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
                        extra={
                            "symbol": sym,
                            "trading_day": td,
                            "ticks_kind": tk,
                            "dataset_version": dv,
                            "field": k,
                            "index": i.get(k),
                            "computed": c.get(k),
                        },
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
                        extra={
                            "symbol": sym,
                            "trading_day": td,
                            "ticks_kind": tk,
                            "dataset_version": dv,
                            "field": k,
                            "index": int(iv),
                            "computed": int(cv),
                        },
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
                            extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv},
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
                    extra={"symbol": sym, "trading_day": td, "ticks_kind": tk, "dataset_version": dv, "index": i},
                )
            )

    return findings


def audit_completeness(
    *,
    data_dir: Path,
    runs_dir: Path,
    dataset_version: str = "v2",
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
    from ghtrader.questdb.index import INDEX_TABLE_V2, list_no_data_trading_days, query_contract_coverage_from_index
    from ghtrader.tq.catalog import get_contract_catalog
    from ghtrader.data.trading_calendar import get_trading_calendar, get_trading_days

    _ = dataset_version  # v2-only

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
    cov = query_contract_coverage_from_index(cfg=cfg, symbols=sorted(target_syms), dataset_version="v2", ticks_kind="raw", index_table=INDEX_TABLE_V2)

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
                    dataset_version="v2",
                    ticks_kind="raw",
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
    dataset_version: str = "v2",
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

    # PRD-required schema conformity (best-effort; includes core tick + index tables).
    if any(s in scopes for s in ["all", "ticks", "main_l5", "completeness"]):
        try:
            findings.extend(audit_schema(connect_timeout_s=2))
        except Exception as e:
            findings.append(Finding("warning", "schema_check_failed", "Schema check failed", extra={"error": str(e)}))

    if "ticks" in scopes or "all" in scopes:
        findings.extend(
            audit_ticks_index_vs_table_sampled(
                ticks_table=ticks_raw_table,
                ticks_kind="raw",
                dataset_version=str(dataset_version),
                symbols=only,
            )
        )
        findings.extend(
            audit_ticks_quality_sampled(
                ticks_table=ticks_raw_table,
                ticks_kind="raw",
                dataset_version=str(dataset_version),
                symbols=only,
            )
        )

    if "main_l5" in scopes or "all" in scopes:
        findings.extend(
            audit_ticks_index_vs_table_sampled(
                ticks_table=ticks_main_l5_table,
                ticks_kind="main_l5",
                dataset_version=str(dataset_version),
                symbols=only,
            )
        )
        findings.extend(
            audit_ticks_quality_sampled(
                ticks_table=ticks_main_l5_table,
                ticks_kind="main_l5",
                dataset_version=str(dataset_version),
                symbols=only,
            )
        )
        findings.extend(
            audit_main_l5_equivalence_sampled(
                raw_ticks_table=ticks_raw_table,
                main_l5_table=ticks_main_l5_table,
                dataset_version=str(dataset_version),
                symbols=only,
            )
        )

    # Feature/label audits are intentionally lightweight (QuestDB-only).
    if "features" in scopes or "all" in scopes:
        try:
            from ghtrader.questdb.client import connect_pg
            from ghtrader.questdb.features_labels import FEATURE_BUILDS_TABLE_V2

            cfg = _qdb_cfg()
            with connect_pg(cfg, connect_timeout_s=2) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT count() FROM {FEATURE_BUILDS_TABLE_V2}")
                    _ = cur.fetchone()
        except Exception as e:
            findings.append(Finding("warning", "features_check_failed", "Features table check failed", extra={"error": str(e)}))

    if "labels" in scopes or "all" in scopes:
        try:
            from ghtrader.questdb.client import connect_pg
            from ghtrader.questdb.features_labels import LABEL_BUILDS_TABLE_V2

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
                dataset_version=str(dataset_version),
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
        "dataset_version": str(dataset_version),
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

