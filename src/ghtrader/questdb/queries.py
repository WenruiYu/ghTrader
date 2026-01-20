from __future__ import annotations

from datetime import datetime, timezone
import os
import threading
import time
from typing import Any, Literal

import re
import structlog

log = structlog.get_logger()

_thread_local = threading.local()


def _clear_thread_conn() -> None:
    try:
        conn = getattr(_thread_local, "questdb_conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        _thread_local.questdb_conn = None
        _thread_local.questdb_conn_cfg = None
    except Exception:
        pass


def _get_thread_conn(*, cfg: QuestDBQueryConfig, connect_timeout_s: int) -> Any:
    try:
        conn = getattr(_thread_local, "questdb_conn", None)
        conn_cfg = getattr(_thread_local, "questdb_conn_cfg", None)
        if conn is not None and conn_cfg == cfg:
            try:
                if getattr(conn, "closed", False):
                    _clear_thread_conn()
                else:
                    return conn
            except Exception:
                _clear_thread_conn()
        conn = _connect_safe(cfg, connect_timeout_s=connect_timeout_s, retries=2, backoff_s=0.2, autocommit=True)
        _thread_local.questdb_conn = conn
        _thread_local.questdb_conn_cfg = cfg
        return conn
    except Exception:
        _clear_thread_conn()
        raise


from .client import QuestDBQueryConfig, connect_pg as _connect, connect_pg_safe as _connect_safe, is_transient_pg_error


_READ_ONLY_START_RE = re.compile(r"^\s*(with|select)\b", flags=re.IGNORECASE)
_DISALLOWED_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|create|alter|truncate|grant|revoke|copy|vacuum|analyze|attach|detach)\b",
    flags=re.IGNORECASE,
)


def _ns_to_iso(ns: Any) -> str | None:
    try:
        n = int(ns)
        if n <= 0:
            return None
        return datetime.fromtimestamp(float(n) / 1_000_000_000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def sanitize_read_only_sql(query: str) -> str:
    """
    Best-effort guardrail for the dashboard SQL explorer.

    Allows only single-statement SELECT/WITH queries.
    """
    q = str(query or "").strip()
    if not q:
        raise ValueError("Query is required")

    # Disallow multi-statement queries. Allow at most one trailing semicolon.
    if ";" in q:
        q2 = q.rstrip()
        if q2.endswith(";"):
            q2 = q2[:-1].rstrip()
        if ";" in q2:
            raise ValueError("Only one SQL statement is allowed")
        q = q2

    if not _READ_ONLY_START_RE.match(q):
        raise ValueError("Only SELECT/WITH queries are allowed")

    if _DISALLOWED_SQL_RE.search(q):
        raise ValueError("Disallowed keyword in query (read-only mode)")

    return q


def query_sql_read_only(
    *,
    cfg: QuestDBQueryConfig,
    query: str,
    limit: int = 200,
    connect_timeout_s: int = 2,
) -> tuple[list[str], list[dict[str, str]]]:
    """
    Run a read-only SQL query against QuestDB via PGWire.

    Returns: (columns, rows) where rows is a list of stringified dict records.

    Notes:
    - Enforces a row cap best-effort by wrapping the query with a LIMIT.
    - If wrapping fails (e.g., unsupported nested WITH), falls back to fetching at most `limit` rows client-side.
    """
    q = sanitize_read_only_sql(query=query)
    lim = max(1, min(int(limit or 200), 500))

    cols: list[str] = []
    rows_out: list[dict[str, str]] = []

    def _stringify(v: Any) -> str:
        if v is None:
            return ""
        try:
            return str(v)
        except Exception:
            return ""

    # Best-effort: execute a wrapped query to enforce LIMIT in the engine.
    wrapped = f"SELECT * FROM ({q}) LIMIT {lim}"
    with _connect(cfg, connect_timeout_s=int(connect_timeout_s)) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(wrapped)
            except Exception:
                # Fallback: execute user query as-is and fetch at most `lim`.
                cur.execute(q)
            desc = cur.description or []
            cols = [str(d.name) for d in desc if getattr(d, "name", None)]
            fetched = cur.fetchmany(size=lim)
            for r in fetched:
                try:
                    row = {c: _stringify(v) for c, v in zip(cols, r)}
                except Exception:
                    row = {}
                rows_out.append(row)

    return cols, rows_out


def _l5_condition_sql() -> str:
    """
    Return SQL WHERE clause fragment for L5 detection.

    Note: This is a thin wrapper around the unified l5_detection module.
    """
    from ghtrader.util.l5_detection import l5_sql_condition

    return l5_sql_condition()


def query_symbol_day_bounds(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "main_l5",
    l5_only: bool = False,
) -> dict[str, dict[str, Any]]:
    """
    Return {symbol: {first_day, last_day, n_days, first_ns, last_ns, first_ts, last_ts}} using QuestDB canonical ticks.

    Notes:
    - Uses `trading_day` column (ISO YYYY-MM-DD strings, stored as SYMBOL).
    - Uses `cast(trading_day as string)` for compatibility with min/max.
    - `l5_only=True` restricts to rows that appear to have true L5 values.
    """
    if not symbols:
        return {}

    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    t0 = time.time()
    try:
        log.debug(
            "questdb.query_symbol_day_bounds.start",
            table=str(table),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            l5_only=bool(l5_only),
            n_symbols=int(len(symbols)),
        )
    except Exception:
        pass

    # Safe placeholders for IN (...)
    placeholders = ", ".join(["%s"] * len(symbols))
    where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s"]
    params: list[Any] = list(symbols) + [tk, dv]
    if l5_only:
        where.append(_l5_condition_sql())

    sql = (
        "SELECT symbol, "
        "min(cast(trading_day as string)) AS first_day, "
        "max(cast(trading_day as string)) AS last_day, "
        "count(DISTINCT cast(trading_day as string)) AS n_days, "
        "min(datetime_ns) AS first_ns, "
        "max(datetime_ns) AS last_ns "
        f"FROM {table} "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY symbol"
    )

    out: dict[str, dict[str, Any]] = {}

    with _connect(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, params)
            except Exception as e:
                raise
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    first_ns = row[4]
                    last_ns = row[5]
                    out[sym] = {
                        "first_day": row[1],
                        "last_day": row[2],
                        "n_days": row[3],
                        "first_ns": int(first_ns) if first_ns is not None else None,
                        "last_ns": int(last_ns) if last_ns is not None else None,
                        "first_ts": _ns_to_iso(first_ns),
                        "last_ts": _ns_to_iso(last_ns),
                    }
                except Exception:
                    continue
    try:
        log.debug(
            "questdb.query_symbol_day_bounds.done",
            table=str(table),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            l5_only=bool(l5_only),
            n_symbols=int(len(symbols)),
            n_rows=int(len(out)),
            ms=int((time.time() - t0) * 1000),
        )
    except Exception:
        pass
    return out


def list_trading_days_for_symbol(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbol: str,
    start_day: date,
    end_day: date,
    dataset_version: str,
    ticks_kind: str = "main_l5",
) -> list[date]:
    """
    List distinct trading_day values for a symbol from the canonical ticks table.
    """
    sym = str(symbol or "").strip()
    if not sym:
        return []
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    d0 = start_day.isoformat()
    d1 = end_day.isoformat()
    tbl = str(table).strip()

    sql = (
        "SELECT DISTINCT cast(trading_day as string) AS trading_day "
        f"FROM {tbl} "
        "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s "
        "AND cast(trading_day as string) >= %s AND cast(trading_day as string) <= %s "
        "ORDER BY trading_day ASC"
    )
    out: list[date] = []
    with _connect(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [sym, tk, dv, d0, d1])
            for (td,) in cur.fetchall():
                try:
                    out.append(date.fromisoformat(str(td)))
                except Exception:
                    continue
    return out


def query_symbol_latest(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "main_l5",
) -> dict[str, dict[str, Any]]:
    """
    Return {symbol: {last_day, last_ns, last_ts}} using QuestDB's optimized LATEST query.

    This is intended for fast Contracts/monitoring views where only freshness is needed.
    """
    if not symbols:
        return {}

    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()

    # Safe placeholders for IN (...)
    placeholders = ", ".join(["%s"] * len(symbols))
    where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s"]
    params: list[Any] = list(symbols) + [tk, dv]

    sql_latest_on = (
        "SELECT symbol, cast(trading_day as string) AS last_day, datetime_ns AS last_ns "
        f"FROM {table} "
        f"WHERE {' AND '.join(where)} "
        "LATEST ON ts PARTITION BY symbol"
    )

    t0 = time.time()
    try:
        log.debug(
            "questdb.query_symbol_latest.start",
            table=str(table),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            n_symbols=int(len(symbols)),
        )
    except Exception:
        pass

    out: dict[str, dict[str, Any]] = {}
    with _connect(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_latest_on, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    last_ns = row[2]
                    out[sym] = {
                        "last_day": row[1],
                        "last_ns": int(last_ns) if last_ns is not None else None,
                        "last_ts": _ns_to_iso(last_ns),
                    }
                except Exception:
                    continue

    try:
        log.debug(
            "questdb.query_symbol_latest.done",
            table=str(table),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            n_symbols=int(len(symbols)),
            n_rows=int(len(out)),
            ms=int((time.time() - t0) * 1000),
        )
    except Exception:
        pass
    return out


def fetch_ticks_for_symbol_day(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbol: str,
    trading_day: str,
    dataset_version: str,
    ticks_kind: str = "main_l5",
    limit: int | None = None,
    order: Literal["asc", "desc"] = "asc",
    include_provenance: bool = False,
    connect_timeout_s: int = 2,
    retries: int = 2,
    backoff_s: float = 0.2,
    reuse_conn: bool = True,
) -> "pd.DataFrame":
    """
    Fetch ticks for a single symbol+trading_day from QuestDB into a pandas DataFrame.

    Returns a DataFrame with canonical tick columns + `row_hash`.

    If `include_provenance=True`, also includes best-effort provenance columns when present:
    - `underlying_contract` (str)
    - `segment_id` (int)
    - `schedule_hash` (str)

    Notes:
    - Older tables may not have `row_hash` or provenance columns; this function is best-effort and will
      fall back to a reduced column set.
      - `symbol` (str)
      - `datetime` (int64 nanoseconds)
      - all numeric tick columns
      - `row_hash` (int64, if present in the table; otherwise set to 0; callers may recompute)
    """
    import pandas as pd

    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

    tbl = str(table).strip()
    sym = str(symbol).strip()
    td = str(trading_day).strip()
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    if not tbl or not sym or not td:
        return pd.DataFrame(columns=list(TICK_COLUMN_NAMES) + ["row_hash"])

    tick_numeric_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
    prov_cols = ["underlying_contract", "segment_id", "schedule_hash"] if bool(include_provenance) else []

    def _run_query(*, include_row_hash: bool, include_prov: bool) -> pd.DataFrame:
        sel = ["datetime_ns AS datetime"]
        if include_row_hash:
            sel.append("row_hash")
        sel += tick_numeric_cols
        if include_prov:
            sel += prov_cols
        cols_sql = ", ".join(sel)

        ord_sql = "ASC" if str(order).lower().strip() != "desc" else "DESC"
        base_sql = (
            f"SELECT {cols_sql} FROM {tbl} "
            "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s AND trading_day=%s "
            f"ORDER BY datetime {ord_sql}"
        )
        q_params: list[Any] = [sym, tk, dv, td]
        q_sql = base_sql
        if lim is not None:
            q_sql = base_sql + " LIMIT %s"
            q_params.append(int(lim))
        rows: list[Any] = []

        def _fetch_rows() -> list[Any]:
            if reuse_conn:
                conn = _get_thread_conn(cfg=cfg, connect_timeout_s=connect_timeout_s)
                try:
                    with conn.cursor() as cur:
                        cur.execute(q_sql, q_params)
                        return cur.fetchall()
                except Exception:
                    _clear_thread_conn()
                    raise
            with _connect_safe(cfg, connect_timeout_s=connect_timeout_s, retries=retries, backoff_s=backoff_s, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(q_sql, q_params)
                    return cur.fetchall()

        last_err: Exception | None = None
        for attempt in range(int(retries) + 1):
            try:
                rows = _fetch_rows()
                break
            except Exception as e:
                last_err = e
                if attempt >= int(retries) or not is_transient_pg_error(e):
                    raise
                try:
                    time.sleep(float(backoff_s) * (2 ** attempt))
                except Exception:
                    pass
        if rows is None and last_err:
            raise last_err
        if not rows:
            cols = list(TICK_COLUMN_NAMES) + ["row_hash"] + (prov_cols if include_prov else [])
            return pd.DataFrame(columns=cols)

        out_cols = ["datetime"]
        if include_row_hash:
            out_cols.append("row_hash")
        out_cols += tick_numeric_cols
        if include_prov:
            out_cols += prov_cols
        df0 = pd.DataFrame(rows, columns=out_cols)
        df0.insert(0, "symbol", sym)
        return df0

    _ = "ASC" if str(order).lower().strip() != "desc" else "DESC"
    lim = int(limit) if limit is not None else None
    if lim is not None:
        lim = max(1, min(lim, 2_000_000))

    try:
        df = _run_query(include_row_hash=True, include_prov=bool(include_provenance))
        # Ensure dtypes are sane for downstream compute.
        df["datetime"] = pd.to_numeric(df["datetime"], errors="coerce").fillna(0).astype("int64")
        df["row_hash"] = pd.to_numeric(df.get("row_hash"), errors="coerce").fillna(0).astype("int64")
        for c in tick_numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        if "segment_id" in df.columns:
            df["segment_id"] = pd.to_numeric(df["segment_id"], errors="coerce").fillna(0).astype("int64")
        extra = [c for c in prov_cols if c in df.columns] if include_provenance else []
        return df[TICK_COLUMN_NAMES + ["row_hash"] + extra]
    except Exception:
        # Backward compat: older tick tables may not have `row_hash` and/or provenance columns.
        try:
            df2 = _run_query(include_row_hash=False, include_prov=bool(include_provenance))
        except Exception:
            df2 = _run_query(include_row_hash=False, include_prov=False)
        df2["datetime"] = pd.to_numeric(df2["datetime"], errors="coerce").fillna(0).astype("int64")
        for c in tick_numeric_cols:
            if c in df2.columns:
                df2[c] = pd.to_numeric(df2[c], errors="coerce")
        if "segment_id" in df2.columns:
            df2["segment_id"] = pd.to_numeric(df2["segment_id"], errors="coerce").fillna(0).astype("int64")
        df2["row_hash"] = pd.Series([0] * len(df2), dtype="int64")
        extra2 = [c for c in prov_cols if c in df2.columns] if include_provenance else []
        return df2[TICK_COLUMN_NAMES + ["row_hash"] + extra2]


def query_symbol_recent_last(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str,
    trading_days: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Fast last-tick bounds limited to a recent set of trading_day partitions.

    Returns {symbol: {last_day, last_ns, last_ts}} for symbols that have any rows
    within the provided `trading_days` set.
    """
    if not symbols or not trading_days:
        return {}

    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()

    placeholders_syms = ", ".join(["%s"] * len(symbols))
    placeholders_days = ", ".join(["%s"] * len(trading_days))
    where = [
        f"symbol IN ({placeholders_syms})",
        "ticks_kind = %s",
        "dataset_version = %s",
        f"cast(trading_day as string) IN ({placeholders_days})",
    ]
    params: list[Any] = list(symbols) + [tk, dv] + [str(d).strip() for d in trading_days if str(d).strip()]

    sql = (
        "SELECT symbol, "
        "max(cast(trading_day as string)) AS last_day, "
        "max(datetime_ns) AS last_ns "
        f"FROM {table} "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY symbol"
    )

    t0 = time.time()
    try:
        log.debug(
            "questdb.query_symbol_recent_last.start",
            table=str(table),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            n_symbols=int(len(symbols)),
            n_days=int(len(trading_days)),
        )
    except Exception:
        pass

    out: dict[str, dict[str, Any]] = {}
    with _connect(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    last_ns = row[2]
                    out[sym] = {
                        "last_day": row[1],
                        "last_ns": int(last_ns) if last_ns is not None else None,
                        "last_ts": _ns_to_iso(last_ns),
                    }
                except Exception:
                    continue

    try:
        log.debug(
            "questdb.query_symbol_recent_last.done",
            table=str(table),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            n_symbols=int(len(symbols)),
            n_days=int(len(trading_days)),
            n_rows=int(len(out)),
            ms=int((time.time() - t0) * 1000),
        )
    except Exception:
        pass
    return out


def query_contract_last_coverage(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "main_l5",
    recent_days: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Fast per-symbol coverage for Contracts UI:
    - last_tick_day/ts (main_l5)
    - last_l5_day/ts (main_l5 when available)

    First-day and day-count fields are returned as None (can be merged from a cached full report).
    """
    if recent_days:
        base_last = query_symbol_recent_last(
            cfg=cfg,
            table=table,
            symbols=symbols,
            dataset_version=dataset_version,
            ticks_kind=ticks_kind,
            trading_days=list(recent_days),
        )
        l5_last = query_symbol_recent_last(
            cfg=cfg,
            table=table,
            symbols=symbols,
            dataset_version=dataset_version,
            ticks_kind="main_l5",
            trading_days=list(recent_days),
        )
    else:
        base_last = query_symbol_latest(cfg=cfg, table=table, symbols=symbols, dataset_version=dataset_version, ticks_kind=ticks_kind)
        l5_last = query_symbol_latest(cfg=cfg, table=table, symbols=symbols, dataset_version=dataset_version, ticks_kind="main_l5")

    out: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        b = base_last.get(sym) or {}
        l = l5_last.get(sym) or {}
        if not b and not l:
            continue
        out[sym] = {
            "first_tick_day": None,
            "last_tick_day": b.get("last_day"),
            "tick_days": None,
            "first_tick_ns": None,
            "last_tick_ns": b.get("last_ns"),
            "first_tick_ts": None,
            "last_tick_ts": b.get("last_ts"),
            "first_l5_day": None,
            "last_l5_day": l.get("last_day"),
            "l5_days": None,
            "first_l5_ns": None,
            "last_l5_ns": l.get("last_ns"),
            "first_l5_ts": None,
            "last_l5_ts": l.get("last_ts"),
        }
    return out


def query_contract_coverage(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "main_l5",
) -> dict[str, dict[str, Any]]:
    """
    Return per-symbol coverage:
      {
        symbol: {
          first_tick_day, last_tick_day,
          first_l5_day, last_l5_day
        }
      }
    """
    t0 = time.time()
    try:
        log.debug(
            "questdb.query_contract_coverage.start",
            table=str(table),
            dataset_version=str(dataset_version),
            ticks_kind=str(ticks_kind),
            n_symbols=int(len(symbols or [])),
        )
    except Exception:
        pass
    base = query_symbol_day_bounds(cfg=cfg, table=table, symbols=symbols, dataset_version=dataset_version, ticks_kind=ticks_kind, l5_only=False)

    # Prefer DB L5 coverage via the derived ticks dataset when available (much cheaper than scanning raw ticks).
    l5 = query_symbol_day_bounds(cfg=cfg, table=table, symbols=symbols, dataset_version=dataset_version, ticks_kind="main_l5", l5_only=False)
    if not l5:
        # Fallback for deployments that only ingest raw ticks into QuestDB.
        l5 = query_symbol_day_bounds(cfg=cfg, table=table, symbols=symbols, dataset_version=dataset_version, ticks_kind=ticks_kind, l5_only=True)

    out: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        b = base.get(sym) or {}
        l = l5.get(sym) or {}
        if not b and not l:
            continue
        out[sym] = {
            "first_tick_day": b.get("first_day"),
            "last_tick_day": b.get("last_day"),
            "tick_days": b.get("n_days"),
            "first_tick_ns": b.get("first_ns"),
            "last_tick_ns": b.get("last_ns"),
            "first_tick_ts": b.get("first_ts"),
            "last_tick_ts": b.get("last_ts"),
            "first_l5_day": l.get("first_day"),
            "last_l5_day": l.get("last_day"),
            "l5_days": l.get("n_days"),
            "first_l5_ns": l.get("first_ns"),
            "last_l5_ns": l.get("last_ns"),
            "first_l5_ts": l.get("first_ts"),
            "last_l5_ts": l.get("last_ts"),
        }
    try:
        log.debug(
            "questdb.query_contract_coverage.done",
            table=str(table),
            dataset_version=str(dataset_version),
            ticks_kind=str(ticks_kind),
            n_symbols=int(len(symbols or [])),
            n_rows=int(len(out)),
            ms=int((time.time() - t0) * 1000),
        )
    except Exception:
        pass
    return out

