from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import time
from typing import Any, Iterable

import structlog

from .client import QuestDBQueryConfig, connect_pg_safe as _connect
from ghtrader.util.worker_policy import resolve_worker_count

log = structlog.get_logger()


INDEX_TABLE_V2 = "ghtrader_symbol_day_index_v2"
NO_DATA_TABLE_V2 = "ghtrader_no_data_days_v2"


def _day_to_ts_utc(trading_day: str) -> datetime:
    """
    Map ISO YYYY-MM-DD trading_day to a stable UTC midnight timestamp for QuestDB TIMESTAMP(ts).
    """
    d = str(trading_day or "").strip()
    if not d:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
    except Exception:
        try:
            dd = date.fromisoformat(d)
            return datetime(dd.year, dd.month, dd.day, tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ns_to_iso(ns: Any) -> str | None:
    try:
        n = int(ns)
        if n <= 0:
            return None
        return datetime.fromtimestamp(float(n) / 1_000_000_000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _l5_condition_sql() -> str:
    """
    Return SQL WHERE clause fragment for L5 detection.

    Note: This is a thin wrapper around the unified l5_detection module.
    """
    from ghtrader.util.l5_detection import l5_sql_condition

    return l5_sql_condition()


def ensure_index_tables(
    *,
    cfg: QuestDBQueryConfig,
    index_table: str = INDEX_TABLE_V2,
    no_data_table: str = NO_DATA_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    """
    Best-effort DDL to ensure the QuestDB coverage/index tables exist.
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    nd = str(no_data_table).strip() or NO_DATA_TABLE_V2

    ddl_idx = f"""
    CREATE TABLE IF NOT EXISTS {idx} (
      ts TIMESTAMP,
      symbol SYMBOL,
      trading_day SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      rows_total LONG,
      first_datetime_ns LONG,
      last_datetime_ns LONG,
      l5_present BOOLEAN,
      row_hash_min LONG,
      row_hash_max LONG,
      row_hash_sum LONG,
      row_hash_sum_abs LONG,
      updated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)
    """

    ddl_nd = f"""
    CREATE TABLE IF NOT EXISTS {nd} (
      ts TIMESTAMP,
      symbol SYMBOL,
      trading_day SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      reason SYMBOL,
      created_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)
    """

    try:
        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            # DDL/schema evolution should be resilient to per-statement failures.
            # In psycopg, any statement error aborts the current transaction, which would
            # prevent subsequent ALTERs from running. Use autocommit so each DDL statement
            # runs in its own transaction.
            try:
                conn.autocommit = True  # type: ignore[attr-defined]
            except Exception:
                pass
            with conn.cursor() as cur:
                cur.execute(ddl_idx)
                cur.execute(ddl_nd)
                # Best-effort column rename migration (v2): tolerate older column names.
                from .migrate import best_effort_rename_provenance_columns_v2

                for tbl in [idx, nd]:
                    best_effort_rename_provenance_columns_v2(cur=cur, table=str(tbl))

                # Best-effort schema evolution.
                for name, typ in [
                    ("trading_day", "SYMBOL"),
                    ("ticks_kind", "SYMBOL"),
                    ("dataset_version", "SYMBOL"),
                    ("rows_total", "LONG"),
                    ("first_datetime_ns", "LONG"),
                    ("last_datetime_ns", "LONG"),
                    ("l5_present", "BOOLEAN"),
                    ("row_hash_min", "LONG"),
                    ("row_hash_max", "LONG"),
                    ("row_hash_sum", "LONG"),
                    ("row_hash_sum_abs", "LONG"),
                    ("updated_at", "TIMESTAMP"),
                ]:
                    try:
                        cur.execute(f"ALTER TABLE {idx} ADD COLUMN {name} {typ}")
                    except Exception:
                        pass

                for name, typ in [
                    ("trading_day", "SYMBOL"),
                    ("ticks_kind", "SYMBOL"),
                    ("dataset_version", "SYMBOL"),
                    ("reason", "SYMBOL"),
                    ("created_at", "TIMESTAMP"),
                ]:
                    try:
                        cur.execute(f"ALTER TABLE {nd} ADD COLUMN {name} {typ}")
                    except Exception:
                        pass

                # Best-effort: enable dedup on existing tables (no-op if already enabled).
                try:
                    cur.execute(f"ALTER TABLE {idx} DEDUP ENABLE UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)")
                except Exception:
                    pass
                try:
                    cur.execute(f"ALTER TABLE {nd} DEDUP ENABLE UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)")
                except Exception:
                    pass
    except Exception as e:
        log.warning("questdb_index.ensure_tables_failed", index_table=idx, no_data_table=nd, error=str(e))


@dataclass(frozen=True)
class SymbolDayIndexRow:
    symbol: str
    trading_day: str
    ticks_kind: str
    dataset_version: str
    rows_total: int
    first_datetime_ns: int | None
    last_datetime_ns: int | None
    l5_present: bool
    row_hash_min: int | None = None
    row_hash_max: int | None = None
    row_hash_sum: int | None = None
    row_hash_sum_abs: int | None = None


def upsert_symbol_day_index_rows(
    *,
    cfg: QuestDBQueryConfig,
    rows: Iterable[SymbolDayIndexRow],
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> int:
    """
    Upsert per-(symbol, trading_day, ticks_kind, dataset_version) coverage rows.
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    rs = list(rows)
    if not rs:
        return 0

    now = datetime.now(timezone.utc)
    params: list[tuple[Any, ...]] = []
    for r in rs:
        params.append(
            (
                _day_to_ts_utc(r.trading_day),
                str(r.symbol),
                str(r.trading_day),
                str(r.ticks_kind),
                str(r.dataset_version),
                int(r.rows_total),
                (int(r.first_datetime_ns) if r.first_datetime_ns is not None else None),
                (int(r.last_datetime_ns) if r.last_datetime_ns is not None else None),
                bool(r.l5_present),
                (int(r.row_hash_min) if r.row_hash_min is not None else None),
                (int(r.row_hash_max) if r.row_hash_max is not None else None),
                (int(r.row_hash_sum) if r.row_hash_sum is not None else None),
                (int(r.row_hash_sum_abs) if r.row_hash_sum_abs is not None else None),
                now,
            )
        )

    sql = (
        f"INSERT INTO {idx} "
        "(ts, symbol, trading_day, ticks_kind, dataset_version, rows_total, first_datetime_ns, last_datetime_ns, l5_present, "
        "row_hash_min, row_hash_max, row_hash_sum, row_hash_sum_abs, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )

    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
    return int(len(params))


def upsert_no_data_days(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    trading_days: Iterable[str],
    ticks_kind: str,
    dataset_version: str,
    reason: str = "tqsdk_no_ticks",
    no_data_table: str = NO_DATA_TABLE_V2,
    connect_timeout_s: int = 2,
) -> int:
    nd = str(no_data_table).strip() or NO_DATA_TABLE_V2
    days = [str(d).strip() for d in trading_days if str(d).strip()]
    if not symbol or not days:
        return 0
    now = datetime.now(timezone.utc)
    params: list[tuple[Any, ...]] = []
    for d in days:
        params.append((_day_to_ts_utc(d), str(symbol), str(d), str(ticks_kind), str(dataset_version), str(reason), now))
    sql = f"INSERT INTO {nd} (ts, symbol, trading_day, ticks_kind, dataset_version, reason, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
    return int(len(params))


def query_present_trading_days(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str,
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, set[str]]:
    """
    Return {symbol: {trading_day, ...}} - the set of trading days present in QuestDB for each symbol.

    This is essential for QuestDB-first status calculation (PRD Section 5.3).
    """
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    if not syms:
        return {}
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    idx = str(index_table).strip() or INDEX_TABLE_V2

    placeholders = ", ".join(["%s"] * len(syms))
    where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s", "rows_total > 0"]
    params: list[Any] = list(syms) + [tk, dv]

    sql = f"SELECT symbol, cast(trading_day as string) AS day FROM {idx} WHERE {' AND '.join(where)}"

    out: dict[str, set[str]] = {s: set() for s in syms}
    try:
        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    try:
                        sym = str(row[0]).strip()
                        day = str(row[1]).strip()
                        if sym and day and sym in out:
                            out[sym].add(day)
                    except Exception:
                        continue
    except Exception as e:
        log.warning("query_present_trading_days.error", error=str(e))
    return out


def query_symbol_day_index_hashes(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str,
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Return {(symbol, trading_day): {rows_total, row_hash_min/max/sum/sum_abs}} from the index table.
    """
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    if not syms:
        return {}
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    idx = str(index_table).strip() or INDEX_TABLE_V2

    placeholders = ", ".join(["%s"] * len(syms))
    where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s", "rows_total > 0"]
    params: list[Any] = list(syms) + [tk, dv]

    sql = (
        "SELECT symbol, cast(trading_day as string) AS trading_day, rows_total, "
        "row_hash_min, row_hash_max, row_hash_sum, row_hash_sum_abs "
        f"FROM {idx} WHERE {' AND '.join(where)}"
    )

    out: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    try:
                        sym = str(row[0]).strip()
                        day = str(row[1]).strip()
                        if not sym or not day:
                            continue
                        out[(sym, day)] = {
                            "rows_total": int(row[2]) if row[2] is not None else None,
                            "row_hash_min": int(row[3]) if row[3] is not None else None,
                            "row_hash_max": int(row[4]) if row[4] is not None else None,
                            "row_hash_sum": int(row[5]) if row[5] is not None else None,
                            "row_hash_sum_abs": int(row[6]) if row[6] is not None else None,
                        }
                    except Exception:
                        continue
    except Exception as e:
        log.warning("query_symbol_day_index_hashes.error", error=str(e))
    return out


def query_symbol_day_index_bounds(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str,
    index_table: str = INDEX_TABLE_V2,
    l5_only: bool = False,
    connect_timeout_s: int = 2,
) -> dict[str, dict[str, Any]]:
    """
    Return {symbol: {first_day, last_day, n_days, first_ns, last_ns, first_ts, last_ts}} from the index table.
    """
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    if not syms:
        return {}
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    idx = str(index_table).strip() or INDEX_TABLE_V2

    placeholders = ", ".join(["%s"] * len(syms))
    where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s", "rows_total > 0"]
    params: list[Any] = list(syms) + [tk, dv]
    if l5_only:
        where.append("l5_present = true")

    sql = (
        "SELECT symbol, "
        "min(cast(trading_day as string)) AS first_day, "
        "max(cast(trading_day as string)) AS last_day, "
        "count() AS n_days, "
        "min(first_datetime_ns) AS first_ns, "
        "max(last_datetime_ns) AS last_ns "
        f"FROM {idx} "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY symbol"
    )

    t0 = time.time()
    try:
        log.debug(
            "questdb_index.query_bounds.start",
            index_table=str(idx),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            l5_only=bool(l5_only),
            n_symbols=int(len(syms)),
        )
    except Exception:
        pass

    out: dict[str, dict[str, Any]] = {}
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    first_ns = row[4]
                    last_ns = row[5]
                    out[sym] = {
                        "first_day": row[1],
                        "last_day": row[2],
                        "n_days": int(row[3]) if row[3] is not None else None,
                        "first_ns": int(first_ns) if first_ns is not None else None,
                        "last_ns": int(last_ns) if last_ns is not None else None,
                        "first_ts": _ns_to_iso(first_ns),
                        "last_ts": _ns_to_iso(last_ns),
                    }
                except Exception:
                    continue

    try:
        log.debug(
            "questdb_index.query_bounds.done",
            index_table=str(idx),
            dataset_version=str(dv),
            ticks_kind=str(tk),
            l5_only=bool(l5_only),
            n_symbols=int(len(syms)),
            n_rows=int(len(out)),
            ms=int((time.time() - t0) * 1000),
        )
    except Exception:
        pass

    return out


def query_contract_coverage_from_index(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "raw",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, dict[str, Any]]:
    """
    Index-backed equivalent of `questdb_queries.query_contract_coverage` (no full tick scans).

    Returns dict per symbol with:
      - first_tick_day, last_tick_day, tick_days (bounds/count)
      - present_dates: set[str] of ISO trading days present in QuestDB (QuestDB-first per PRD)
      - L5 bounds if available
    """
    base = query_symbol_day_index_bounds(
        cfg=cfg,
        symbols=symbols,
        dataset_version=dataset_version,
        ticks_kind=ticks_kind,
        index_table=index_table,
        l5_only=False,
        connect_timeout_s=connect_timeout_s,
    )
    l5 = query_symbol_day_index_bounds(
        cfg=cfg,
        symbols=symbols,
        dataset_version=dataset_version,
        ticks_kind=ticks_kind,
        index_table=index_table,
        l5_only=True,
        connect_timeout_s=connect_timeout_s,
    )
    # Fetch actual trading days present (essential for QuestDB-first status calculation)
    present_days = query_present_trading_days(
        cfg=cfg,
        symbols=symbols,
        dataset_version=dataset_version,
        ticks_kind=ticks_kind,
        index_table=index_table,
        connect_timeout_s=connect_timeout_s,
    )

    out: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        b = base.get(sym) or {}
        ll = l5.get(sym) or {}
        pd = present_days.get(sym) or set()
        if not b and not ll and not pd:
            continue
        out[sym] = {
            "first_tick_day": b.get("first_day"),
            "last_tick_day": b.get("last_day"),
            "tick_days": b.get("n_days"),
            "present_dates": pd,  # QuestDB-first: set of ISO dates present
            "first_tick_ns": b.get("first_ns"),
            "last_tick_ns": b.get("last_ns"),
            "first_tick_ts": b.get("first_ts"),
            "last_tick_ts": b.get("last_ts"),
            "first_l5_day": ll.get("first_day"),
            "last_l5_day": ll.get("last_day"),
            "l5_days": ll.get("n_days"),
            "first_l5_ns": ll.get("first_ns"),
            "last_l5_ns": ll.get("last_ns"),
            "first_l5_ts": ll.get("first_ts"),
            "last_l5_ts": ll.get("last_ts"),
        }
    return out


def list_symbols_from_index(
    *,
    cfg: QuestDBQueryConfig,
    dataset_version: str,
    ticks_kind: str = "raw",
    prefix: str | None = None,
    index_table: str = INDEX_TABLE_V2,
    limit: int = 10000,
    connect_timeout_s: int = 2,
) -> list[str]:
    """
    List distinct symbols present in the index table (optionally filtered by a string prefix).
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    lim = max(1, min(int(limit or 10000), 200000))

    where = ["ticks_kind = %s", "dataset_version = %s", "rows_total > 0"]
    params: list[Any] = [tk, dv]
    if prefix:
        p = str(prefix).strip()
        if p:
            where.append("cast(symbol as string) like %s")
            params.append(p + "%")

    sql = f"SELECT DISTINCT symbol FROM {idx} WHERE {' AND '.join(where)} LIMIT {lim}"
    out: list[str] = []
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    s = str(row[0]).strip()
                    if s:
                        out.append(s)
                except Exception:
                    continue
    return sorted(set(out))


def query_index_coverage_rows(
    *,
    cfg: QuestDBQueryConfig,
    dataset_version: str,
    ticks_kind: str,
    limit: int = 200,
    search: str = "",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> list[dict[str, Any]]:
    """
    Return a small coverage listing from the index (for dashboard overview lists).
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    lim = max(1, min(int(limit or 200), 5000))
    q = str(search or "").strip()
    ql = q.lower()

    base_where = ["ticks_kind = %s", "dataset_version = %s", "rows_total > 0"]
    base_params: list[Any] = [tk, dv]

    def _run(*, with_search_sql: bool) -> list[tuple[Any, ...]]:
        where = list(base_where)
        params = list(base_params)
        if with_search_sql and ql:
            where.append("lower(cast(symbol as string)) like %s")
            params.append(f"%{ql}%")
        sql = (
            "SELECT symbol, count() AS n_days, "
            "min(cast(trading_day as string)) AS min_day, "
            "max(cast(trading_day as string)) AS max_day "
            f"FROM {idx} "
            f"WHERE {' AND '.join(where)} "
            "GROUP BY symbol "
            "ORDER BY symbol "
            f"LIMIT {lim}"
        )
        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

    rows: list[tuple[Any, ...]] = []
    if ql:
        try:
            rows = _run(with_search_sql=True)
        except Exception:
            # Fallback: do not push substring search into SQL; filter client-side.
            rows = _run(with_search_sql=False)
            rows = [r for r in rows if ql in str(r[0]).lower()]
    else:
        rows = _run(with_search_sql=False)

    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            out.append(
                {
                    "symbol": str(r[0]),
                    "n_days": int(r[1]) if r[1] is not None else 0,
                    "min_day": str(r[2] or ""),
                    "max_day": str(r[3] or ""),
                }
            )
        except Exception:
            continue
    return out


def list_present_trading_days(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    start_day: date,
    end_day: date,
    dataset_version: str,
    ticks_kind: str = "raw",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> set[date]:
    """
    List trading days present in the index for a symbol within a day-range (inclusive).
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    sym = str(symbol).strip()
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    if not sym:
        return set()
    s_ts = _day_to_ts_utc(start_day.isoformat())
    e_ts = _day_to_ts_utc(end_day.isoformat())

    sql = (
        f"SELECT cast(trading_day as string) FROM {idx} "
        "WHERE symbol = %s AND ticks_kind = %s AND dataset_version = %s AND rows_total > 0 "
        "AND ts >= %s AND ts <= %s"
    )
    out: set[date] = set()
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [sym, tk, dv, s_ts, e_ts])
            for row in cur.fetchall():
                try:
                    out.add(date.fromisoformat(str(row[0])))
                except Exception:
                    continue
    return out


def fetch_present_days_by_symbol(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    start_day: date,
    end_day: date,
    dataset_version: str,
    ticks_kind: str = "raw",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, set[date]]:
    """
    Fetch present trading days for many symbols in one query.
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    if not syms:
        return {}
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    s_ts = _day_to_ts_utc(start_day.isoformat())
    e_ts = _day_to_ts_utc(end_day.isoformat())

    placeholders = ", ".join(["%s"] * len(syms))
    sql = (
        f"SELECT symbol, cast(trading_day as string) "
        f"FROM {idx} "
        f"WHERE symbol IN ({placeholders}) AND ticks_kind=%s AND dataset_version=%s AND rows_total > 0 "
        "AND ts >= %s AND ts <= %s"
    )
    params: list[Any] = list(syms) + [tk, dv, s_ts, e_ts]

    out: dict[str, set[date]] = {}
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    d = date.fromisoformat(str(row[1]))
                except Exception:
                    continue
                out.setdefault(sym, set()).add(d)
    return out


def list_no_data_trading_days(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    start_day: date,
    end_day: date,
    dataset_version: str,
    ticks_kind: str = "raw",
    no_data_table: str = NO_DATA_TABLE_V2,
    connect_timeout_s: int = 2,
) -> set[date]:
    """
    List known-no-data trading days for a symbol within a day-range (inclusive).
    """
    nd = str(no_data_table).strip() or NO_DATA_TABLE_V2
    sym = str(symbol).strip()
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    if not sym:
        return set()
    s_ts = _day_to_ts_utc(start_day.isoformat())
    e_ts = _day_to_ts_utc(end_day.isoformat())

    sql = (
        f"SELECT cast(trading_day as string) FROM {nd} "
        "WHERE symbol = %s AND ticks_kind = %s AND dataset_version = %s "
        "AND ts >= %s AND ts <= %s"
    )
    out: set[date] = set()
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [sym, tk, dv, s_ts, e_ts])
            for row in cur.fetchall():
                try:
                    out.add(date.fromisoformat(str(row[0])))
                except Exception:
                    continue
    return out


def fetch_no_data_days_by_symbol(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    start_day: date,
    end_day: date,
    dataset_version: str,
    ticks_kind: str = "raw",
    no_data_table: str = NO_DATA_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, set[date]]:
    """
    Fetch known-no-data trading days for many symbols in one query.
    """
    nd = str(no_data_table).strip() or NO_DATA_TABLE_V2
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    if not syms:
        return {}
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    s_ts = _day_to_ts_utc(start_day.isoformat())
    e_ts = _day_to_ts_utc(end_day.isoformat())

    placeholders = ", ".join(["%s"] * len(syms))
    sql = (
        f"SELECT symbol, cast(trading_day as string) "
        f"FROM {nd} "
        f"WHERE symbol IN ({placeholders}) AND ticks_kind=%s AND dataset_version=%s "
        "AND ts >= %s AND ts <= %s"
    )
    params: list[Any] = list(syms) + [tk, dv, s_ts, e_ts]

    out: dict[str, set[date]] = {}
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    d = date.fromisoformat(str(row[1]))
                except Exception:
                    continue
                out.setdefault(sym, set()).add(d)
    return out


def get_symbol_day_index_row(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    trading_day: str,
    dataset_version: str,
    ticks_kind: str = "raw",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, Any] | None:
    """
    Fetch the current index row for a (symbol, trading_day, ticks_kind, dataset_version) key.
    """
    idx = str(index_table).strip() or INDEX_TABLE_V2
    sym = str(symbol).strip()
    td = str(trading_day).strip()
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    if not sym or not td:
        return None
    sql = (
        f"SELECT rows_total, first_datetime_ns, last_datetime_ns, l5_present, "
        "row_hash_min, row_hash_max, row_hash_sum, row_hash_sum_abs "
        f"FROM {idx} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s "
        "LIMIT 1"
    )
    sql_legacy = (
        f"SELECT rows_total, first_datetime_ns, last_datetime_ns, l5_present "
        f"FROM {idx} "
        "WHERE symbol=%s AND trading_day=%s AND ticks_kind=%s AND dataset_version=%s "
        "LIMIT 1"
    )
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            has_row_hash = True
            try:
                cur.execute(sql, [sym, td, tk, dv])
            except Exception:
                has_row_hash = False
                cur.execute(sql_legacy, [sym, td, tk, dv])
            row = cur.fetchone()
            if not row:
                return None
            out: dict[str, Any] = {
                "rows_total": int(row[0]) if row[0] is not None else None,
                "first_datetime_ns": int(row[1]) if row[1] is not None else None,
                "last_datetime_ns": int(row[2]) if row[2] is not None else None,
                "l5_present": bool(row[3]) if row[3] is not None else False,
            }
            if has_row_hash:
                out.update(
                    {
                        "row_hash_min": int(row[4]) if row[4] is not None else None,
                        "row_hash_max": int(row[5]) if row[5] is not None else None,
                        "row_hash_sum": int(row[6]) if row[6] is not None else None,
                        "row_hash_sum_abs": int(row[7]) if row[7] is not None else None,
                    }
                )
            return out


def bootstrap_symbol_day_index_from_ticks(
    *,
    cfg: QuestDBQueryConfig,
    ticks_table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "raw",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
    batch_symbols: int = 50,
) -> dict[str, Any]:
    """
    Build/refresh `ghtrader_symbol_day_index_v2` for the provided symbols by scanning the canonical ticks table.

    This is intended as a bootstrap tool for existing deployments that already have ticks in QuestDB
    but do not yet have the index populated.
    """
    tbl = str(ticks_table).strip()
    idx = str(index_table).strip() or INDEX_TABLE_V2
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()
    if not tbl or not syms:
        return {"ok": False, "error": "missing_table_or_symbols", "rows": 0}

    ensure_index_tables(cfg=cfg, index_table=idx, connect_timeout_s=connect_timeout_s)

    def _chunks(xs: list[str], n: int) -> Iterable[list[str]]:
        nn = max(1, int(n))
        for i in range(0, len(xs), nn):
            yield xs[i : i + nn]

    total_rows = 0
    t0 = time.time()
    for batch in _chunks(syms, batch_symbols):
        placeholders = ", ".join(["%s"] * len(batch))
        where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s"]
        params: list[Any] = list(batch) + [tk, dv]

        l5_i = f"max(CASE WHEN {_l5_condition_sql()} THEN 1 ELSE 0 END) AS l5_present_i"
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

        log.info("questdb_index.bootstrap.start", ticks_table=tbl, index_table=idx, n_symbols=int(len(batch)))
        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                has_row_hash = True
                try:
                    cur.execute(sql, params)
                except Exception:
                    has_row_hash = False
                    cur.execute(sql_legacy, params)

                rows_out: list[SymbolDayIndexRow] = []
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
                        rows_out.append(
                            SymbolDayIndexRow(
                                symbol=sym,
                                trading_day=td,
                                ticks_kind=tk,
                                dataset_version=dv,
                                rows_total=rows_total,
                                first_datetime_ns=first_ns,
                                last_datetime_ns=last_ns,
                                l5_present=l5_present,
                                row_hash_min=rh_min,
                                row_hash_max=rh_max,
                                row_hash_sum=rh_sum,
                                row_hash_sum_abs=rh_sum_abs,
                            )
                        )
                    except Exception:
                        continue

        n_up = upsert_symbol_day_index_rows(cfg=cfg, rows=rows_out, index_table=idx, connect_timeout_s=connect_timeout_s)
        total_rows += int(n_up)
        log.info("questdb_index.bootstrap.done", ticks_table=tbl, index_table=idx, n_symbols=int(len(batch)), upserted=int(n_up))

    return {"ok": True, "ticks_table": tbl, "index_table": idx, "rows": int(total_rows), "seconds": float(time.time() - t0)}


# ---------------------------------------------------------------------------
# Parallel index bootstrap (for high-core-count systems)
# ---------------------------------------------------------------------------


def _parallel_bootstrap_worker(args: tuple) -> dict[str, Any]:
    """
    Worker function for parallel bootstrap. Processes a batch of symbols.

    Args:
        args: Tuple of (cfg_dict, ticks_table, symbols, dataset_version, ticks_kind, index_table, connect_timeout_s)

    Returns:
        dict with rows_upserted and any errors
    """
    from .client import QuestDBQueryConfig

    cfg_dict, ticks_table, symbols, dataset_version, ticks_kind, index_table, connect_timeout_s = args

    cfg = QuestDBQueryConfig(**cfg_dict)
    tbl = str(ticks_table).strip()
    idx = str(index_table).strip() or INDEX_TABLE_V2
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()

    if not symbols:
        return {"rows_upserted": 0, "symbols": []}

    try:
        placeholders = ", ".join(["%s"] * len(symbols))
        where = [f"symbol IN ({placeholders})", "ticks_kind = %s", "dataset_version = %s"]
        params: list[Any] = list(symbols) + [tk, dv]

        l5_i = f"max(CASE WHEN {_l5_condition_sql()} THEN 1 ELSE 0 END) AS l5_present_i"
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

        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                has_row_hash = True
                try:
                    cur.execute(sql, params)
                except Exception:
                    has_row_hash = False
                    cur.execute(sql_legacy, params)

                rows_out: list[SymbolDayIndexRow] = []
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
                        rows_out.append(
                            SymbolDayIndexRow(
                                symbol=sym,
                                trading_day=td,
                                ticks_kind=tk,
                                dataset_version=dv,
                                rows_total=rows_total,
                                first_datetime_ns=first_ns,
                                last_datetime_ns=last_ns,
                                l5_present=l5_present,
                                row_hash_min=rh_min,
                                row_hash_max=rh_max,
                                row_hash_sum=rh_sum,
                                row_hash_sum_abs=rh_sum_abs,
                            )
                        )
                    except Exception:
                        continue

        n_up = upsert_symbol_day_index_rows(cfg=cfg, rows=rows_out, index_table=idx, connect_timeout_s=connect_timeout_s)
        return {"rows_upserted": int(n_up), "symbols": symbols}
    except Exception as e:
        return {"rows_upserted": 0, "symbols": symbols, "error": str(e)}


def bootstrap_symbol_day_index_parallel(
    *,
    cfg: QuestDBQueryConfig,
    ticks_table: str,
    symbols: list[str],
    dataset_version: str,
    ticks_kind: str = "raw",
    index_table: str = INDEX_TABLE_V2,
    connect_timeout_s: int = 2,
    batch_symbols: int = 50,
    n_workers: int | None = None,
) -> dict[str, Any]:
    """
    Parallel version of bootstrap_symbol_day_index_from_ticks.

    Uses multiprocessing to process multiple symbol batches concurrently.
    This is optimized for high-core-count systems (e.g., 128-256 cores).

    Args:
        cfg: QuestDB query configuration
        ticks_table: Source ticks table name
        symbols: List of symbols to process
        dataset_version: Dataset version (v2)
        ticks_kind: Ticks kind (raw/main_l5)
        index_table: Target index table name
        connect_timeout_s: Connection timeout in seconds
        batch_symbols: Symbols per batch (default 50)
        n_workers: Number of parallel workers (default: CPU count / 4)

    Returns:
        dict with ok, ticks_table, index_table, rows, seconds, workers_used
    """
    import multiprocessing
    import os
    from concurrent.futures import ProcessPoolExecutor, as_completed

    tbl = str(ticks_table).strip()
    idx = str(index_table).strip() or INDEX_TABLE_V2
    syms = [str(s).strip() for s in (symbols or []) if str(s).strip()]
    dv = str(dataset_version).lower().strip()
    tk = str(ticks_kind).lower().strip()

    if not tbl or not syms:
        return {"ok": False, "error": "missing_table_or_symbols", "rows": 0}

    ensure_index_tables(cfg=cfg, index_table=idx, connect_timeout_s=connect_timeout_s)

    # Determine worker count
    cpu_count = multiprocessing.cpu_count()
    n_workers = resolve_worker_count(kind="index_bootstrap", requested=n_workers, cpu_count=cpu_count)

    # Prepare batches
    batches: list[list[str]] = []
    env_batch = os.getenv("GHTRADER_INDEX_BOOTSTRAP_BATCH_SYMBOLS")
    if env_batch:
        try:
            batch_symbols = int(env_batch)
        except Exception:
            pass
    nn = max(1, int(batch_symbols))
    for i in range(0, len(syms), nn):
        batches.append(syms[i : i + nn])

    if not batches:
        return {"ok": True, "ticks_table": tbl, "index_table": idx, "rows": 0, "seconds": 0.0, "workers_used": 0}

    # Convert config to dict for pickling
    cfg_dict = {
        "host": cfg.host,
        "pg_port": cfg.pg_port,
        "pg_user": cfg.pg_user,
        "pg_password": cfg.pg_password,
        "pg_dbname": cfg.pg_dbname,
    }

    # Prepare worker args
    worker_args = [
        (cfg_dict, tbl, batch, dv, tk, idx, connect_timeout_s)
        for batch in batches
    ]

    total_rows = 0
    errors: list[str] = []
    t0 = time.time()

    log.info(
        "questdb_index.parallel_bootstrap.start",
        ticks_table=tbl,
        index_table=idx,
        n_symbols=len(syms),
        n_batches=len(batches),
        n_workers=n_workers,
    )

    # Use ProcessPoolExecutor for CPU-bound parallel work
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(_parallel_bootstrap_worker, args) for args in worker_args]
        for future in as_completed(futures):
            try:
                result = future.result()
                total_rows += result.get("rows_upserted", 0)
                if "error" in result:
                    errors.append(result["error"])
            except Exception as e:
                errors.append(str(e))

    elapsed = time.time() - t0
    log.info(
        "questdb_index.parallel_bootstrap.done",
        ticks_table=tbl,
        index_table=idx,
        n_symbols=len(syms),
        rows=total_rows,
        seconds=elapsed,
        workers_used=n_workers,
        errors=len(errors),
    )

    return {
        "ok": True,
        "ticks_table": tbl,
        "index_table": idx,
        "rows": int(total_rows),
        "seconds": float(elapsed),
        "workers_used": int(n_workers),
        "errors": errors if errors else None,
    }

