from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import structlog

from .client import QuestDBQueryConfig, connect_pg

log = structlog.get_logger()

MAIN_L5_DAILY_AGG_TABLE_V2 = "ghtrader_main_l5_daily_agg_v2"
MAIN_L5_SOURCE_TABLE_V2 = "ghtrader_ticks_main_l5_v2"


def _day_to_ts_utc(day_s: str) -> datetime:
    d = date.fromisoformat(str(day_s).strip())
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _ns_to_iso(ns: Any) -> str | None:
    try:
        n = int(ns)
        if n <= 0:
            return None
        return datetime.fromtimestamp(float(n) / 1_000_000_000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def ensure_main_l5_daily_agg_table(
    *,
    cfg: QuestDBQueryConfig,
    table: str = MAIN_L5_DAILY_AGG_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    tbl = str(table).strip() or MAIN_L5_DAILY_AGG_TABLE_V2
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {tbl} (
      ts TIMESTAMP,
      symbol SYMBOL,
      trading_day SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      rows_total LONG,
      first_datetime_ns LONG,
      last_datetime_ns LONG,
      updated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)
    """
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            cur.execute(ddl)
            for name, typ in [
                ("symbol", "SYMBOL"),
                ("trading_day", "SYMBOL"),
                ("ticks_kind", "SYMBOL"),
                ("dataset_version", "SYMBOL"),
                ("rows_total", "LONG"),
                ("first_datetime_ns", "LONG"),
                ("last_datetime_ns", "LONG"),
                ("updated_at", "TIMESTAMP"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {name} {typ}")
                except Exception:
                    pass
            try:
                cur.execute(
                    f"ALTER TABLE {tbl} DEDUP ENABLE UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)"
                )
            except Exception:
                pass


def rebuild_main_l5_daily_agg(
    *,
    cfg: QuestDBQueryConfig,
    start_day: date | None = None,
    end_day: date | None = None,
    symbol: str | None = None,
    source_table: str = MAIN_L5_SOURCE_TABLE_V2,
    table: str = MAIN_L5_DAILY_AGG_TABLE_V2,
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    connect_timeout_s: int = 2,
) -> dict[str, Any]:
    ensure_main_l5_daily_agg_table(cfg=cfg, table=table, connect_timeout_s=connect_timeout_s)

    src = str(source_table).strip() or MAIN_L5_SOURCE_TABLE_V2
    tgt = str(table).strip() or MAIN_L5_DAILY_AGG_TABLE_V2
    dv = str(dataset_version).strip()
    tk = str(ticks_kind).strip()
    sym = str(symbol or "").strip()

    where = ["ticks_kind=%s", "dataset_version=%s"]
    params: list[Any] = [tk, dv]
    if sym:
        where.append("symbol=%s")
        params.append(sym)
    if start_day is not None:
        where.append("trading_day >= %s")
        params.append(start_day.isoformat())
    if end_day is not None:
        where.append("trading_day <= %s")
        params.append(end_day.isoformat())

    sql = (
        "SELECT symbol, trading_day, count() AS rows_total, "
        "min(datetime_ns) AS first_datetime_ns, max(datetime_ns) AS last_datetime_ns "
        f"FROM {src} WHERE {' AND '.join(where)} "
        "GROUP BY symbol, trading_day"
    )

    rows: list[tuple[Any, ...]] = []
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall() or [])

    now = datetime.now(timezone.utc)
    upserts: list[tuple[Any, ...]] = []
    invalid_rows = 0
    symbols: set[str] = set()
    for row in rows:
        try:
            row_sym = str(row[0] or "").strip()
            row_day = str(row[1] or "").strip()
            if not row_sym or not row_day:
                invalid_rows += 1
                continue
            ts = _day_to_ts_utc(row_day)
            row_total = int(row[2] or 0)
            row_first_ns = int(row[3] or 0)
            row_last_ns = int(row[4] or 0)
            upserts.append((ts, row_sym, row_day, tk, dv, row_total, row_first_ns, row_last_ns, now))
            symbols.add(row_sym)
        except Exception:
            invalid_rows += 1

    if upserts:
        insert_sql = (
            f"INSERT INTO {tgt} "
            "(ts, symbol, trading_day, ticks_kind, dataset_version, rows_total, first_datetime_ns, last_datetime_ns, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, upserts)

    return {
        "ok": True,
        "source_table": src,
        "target_table": tgt,
        "ticks_kind": tk,
        "dataset_version": dv,
        "symbol_filter": (sym or None),
        "start_day": (start_day.isoformat() if start_day is not None else None),
        "end_day": (end_day.isoformat() if end_day is not None else None),
        "source_groups": int(len(rows)),
        "upserted_rows": int(len(upserts)),
        "invalid_rows": int(invalid_rows),
        "distinct_symbols": int(len(symbols)),
    }


def query_main_l5_symbol_count_for_variety(
    *,
    cfg: QuestDBQueryConfig,
    variety: str,
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    source_table: str = MAIN_L5_SOURCE_TABLE_V2,
    agg_table: str = MAIN_L5_DAILY_AGG_TABLE_V2,
    connect_timeout_s: int = 2,
) -> int:
    var = str(variety or "").strip().lower()
    if not var:
        return 0
    dv = str(dataset_version).strip()
    tk = str(ticks_kind).strip()
    like_pattern = f"%shfe.{var}%"

    agg_sql = (
        f"SELECT count(DISTINCT symbol) FROM {agg_table} "
        "WHERE ticks_kind=%s AND dataset_version=%s AND lower(symbol) LIKE %s"
    )
    raw_sql = (
        f"SELECT count(DISTINCT symbol) FROM {source_table} "
        "WHERE ticks_kind=%s AND dataset_version=%s AND lower(symbol) LIKE %s"
    )

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(agg_sql, [tk, dv, like_pattern])
                row = cur.fetchone() or (0,)
                agg_count = int(row[0] or 0)
                if agg_count > 0:
                    return agg_count
            except Exception as e:
                # Reset transaction state so fallback raw query can proceed.
                try:
                    conn.rollback()  # type: ignore[attr-defined]
                except Exception:
                    pass
                try:
                    log.debug("main_l5_daily_agg.query_fallback_raw", reason=str(e))
                except Exception:
                    pass

            cur.execute(raw_sql, [tk, dv, like_pattern])
            row = cur.fetchone() or (0,)
            return int(row[0] or 0)


def query_main_l5_day_bounds_from_agg(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    agg_table: str = MAIN_L5_DAILY_AGG_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    dv = str(dataset_version).strip()
    tk = str(ticks_kind).strip()
    placeholders = ", ".join(["%s"] * len(symbols))
    sql = (
        "SELECT symbol, min(trading_day) AS first_day, max(trading_day) AS last_day, "
        "count(DISTINCT trading_day) AS n_days, min(first_datetime_ns) AS first_ns, max(last_datetime_ns) AS last_ns "
        f"FROM {agg_table} "
        f"WHERE symbol IN ({placeholders}) AND ticks_kind=%s AND dataset_version=%s "
        "GROUP BY symbol"
    )
    params: list[Any] = list(symbols) + [tk, dv]

    out: dict[str, dict[str, Any]] = {}
    try:
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in (cur.fetchall() or []):
                    try:
                        sym = str(row[0] or "").strip()
                        if not sym:
                            continue
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
    except Exception:
        return {}
    return out


def query_main_l5_latest_from_agg(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    agg_table: str = MAIN_L5_DAILY_AGG_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    dv = str(dataset_version).strip()
    tk = str(ticks_kind).strip()
    placeholders = ", ".join(["%s"] * len(symbols))
    sql = (
        "SELECT symbol, trading_day AS last_day, last_datetime_ns AS last_ns "
        f"FROM {agg_table} "
        f"WHERE symbol IN ({placeholders}) AND ticks_kind=%s AND dataset_version=%s "
        "LATEST ON ts PARTITION BY symbol"
    )
    params: list[Any] = list(symbols) + [tk, dv]

    out: dict[str, dict[str, Any]] = {}
    try:
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in (cur.fetchall() or []):
                    try:
                        sym = str(row[0] or "").strip()
                        if not sym:
                            continue
                        last_ns = row[2]
                        out[sym] = {
                            "last_day": row[1],
                            "last_ns": int(last_ns) if last_ns is not None else None,
                            "last_ts": _ns_to_iso(last_ns),
                        }
                    except Exception:
                        continue
    except Exception:
        return {}
    return out


def query_main_l5_recent_last_from_agg(
    *,
    cfg: QuestDBQueryConfig,
    symbols: list[str],
    trading_days: list[str],
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    agg_table: str = MAIN_L5_DAILY_AGG_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, dict[str, Any]]:
    if not symbols or not trading_days:
        return {}

    day_values = [str(d).strip() for d in trading_days if str(d).strip()]
    if not day_values:
        return {}

    dv = str(dataset_version).strip()
    tk = str(ticks_kind).strip()
    placeholders_syms = ", ".join(["%s"] * len(symbols))
    placeholders_days = ", ".join(["%s"] * len(day_values))
    sql = (
        "SELECT symbol, max(trading_day) AS last_day, max(last_datetime_ns) AS last_ns "
        f"FROM {agg_table} "
        f"WHERE symbol IN ({placeholders_syms}) AND ticks_kind=%s AND dataset_version=%s "
        f"AND trading_day IN ({placeholders_days}) "
        "GROUP BY symbol"
    )
    params: list[Any] = list(symbols) + [tk, dv] + day_values

    out: dict[str, dict[str, Any]] = {}
    try:
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in (cur.fetchall() or []):
                    try:
                        sym = str(row[0] or "").strip()
                        if not sym:
                            continue
                        last_ns = row[2]
                        out[sym] = {
                            "last_day": row[1],
                            "last_ns": int(last_ns) if last_ns is not None else None,
                            "last_ts": _ns_to_iso(last_ns),
                        }
                    except Exception:
                        continue
    except Exception:
        return {}
    return out
