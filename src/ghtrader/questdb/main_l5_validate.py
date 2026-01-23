from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import structlog

from .client import QuestDBQueryConfig, connect_pg

log = structlog.get_logger()

MAIN_L5_VALIDATE_SUMMARY_TABLE = "ghtrader_main_l5_validate_summary_v2"
MAIN_L5_VALIDATE_GAPS_TABLE = "ghtrader_main_l5_validate_gaps_v2"


def _day_to_ts_utc(day_s: str) -> datetime:
    d = date.fromisoformat(str(day_s).strip())
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


@dataclass(frozen=True)
class MainL5ValidateSummaryRow:
    symbol: str
    trading_day: str
    cadence_mode: str
    expected_seconds: int
    expected_seconds_strict: int
    seconds_with_ticks: int
    seconds_with_two_plus: int
    two_plus_ratio: float
    observed_segments: int
    total_segments: int
    missing_day: int
    missing_segments: int
    missing_half_seconds: int
    max_gap_s: int
    gap_threshold_s: float
    schedule_hash: str


@dataclass(frozen=True)
class MainL5ValidateGapRow:
    symbol: str
    trading_day: str
    session: str
    start_ts: datetime
    end_ts: datetime
    duration_s: int
    tqsdk_status: str
    schedule_hash: str


def ensure_main_l5_validate_summary_table(
    *,
    cfg: QuestDBQueryConfig,
    table: str = MAIN_L5_VALIDATE_SUMMARY_TABLE,
    connect_timeout_s: int = 2,
) -> None:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_SUMMARY_TABLE
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {tbl} (
      ts TIMESTAMP,
      symbol SYMBOL,
      trading_day SYMBOL,
      cadence_mode SYMBOL,
      expected_seconds LONG,
      expected_seconds_strict LONG,
      seconds_with_ticks LONG,
      seconds_with_two_plus LONG,
      two_plus_ratio DOUBLE,
      observed_segments LONG,
      total_segments LONG,
      missing_day LONG,
      missing_segments LONG,
      missing_half_seconds LONG,
      max_gap_s LONG,
      gap_threshold_s DOUBLE,
      schedule_hash SYMBOL,
      updated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, trading_day)
    """
    try:
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
                    ("cadence_mode", "SYMBOL"),
                    ("expected_seconds", "LONG"),
                    ("expected_seconds_strict", "LONG"),
                    ("seconds_with_ticks", "LONG"),
                    ("seconds_with_two_plus", "LONG"),
                    ("two_plus_ratio", "DOUBLE"),
                    ("observed_segments", "LONG"),
                    ("total_segments", "LONG"),
                    ("missing_day", "LONG"),
                    ("missing_segments", "LONG"),
                    ("missing_half_seconds", "LONG"),
                    ("max_gap_s", "LONG"),
                    ("gap_threshold_s", "DOUBLE"),
                    ("schedule_hash", "SYMBOL"),
                    ("updated_at", "TIMESTAMP"),
                ]:
                    try:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {name} {typ}")
                    except Exception:
                        pass
                try:
                    cur.execute(f"ALTER TABLE {tbl} DEDUP ENABLE UPSERT KEYS(ts, symbol, trading_day)")
                except Exception:
                    pass
    except Exception as e:
        log.warning("questdb_main_l5_validate.summary_ensure_failed", table=tbl, error=str(e))


def ensure_main_l5_validate_gaps_table(
    *,
    cfg: QuestDBQueryConfig,
    table: str = MAIN_L5_VALIDATE_GAPS_TABLE,
    connect_timeout_s: int = 2,
) -> None:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_GAPS_TABLE
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {tbl} (
      ts TIMESTAMP,
      symbol SYMBOL,
      trading_day SYMBOL,
      session SYMBOL,
      start_ts TIMESTAMP,
      end_ts TIMESTAMP,
      duration_s LONG,
      tqsdk_status SYMBOL,
      schedule_hash SYMBOL,
      updated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, trading_day, start_ts)
    """
    try:
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
                    ("session", "SYMBOL"),
                    ("start_ts", "TIMESTAMP"),
                    ("end_ts", "TIMESTAMP"),
                    ("duration_s", "LONG"),
                    ("tqsdk_status", "SYMBOL"),
                    ("schedule_hash", "SYMBOL"),
                    ("updated_at", "TIMESTAMP"),
                ]:
                    try:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {name} {typ}")
                    except Exception:
                        pass
                try:
                    cur.execute(f"ALTER TABLE {tbl} DEDUP ENABLE UPSERT KEYS(ts, symbol, trading_day, start_ts)")
                except Exception:
                    pass
    except Exception as e:
        log.warning("questdb_main_l5_validate.gaps_ensure_failed", table=tbl, error=str(e))


def upsert_main_l5_validate_summary_rows(
    *,
    cfg: QuestDBQueryConfig,
    rows: Iterable[MainL5ValidateSummaryRow],
    table: str = MAIN_L5_VALIDATE_SUMMARY_TABLE,
    connect_timeout_s: int = 2,
) -> int:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_SUMMARY_TABLE
    rs = list(rows)
    if not rs:
        return 0
    now = datetime.now(timezone.utc)
    params: list[tuple[Any, ...]] = []
    for r in rs:
        td = str(r.trading_day).strip()
        if not td:
            continue
        params.append(
            (
                _day_to_ts_utc(td),
                str(r.symbol).strip(),
                td,
                str(r.cadence_mode).strip(),
                int(r.expected_seconds),
                int(r.expected_seconds_strict),
                int(r.seconds_with_ticks),
                int(r.seconds_with_two_plus),
                float(r.two_plus_ratio),
                int(r.observed_segments),
                int(r.total_segments),
                int(r.missing_day),
                int(r.missing_segments),
                int(r.missing_half_seconds),
                int(r.max_gap_s),
                float(r.gap_threshold_s),
                str(r.schedule_hash).strip(),
                now,
            )
        )
    if not params:
        return 0
    sql = (
        f"INSERT INTO {tbl} "
        "(ts, symbol, trading_day, cadence_mode, expected_seconds, expected_seconds_strict, "
        "seconds_with_ticks, seconds_with_two_plus, two_plus_ratio, observed_segments, total_segments, "
        "missing_day, missing_segments, missing_half_seconds, max_gap_s, gap_threshold_s, schedule_hash, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
    return int(len(params))


def insert_main_l5_validate_gap_rows(
    *,
    cfg: QuestDBQueryConfig,
    rows: Iterable[MainL5ValidateGapRow],
    table: str = MAIN_L5_VALIDATE_GAPS_TABLE,
    connect_timeout_s: int = 2,
) -> int:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_GAPS_TABLE
    rs = list(rows)
    if not rs:
        return 0
    now = datetime.now(timezone.utc)
    params: list[tuple[Any, ...]] = []
    for r in rs:
        td = str(r.trading_day).strip()
        if not td:
            continue
        params.append(
            (
                r.start_ts,
                str(r.symbol).strip(),
                td,
                str(r.session).strip(),
                r.start_ts,
                r.end_ts,
                int(r.duration_s),
                str(r.tqsdk_status).strip(),
                str(r.schedule_hash).strip(),
                now,
            )
        )
    if not params:
        return 0
    sql = (
        f"INSERT INTO {tbl} "
        "(ts, symbol, trading_day, session, start_ts, end_ts, duration_s, tqsdk_status, schedule_hash, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
    return int(len(params))


def clear_main_l5_validate_gap_rows(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    start_day: date | None = None,
    end_day: date | None = None,
    table: str = MAIN_L5_VALIDATE_GAPS_TABLE,
    connect_timeout_s: int = 2,
) -> int:
    """
    Delete gap rows for a symbol, optionally limited to trading_day range.

    Falls back to table rebuild if DELETE is not supported.
    """
    tbl = str(table).strip() or MAIN_L5_VALIDATE_GAPS_TABLE
    sym = str(symbol or "").strip()
    if not sym:
        return 0
    where = ["symbol=%s"]
    params: list[Any] = [sym]
    if start_day is not None:
        where.append("cast(trading_day as string) >= %s")
        params.append(start_day.isoformat())
    if end_day is not None:
        where.append("cast(trading_day as string) <= %s")
        params.append(end_day.isoformat())
    where_sql = " AND ".join(where)
    count_sql = f"SELECT count() FROM {tbl} WHERE {where_sql}"
    delete_sql = f"DELETE FROM {tbl} WHERE {where_sql}"
    cols = "ts, symbol, trading_day, session, start_ts, end_ts, duration_s, tqsdk_status, schedule_hash, updated_at"
    tmp_table = f"{tbl}_clean"
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            try:
                cur.execute(count_sql, params)
                (count,) = cur.fetchone() or (0,)
            except Exception:
                count = 0
            try:
                cur.execute(delete_sql, params)
                return int(count)
            except Exception as e:
                log.warning("questdb_main_l5_validate.gaps_delete_failed", table=tbl, error=str(e))
                ensure_main_l5_validate_gaps_table(cfg=cfg, table=tmp_table, connect_timeout_s=connect_timeout_s)
                insert_sql = (
                    f"INSERT INTO {tmp_table} ({cols}) "
                    f"SELECT {cols} FROM {tbl} WHERE NOT ({where_sql})"
                )
                cur.execute(insert_sql, params)
                cur.execute(f"DROP TABLE {tbl}")
                cur.execute(f"RENAME TABLE {tmp_table} TO {tbl}")
                return int(count)


def fetch_latest_main_l5_validate_summary(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    limit: int = 30,
    table: str = MAIN_L5_VALIDATE_SUMMARY_TABLE,
    connect_timeout_s: int = 2,
) -> list[dict[str, Any]]:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_SUMMARY_TABLE
    sym = str(symbol or "").strip()
    lim = max(1, min(int(limit or 30), 3650))
    sql = (
        "SELECT trading_day, cadence_mode, expected_seconds, expected_seconds_strict, seconds_with_ticks, "
        "seconds_with_two_plus, two_plus_ratio, observed_segments, total_segments, missing_day, missing_segments, "
        "missing_half_seconds, max_gap_s, gap_threshold_s, schedule_hash, updated_at "
        f"FROM {tbl} WHERE symbol=%s "
        "ORDER BY cast(trading_day as string) DESC LIMIT %s"
    )
    out: list[dict[str, Any]] = []
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [sym, lim])
            for row in cur.fetchall():
                out.append(
                    {
                        "trading_day": row[0],
                        "cadence_mode": row[1],
                        "expected_seconds": row[2],
                        "expected_seconds_strict": row[3],
                        "seconds_with_ticks": row[4],
                        "seconds_with_two_plus": row[5],
                        "two_plus_ratio": row[6],
                        "observed_segments": row[7],
                        "total_segments": row[8],
                        "missing_day": row[9],
                        "missing_segments": row[10],
                        "missing_half_seconds": row[11],
                        "max_gap_s": row[12],
                        "gap_threshold_s": row[13],
                        "schedule_hash": row[14],
                        "updated_at": row[15].isoformat() if row[15] else None,
                    }
                )
    return out


def list_main_l5_validate_gaps(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    start_day: date | None = None,
    end_day: date | None = None,
    limit: int = 500,
    table: str = MAIN_L5_VALIDATE_GAPS_TABLE,
    connect_timeout_s: int = 2,
) -> list[dict[str, Any]]:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_GAPS_TABLE
    sym = str(symbol or "").strip()
    lim = max(1, min(int(limit or 500), 10000))
    where = ["symbol=%s"]
    params: list[Any] = [sym]
    if start_day is not None:
        where.append("cast(trading_day as string) >= %s")
        params.append(start_day.isoformat())
    if end_day is not None:
        where.append("cast(trading_day as string) <= %s")
        params.append(end_day.isoformat())
    sql = (
        "SELECT trading_day, session, start_ts, end_ts, duration_s, tqsdk_status, schedule_hash "
        f"FROM {tbl} WHERE {' AND '.join(where)} "
        "ORDER BY start_ts DESC LIMIT %s"
    )
    params.append(lim)
    out: list[dict[str, Any]] = []
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                out.append(
                    {
                        "trading_day": row[0],
                        "session": row[1],
                        "start_ts": row[2].isoformat() if row[2] else None,
                        "end_ts": row[3].isoformat() if row[3] else None,
                        "duration_s": row[4],
                        "tqsdk_status": row[5],
                        "schedule_hash": row[6],
                    }
                )
    return out


def fetch_main_l5_validate_overview(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    table: str = MAIN_L5_VALIDATE_SUMMARY_TABLE,
    connect_timeout_s: int = 2,
) -> dict[str, Any]:
    tbl = str(table).strip() or MAIN_L5_VALIDATE_SUMMARY_TABLE
    sym = str(symbol or "").strip()
    sql = (
        "SELECT "
        "count() AS days_total, "
        "sum(missing_day) AS missing_days, "
        "sum(case when missing_segments > 0 then 1 else 0 end) AS days_with_gaps, "
        "sum(missing_segments) AS missing_segments, "
        "sum(missing_half_seconds) AS missing_half_seconds, "
        "sum(expected_seconds_strict) AS expected_seconds_strict_total, "
        "sum(expected_seconds) AS expected_seconds_total, "
        "sum(total_segments) AS total_segments, "
        "max(max_gap_s) AS max_gap_s, "
        "max(gap_threshold_s) AS gap_threshold_s, "
        "max(cast(trading_day as string)) AS last_day "
        f"FROM {tbl} WHERE symbol=%s"
    )
    out: dict[str, Any] = {
        "days_total": 0,
        "missing_days": 0,
        "days_with_gaps": 0,
        "missing_segments": 0,
        "missing_half_seconds": 0,
        "expected_seconds_strict_total": 0,
        "expected_seconds_total": 0,
        "total_segments": 0,
        "max_gap_s": 0,
        "gap_threshold_s": 0.0,
        "last_day": None,
    }
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [sym])
            row = cur.fetchone()
            if row:
                out["days_total"] = int(row[0] or 0)
                out["missing_days"] = int(row[1] or 0)
                out["days_with_gaps"] = int(row[2] or 0)
                out["missing_segments"] = int(row[3] or 0)
                out["missing_half_seconds"] = int(row[4] or 0)
                out["expected_seconds_strict_total"] = int(row[5] or 0)
                out["expected_seconds_total"] = int(row[6] or 0)
                out["total_segments"] = int(row[7] or 0)
                out["max_gap_s"] = int(row[8] or 0)
                out["gap_threshold_s"] = float(row[9] or 0.0)
                out["last_day"] = row[10]
    return out
