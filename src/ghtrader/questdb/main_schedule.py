from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import pandas as pd
import structlog

from .client import QuestDBQueryConfig, connect_pg

log = structlog.get_logger()

MAIN_SCHEDULE_TABLE_V2 = "ghtrader_main_schedule_v2"


def _day_to_ts_utc(day_s: str) -> datetime:
    d = date.fromisoformat(str(day_s).strip())
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


@dataclass(frozen=True)
class MainScheduleRow:
    exchange: str
    variety: str
    trading_day: str
    main_contract: str
    segment_id: int
    schedule_hash: str


def fetch_main_schedule_state(
    *,
    cfg: QuestDBQueryConfig,
    exchange: str,
    variety: str,
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, Any]:
    """
    Return existing schedule state for (exchange, variety).

    Includes first/last trading day, day count, and distinct schedule hashes.
    """
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    state: dict[str, Any] = {"first_day": None, "last_day": None, "n_days": 0, "schedule_hashes": set()}

    sql_bounds = (
        f"SELECT min(cast(trading_day as string)) AS first_day, "
        f"max(cast(trading_day as string)) AS last_day, "
        f"count() AS n_days "
        f"FROM {tbl} WHERE exchange=%s AND variety=%s"
    )
    sql_hashes = f"SELECT DISTINCT schedule_hash FROM {tbl} WHERE exchange=%s AND variety=%s"

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_bounds, [ex, var])
                row = cur.fetchone()
                if row:
                    first_day = row[0]
                    last_day = row[1]
                    n_days = row[2]
                    try:
                        state["first_day"] = date.fromisoformat(str(first_day)) if first_day else None
                    except Exception:
                        state["first_day"] = None
                    try:
                        state["last_day"] = date.fromisoformat(str(last_day)) if last_day else None
                    except Exception:
                        state["last_day"] = None
                    try:
                        state["n_days"] = int(n_days or 0)
                    except Exception:
                        state["n_days"] = 0
            except Exception:
                pass

            try:
                cur.execute(sql_hashes, [ex, var])
                hashes = {str(r[0]) for r in cur.fetchall() if str(r[0] or "").strip()}
                state["schedule_hashes"] = hashes
            except Exception:
                state["schedule_hashes"] = set()

    return state


def ensure_main_schedule_table(
    *,
    cfg: QuestDBQueryConfig,
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    """
    Ensure the canonical roll schedule table exists in QuestDB.

    Keyed by (exchange, variety, trading_day); `schedule_hash` is expected to be
    stable for a given schedule build (same value across rows written in one run).
    """
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {tbl} (
      ts TIMESTAMP,
      exchange SYMBOL,
      variety SYMBOL,
      trading_day SYMBOL,
      main_contract SYMBOL,
      segment_id LONG,
      schedule_hash SYMBOL,
      updated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, exchange, variety, trading_day)
    """
    try:
        with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
            # DDL/schema evolution: use autocommit so per-statement failures don't
            # abort the whole transaction (psycopg behavior).
            try:
                conn.autocommit = True  # type: ignore[attr-defined]
            except Exception:
                pass
            with conn.cursor() as cur:
                cur.execute(ddl)
                # Best-effort schema evolution for older tables.
                for name, typ in [
                    ("exchange", "SYMBOL"),
                    ("variety", "SYMBOL"),
                    ("trading_day", "SYMBOL"),
                    ("main_contract", "SYMBOL"),
                    ("segment_id", "LONG"),
                    ("schedule_hash", "SYMBOL"),
                    ("updated_at", "TIMESTAMP"),
                ]:
                    try:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {name} {typ}")
                    except Exception:
                        pass
                try:
                    cur.execute(f"ALTER TABLE {tbl} DEDUP ENABLE UPSERT KEYS(ts, exchange, variety, trading_day)")
                except Exception:
                    pass
    except Exception as e:
        log.warning("questdb_main_schedule.ensure_failed", table=tbl, error=str(e))


def clear_main_schedule_rows(
    *,
    cfg: QuestDBQueryConfig,
    exchange: str,
    variety: str,
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> int:
    """
    Delete all schedule rows for a given exchange/variety.
    """
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    from .row_cleanup import replace_table_delete_where

    where = "exchange=%s AND variety=%s"
    cols = ["ts", "exchange", "variety", "trading_day", "main_contract", "segment_id", "schedule_hash", "updated_at"]
    return int(
        replace_table_delete_where(
            cfg=cfg,
            table=tbl,
            columns=cols,
            delete_where_sql=where,
            delete_params=[ex, var],
            ensure_table=lambda t: ensure_main_schedule_table(cfg=cfg, table=t, connect_timeout_s=connect_timeout_s),
            connect_timeout_s=connect_timeout_s,
        )
    )


def trim_main_schedule_before(
    *,
    cfg: QuestDBQueryConfig,
    exchange: str,
    variety: str,
    start_day: date,
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> int:
    """
    Delete schedule rows for (exchange, variety) earlier than `start_day`.
    """
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    if start_day is None:
        return 0
    from .row_cleanup import replace_table_delete_where

    cutoff = start_day.isoformat()
    where = "exchange=%s AND variety=%s AND cast(trading_day as string) < %s"
    cols = ["ts", "exchange", "variety", "trading_day", "main_contract", "segment_id", "schedule_hash", "updated_at"]
    return int(
        replace_table_delete_where(
            cfg=cfg,
            table=tbl,
            columns=cols,
            delete_where_sql=where,
            delete_params=[ex, var, cutoff],
            ensure_table=lambda t: ensure_main_schedule_table(cfg=cfg, table=t, connect_timeout_s=connect_timeout_s),
            connect_timeout_s=connect_timeout_s,
        )
    )


def upsert_main_schedule_rows(
    *,
    cfg: QuestDBQueryConfig,
    exchange: str,
    variety: str,
    schedule_hash: str,
    rows: Iterable[MainScheduleRow],
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> int:
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    sh = str(schedule_hash).strip()
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
                ex,
                var,
                td,
                str(r.main_contract).strip(),
                int(r.segment_id),
                sh,
                now,
            )
        )

    if not params:
        return 0

    sql = (
        f"INSERT INTO {tbl} "
        "(ts, exchange, variety, trading_day, main_contract, segment_id, schedule_hash, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
    return int(len(params))


def fetch_schedule(
    *,
    cfg: QuestDBQueryConfig,
    exchange: str,
    variety: str,
    start_day: date | None = None,
    end_day: date | None = None,
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> pd.DataFrame:
    """
    Fetch schedule rows as a DataFrame with columns:
      - trading_day (date)
      - main_contract (str)
      - segment_id (int)
      - schedule_hash (str)
    """
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    if not ex or not var:
        return pd.DataFrame(columns=["trading_day", "main_contract", "segment_id", "schedule_hash"])

    where = ["exchange=%s", "variety=%s"]
    params: list[Any] = [ex, var]
    if start_day is not None:
        where.append("cast(trading_day as string) >= %s")
        params.append(start_day.isoformat())
    if end_day is not None:
        where.append("cast(trading_day as string) <= %s")
        params.append(end_day.isoformat())

    sql = (
        "SELECT cast(trading_day as string) AS trading_day, main_contract, segment_id, schedule_hash "
        f"FROM {tbl} WHERE {' AND '.join(where)} ORDER BY trading_day ASC"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["trading_day", "main_contract", "segment_id", "schedule_hash"])

    df = pd.DataFrame(rows, columns=["trading_day", "main_contract", "segment_id", "schedule_hash"])
    df["trading_day"] = pd.to_datetime(df["trading_day"], errors="coerce").dt.date
    df["main_contract"] = df["main_contract"].astype(str)
    df["segment_id"] = pd.to_numeric(df["segment_id"], errors="coerce").fillna(0).astype("int64")
    df["schedule_hash"] = df["schedule_hash"].astype(str)
    df = df.dropna(subset=["trading_day", "main_contract"]).sort_values("trading_day").reset_index(drop=True)
    return df


def resolve_main_contract(
    *,
    cfg: QuestDBQueryConfig,
    exchange: str,
    variety: str,
    trading_day: date,
    table: str = MAIN_SCHEDULE_TABLE_V2,
    connect_timeout_s: int = 2,
) -> tuple[str, int, str]:
    """
    Resolve (main_contract, segment_id, schedule_hash) for `trading_day` using
    the most recent schedule row where trading_day <= requested day.
    """
    tbl = str(table).strip() or MAIN_SCHEDULE_TABLE_V2
    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    td = trading_day.isoformat()
    sql = (
        "SELECT main_contract, segment_id, schedule_hash "
        f"FROM {tbl} "
        "WHERE exchange=%s AND variety=%s AND cast(trading_day as string) <= %s "
        "ORDER BY trading_day DESC LIMIT 1"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [ex, var, td])
            row = cur.fetchone()
    if not row:
        raise FileNotFoundError(f"No schedule rows found in {tbl} for {ex}.{var} <= {td}")

    main_contract = str(row[0])
    seg = int(row[1] or 0)
    sh = str(row[2] or "")
    return main_contract, seg, sh

