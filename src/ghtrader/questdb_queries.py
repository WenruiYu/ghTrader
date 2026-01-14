from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

log = structlog.get_logger()


@dataclass(frozen=True)
class QuestDBQueryConfig:
    host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_dbname: str


def _psycopg():
    try:
        import psycopg  # type: ignore

        return psycopg
    except Exception as e:
        raise RuntimeError("psycopg not installed. Install with: pip install -e '.[questdb]'") from e


def _connect(cfg: QuestDBQueryConfig):
    psycopg = _psycopg()
    return psycopg.connect(
        user=cfg.pg_user,
        password=cfg.pg_password,
        host=cfg.host,
        port=int(cfg.pg_port),
        dbname=cfg.pg_dbname,
    )


def _l5_condition_sql() -> str:
    # True L5 means at least one of levels 2–5 has positive, non-NaN price values.
    parts: list[str] = []
    for i in range(2, 6):
        parts.append(f"bid_price{i} > 0")
        parts.append(f"ask_price{i} > 0")
    return "(" + " OR ".join(parts) + ")"


def query_symbol_day_bounds(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    lake_version: str,
    ticks_lake: str = "raw",
    l5_only: bool = False,
) -> dict[str, dict[str, Any]]:
    """
    Return {symbol: {first_day, last_day, n_days}} using QuestDB canonical ticks.

    Notes:
    - Uses `trading_day` column (ISO YYYY-MM-DD strings, stored as SYMBOL).
    - Uses `cast(trading_day as string)` for compatibility with min/max.
    - `l5_only=True` restricts to rows that appear to have true L5 values.
    """
    if not symbols:
        return {}

    lv = str(lake_version).lower().strip()
    tl = str(ticks_lake).lower().strip()

    # Safe placeholders for IN (...)
    placeholders = ", ".join(["%s"] * len(symbols))
    where = [f"symbol IN ({placeholders})", "ticks_lake = %s", "lake_version = %s"]
    params: list[Any] = list(symbols) + [tl, lv]
    if l5_only:
        where.append(_l5_condition_sql())

    sql = (
        "SELECT symbol, "
        "min(cast(trading_day as string)) AS first_day, "
        "max(cast(trading_day as string)) AS last_day, "
        "count(DISTINCT cast(trading_day as string)) AS n_days "
        f"FROM {table} "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY symbol"
    )

    out: dict[str, dict[str, Any]] = {}
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    out[sym] = {"first_day": row[1], "last_day": row[2], "n_days": row[3]}
                except Exception:
                    continue
    return out


def query_contract_coverage(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    lake_version: str,
    ticks_lake: str = "raw",
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
    base = query_symbol_day_bounds(
        cfg=cfg, table=table, symbols=symbols, lake_version=lake_version, ticks_lake=ticks_lake, l5_only=False
    )
    l5 = query_symbol_day_bounds(
        cfg=cfg, table=table, symbols=symbols, lake_version=lake_version, ticks_lake=ticks_lake, l5_only=True
    )

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
            "first_l5_day": l.get("first_day"),
            "last_l5_day": l.get("last_day"),
            "l5_days": l.get("n_days"),
        }
    return out

