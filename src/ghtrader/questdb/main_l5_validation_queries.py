from __future__ import annotations

from datetime import date
from typing import Any

from ghtrader.questdb.client import connect_pg
from ghtrader.util.l5_detection import l5_sql_condition


def fetch_day_second_stats(
    *,
    cfg: Any,
    symbol: str,
    start_day: date,
    end_day: date,
    l5_only: bool = True,
) -> dict[date, dict[str, int]]:
    l5_cond = l5_sql_condition() if l5_only else ""
    l5_where = f" AND {l5_cond} " if l5_cond else " "
    sql = (
        "WITH per_sec AS ("
        "  SELECT cast(trading_day as string) AS trading_day, "
        "         cast(datetime_ns/1000000000 as long) AS sec, "
        "         count() AS n "
        "  FROM ghtrader_ticks_main_l5_v2 "
        f"  WHERE symbol=%s AND ticks_kind='main_l5' AND dataset_version='v2'{l5_where}"
        "    AND cast(trading_day as string) >= %s AND cast(trading_day as string) <= %s "
        "  GROUP BY trading_day, sec"
        ") "
        "SELECT trading_day, "
        "       count() AS seconds_with_ticks, "
        "       sum(case when n=1 then 1 else 0 end) AS seconds_with_one, "
        "       sum(case when n>=2 then 1 else 0 end) AS seconds_with_two_plus "
        "FROM per_sec GROUP BY trading_day"
    )
    out: dict[date, dict[str, int]] = {}
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol).strip(), start_day.isoformat(), end_day.isoformat()])
            for row in cur.fetchall():
                try:
                    td = date.fromisoformat(str(row[0]))
                except Exception:
                    continue
                out[td] = {
                    "seconds_with_ticks": int(row[1] or 0),
                    "seconds_with_one": int(row[2] or 0),
                    "seconds_with_two_plus": int(row[3] or 0),
                }
    return out


def fetch_per_second_counts(
    *,
    cfg: Any,
    symbol: str,
    trading_day: date,
    l5_only: bool = True,
) -> dict[int, int]:
    l5_cond = l5_sql_condition() if l5_only else ""
    l5_where = f" AND {l5_cond} " if l5_cond else " "
    sql = (
        "SELECT cast(datetime_ns/1000000000 as long) AS sec, count() AS n "
        "FROM ghtrader_ticks_main_l5_v2 "
        f"WHERE symbol=%s AND ticks_kind='main_l5' AND dataset_version='v2'{l5_where}AND trading_day=%s "
        "GROUP BY sec"
    )
    out: dict[int, int] = {}
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol).strip(), trading_day.isoformat()])
            for sec, n in cur.fetchall():
                if sec is None:
                    continue
                out[int(sec)] = int(n or 0)
    return out


def get_last_validated_day(
    *,
    cfg: Any,
    symbol: str,
    table: str = "ghtrader_main_l5_validate_summary_v2",
) -> date | None:
    """Get the last validated trading day for a symbol from QuestDB."""
    sql = f"SELECT max(cast(trading_day as string)) FROM {table} WHERE symbol=%s"
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol).strip()])
            row = cur.fetchone()
            if row and row[0]:
                try:
                    return date.fromisoformat(str(row[0]))
                except Exception:
                    pass
    return None


__all__ = [
    "fetch_day_second_stats",
    "fetch_per_second_counts",
    "get_last_validated_day",
]

