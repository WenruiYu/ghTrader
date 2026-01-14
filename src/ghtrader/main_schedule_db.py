from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from ghtrader.config import (
    get_data_dir,
    get_questdb_host,
    get_questdb_pg_dbname,
    get_questdb_pg_password,
    get_questdb_pg_port,
    get_questdb_pg_user,
)
from ghtrader.main_contract import MainScheduleResult, compute_shfe_main_schedule_from_daily
from ghtrader.questdb_queries import QuestDBQueryConfig, _psycopg

log = structlog.get_logger()


def _stable_hash_df(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _schedule_dir(data_dir: Path, var: str) -> Path:
    return data_dir / "rolls" / "shfe_main_schedule" / f"var={str(var).lower().strip()}"


def _default_ticks_table(*, lake_version: str) -> str:
    lv = str(lake_version).lower().strip()
    lv = "v2" if lv == "v2" else "v1"
    return f"ghtrader_ticks_raw_{lv}"


def _connect(cfg: QuestDBQueryConfig):
    psycopg = _psycopg()
    return psycopg.connect(
        user=cfg.pg_user,
        password=cfg.pg_password,
        host=cfg.host,
        port=int(cfg.pg_port),
        dbname=cfg.pg_dbname,
    )


def _select_distinct_symbols(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    exchange: str,
    var: str,
    lake_version: str,
    ticks_lake: str = "raw",
) -> list[str]:
    ex = str(exchange).upper().strip()
    v = str(var).lower().strip()
    lv = str(lake_version).lower().strip()
    tl = str(ticks_lake).lower().strip()

    prefix = f"{ex}.{v}"
    pat = prefix + "%"

    # Prefer server-side prefix filter; fall back to full distinct + client filter.
    sql1 = f"SELECT DISTINCT symbol FROM {table} WHERE ticks_lake = %s AND lake_version = %s AND symbol LIKE %s"
    sql2 = f"SELECT DISTINCT symbol FROM {table} WHERE ticks_lake = %s AND lake_version = %s"

    syms: list[str] = []
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql1, (tl, lv, pat))
                syms = [str(r[0]) for r in cur.fetchall()]
            except Exception:
                cur.execute(sql2, (tl, lv))
                syms = [str(r[0]) for r in cur.fetchall()]

    # Keep only specific contracts (exclude continuous aliases).
    rx = re.compile(rf"^{re.escape(prefix)}\d{{4}}$", re.IGNORECASE)
    out = [s for s in syms if rx.match(str(s).strip())]
    return sorted(set(out))


def _fetch_daily_oi(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    start: date,
    end: date,
    lake_version: str,
    ticks_lake: str = "raw",
) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["date", "symbol", "open_interest"])

    lv = str(lake_version).lower().strip()
    tl = str(ticks_lake).lower().strip()

    placeholders = ", ".join(["%s"] * len(symbols))
    # Use trading_day (ISO strings) to avoid timezone ambiguity.
    # Use last(open_interest) to approximate end-of-day OI per (day,symbol).
    sql = (
        "SELECT cast(trading_day as string) AS day, symbol, last(open_interest) AS open_interest "
        f"FROM {table} "
        f"WHERE symbol IN ({placeholders}) AND ticks_lake = %s AND lake_version = %s "
        "AND cast(trading_day as string) >= %s AND cast(trading_day as string) <= %s "
        "GROUP BY day, symbol "
        "ORDER BY day ASC"
    )
    params: list[Any] = list(symbols) + [tl, lv, start.isoformat(), end.isoformat()]

    rows: list[tuple[Any, Any, Any]] = []
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "open_interest"])

    df = pd.DataFrame(rows, columns=["date", "symbol", "open_interest"])
    return df


def build_shfe_main_schedule_from_questdb(
    *,
    var: str,
    start: date,
    end: date,
    rule_threshold: float = 1.1,
    data_dir: Path | None = None,
    lake_version: str = "v2",
    table: str | None = None,
    exchange: str = "SHFE",
) -> MainScheduleResult:
    """
    Build and persist SHFE-style main schedule using QuestDB canonical ticks.
    """
    if data_dir is None:
        data_dir = get_data_dir()
    if end < start:
        raise ValueError("end must be >= start")

    lv = str(lake_version).lower().strip()
    tbl = str(table).strip() if table is not None and str(table).strip() else _default_ticks_table(lake_version=lv)

    cfg = QuestDBQueryConfig(
        host=get_questdb_host(),
        pg_port=int(get_questdb_pg_port()),
        pg_user=str(get_questdb_pg_user()),
        pg_password=str(get_questdb_pg_password()),
        pg_dbname=str(get_questdb_pg_dbname()),
    )

    symbols = _select_distinct_symbols(cfg=cfg, table=tbl, exchange=exchange, var=var, lake_version=lv, ticks_lake="raw")
    if not symbols:
        raise RuntimeError(f"No symbols found in QuestDB table={tbl} for {exchange}.{var} (lake_version={lv})")

    daily = _fetch_daily_oi(cfg=cfg, table=tbl, symbols=symbols, start=start, end=end, lake_version=lv, ticks_lake="raw")
    if daily.empty:
        raise RuntimeError(f"No daily OI rows from QuestDB table={tbl} for {exchange}.{var} {start}->{end}")

    schedule = compute_shfe_main_schedule_from_daily(daily, var=var, rule_threshold=float(rule_threshold), market=exchange)

    out_dir = _schedule_dir(data_dir, var)
    out_dir.mkdir(parents=True, exist_ok=True)
    schedule_path = out_dir / "schedule.parquet"
    schedule.to_parquet(schedule_path, index=False)

    schedule_hash = _stable_hash_df(schedule[["date", "main_contract", "segment_id"]])
    manifest = {
        "created_at": datetime.now().isoformat(),
        "exchange": str(exchange).upper().strip(),
        "variety": str(var).lower().strip(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "rule_threshold": float(rule_threshold),
        "rows": int(len(schedule)),
        "schedule_hash": schedule_hash,
        "schedule_path": str(schedule_path),
        "distinct_main_contracts": sorted([c for c in schedule["main_contract"].astype(str).unique().tolist() if c]),
        "source": "questdb",
        "questdb": {"table": tbl, "lake_version": lv, "ticks_lake": "raw", "symbols_used": int(len(symbols))},
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))

    log.info(
        "main_schedule.questdb_built",
        exchange=manifest["exchange"],
        var=manifest["variety"],
        start=start.isoformat(),
        end=end.isoformat(),
        threshold=float(rule_threshold),
        rows=int(len(schedule)),
        schedule_hash=schedule_hash,
        table=tbl,
    )

    return MainScheduleResult(
        schedule=schedule,
        schedule_hash=schedule_hash,
        schedule_path=schedule_path,
        manifest_path=manifest_path,
    )

