from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import structlog

from ghtrader.config import (
    get_data_dir,
    get_runs_dir,
)
from ghtrader.main_contract import MainScheduleResult, compute_shfe_main_schedule_from_daily
from ghtrader.questdb_client import make_questdb_query_config_from_env
from ghtrader.questdb_main_schedule import MAIN_SCHEDULE_TABLE_V2, MainScheduleRow, ensure_main_schedule_table, upsert_main_schedule_rows

log = structlog.get_logger()


def _stable_hash_df(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _schedule_report_dir(runs_dir: Path) -> Path:
    # Reports live under runs/ (never used by runtime resolution).
    return runs_dir / "control" / "reports" / "main_schedule"


def _default_ticks_table(*, lake_version: str) -> str:
    _ = lake_version  # v2-only
    return "ghtrader_ticks_raw_v2"


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
    from ghtrader.questdb_client import connect_pg

    with connect_pg(cfg, connect_timeout_s=2) as conn:
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
    from ghtrader.questdb_client import connect_pg

    with connect_pg(cfg, connect_timeout_s=2) as conn:
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
    runs_dir: Path | None = None,
    lake_version: str = "v2",
    table: str | None = None,
    exchange: str = "SHFE",
) -> MainScheduleResult:
    """
    Build and persist SHFE-style main schedule using QuestDB canonical ticks.

    Canonical persistence:
    - QuestDB table `ghtrader_main_schedule_v2`
    """
    if data_dir is None:
        data_dir = get_data_dir()
    if runs_dir is None:
        runs_dir = get_runs_dir()
    if end < start:
        raise ValueError("end must be >= start")

    _ = data_dir  # schedule is QuestDB-canonical; data_dir is unused (kept for API compatibility)
    _ = lake_version  # v2-only
    lv = "v2"
    tbl = str(table).strip() if table is not None and str(table).strip() else _default_ticks_table(lake_version=lv)

    cfg = make_questdb_query_config_from_env()

    symbols = _select_distinct_symbols(cfg=cfg, table=tbl, exchange=exchange, var=var, lake_version=lv, ticks_lake="raw")
    if not symbols:
        raise RuntimeError(f"No symbols found in QuestDB table={tbl} for {exchange}.{var} (lake_version={lv})")

    daily = _fetch_daily_oi(cfg=cfg, table=tbl, symbols=symbols, start=start, end=end, lake_version=lv, ticks_lake="raw")
    if daily.empty:
        raise RuntimeError(f"No daily OI rows from QuestDB table={tbl} for {exchange}.{var} {start}->{end}")

    schedule = compute_shfe_main_schedule_from_daily(daily, var=var, rule_threshold=float(rule_threshold), market=exchange)

    schedule_hash = _stable_hash_df(schedule[["date", "main_contract", "segment_id"]])

    # Persist schedule to QuestDB (canonical).
    ensure_main_schedule_table(cfg=cfg, table=MAIN_SCHEDULE_TABLE_V2, connect_timeout_s=2)
    rows = [
        MainScheduleRow(
            exchange=str(exchange).upper().strip(),
            variety=str(var).lower().strip(),
            trading_day=str(d.isoformat()),
            main_contract=str(mc),
            segment_id=int(seg),
            schedule_hash=str(schedule_hash),
        )
        for d, mc, seg in schedule[["date", "main_contract", "segment_id"]].itertuples(index=False, name=None)
        if d is not None and str(mc).strip()
    ]
    n_upsert = upsert_main_schedule_rows(
        cfg=cfg,
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        schedule_hash=str(schedule_hash),
        rows=rows,
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )

    # Best-effort report (debugging only; not used by runtime resolution).
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "exchange": str(exchange).upper().strip(),
        "variety": str(var).lower().strip(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "rule_threshold": float(rule_threshold),
        "rows": int(len(schedule)),
        "schedule_hash": schedule_hash,
        "distinct_main_contracts": sorted([c for c in schedule["main_contract"].astype(str).unique().tolist() if c]),
        "source": "questdb",
        "questdb": {
            "ticks_table": tbl,
            "lake_version": lv,
            "ticks_lake": "raw",
            "symbols_used": int(len(symbols)),
            "schedule_table": MAIN_SCHEDULE_TABLE_V2,
            "schedule_rows_upserted": int(n_upsert),
        },
    }
    report_path: Path | None = None
    try:
        out_dir = _schedule_report_dir(Path(runs_dir))
        out_dir.mkdir(parents=True, exist_ok=True)
        run_id = uuid4().hex[:12]
        report_path = out_dir / f"main_schedule_exchange={report['exchange']}_var={report['variety']}_{run_id}.json"
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    except Exception:
        report_path = None

    log.info(
        "main_schedule.questdb_built",
        exchange=report["exchange"],
        var=report["variety"],
        start=start.isoformat(),
        end=end.isoformat(),
        threshold=float(rule_threshold),
        rows=int(len(schedule)),
        schedule_hash=schedule_hash,
        ticks_table=tbl,
        schedule_table=MAIN_SCHEDULE_TABLE_V2,
        schedule_rows_upserted=int(n_upsert),
        report_path=(str(report_path) if report_path else ""),
    )

    return MainScheduleResult(
        schedule=schedule,
        schedule_hash=schedule_hash,
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        questdb_table=MAIN_SCHEDULE_TABLE_V2,
    )

