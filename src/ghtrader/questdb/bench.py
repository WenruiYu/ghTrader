"""
Database benchmarking utilities for QuestDB performance testing.

This module provides tools to benchmark QuestDB ingest and query performance.
It reads sample ticks from QuestDB (canonical store).
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import structlog

from ghtrader.data.ticks_schema import DatasetVersion, TicksKind

log = structlog.get_logger()


DBType = Literal["questdb"]


@dataclass(frozen=True)
class BenchConfig:
    db_type: DBType
    host: str = "127.0.0.1"
    port: int | None = None

    # QuestDB specifics
    questdb_ilp_port: int = 9009
    questdb_pg_port: int = 8812
    questdb_pg_user: str = "admin"
    questdb_pg_password: str = "quest"
    questdb_pg_dbname: str = "qdb"


@dataclass(frozen=True)
class BenchResult:
    db_type: DBType
    table: str
    rows: int
    ingest_seconds: float
    query_seconds: float
    query: str
    extra: dict[str, Any]


from ghtrader.util.time import now_iso as _now_iso


def load_tick_sample(
    *,
    data_dir: Path,
    symbol: str,
    start_date: date,
    end_date: date,
    ticks_kind: TicksKind,
    dataset_version: DatasetVersion,
    max_rows: int = 1_000_000,
) -> pd.DataFrame:
    """
    Load a tick sample from QuestDB for benchmarking.
    """
    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.index import list_present_trading_days
    from ghtrader.questdb.queries import fetch_ticks_for_symbol_day

    _ = data_dir  # QuestDB is the canonical source

    dv = str(dataset_version).lower().strip() or "v2"
    tk = str(ticks_kind).lower().strip() or "raw"
    table = "ghtrader_ticks_main_l5_v2" if tk == "main_l5" else "ghtrader_ticks_raw_v2"
    cfg = make_questdb_query_config_from_env()

    days = sorted(
        list_present_trading_days(
            cfg=cfg,
            symbol=str(symbol),
            start_day=start_date,
            end_day=end_date,
            dataset_version=dv,
            ticks_kind=tk,
        )
    )

    frames: list[pd.DataFrame] = []
    rows = 0
    for d in days:
        if rows >= int(max_rows):
            break
        df_day = fetch_ticks_for_symbol_day(
            cfg=cfg,
            table=table,
            symbol=str(symbol),
            trading_day=d.isoformat(),
            dataset_version=dv,
            ticks_kind=tk,
            limit=None,
            order="asc",
            include_provenance=(tk == "main_l5"),
            connect_timeout_s=2,
        )
        if df_day.empty:
            continue
        frames.append(df_day)
        rows += int(len(df_day))
        if rows >= int(max_rows):
            break

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    if int(len(df)) > int(max_rows):
        df = df.iloc[: int(max_rows)].copy()

    # Keep a compact subset that represents typical query patterns.
    cols = ["symbol", "datetime", "last_price", "bid_price1", "ask_price1", "volume", "open_interest"]
    for c in ["underlying_contract", "segment_id"]:
        if c in df.columns:
            cols.append(c)
    cols = [c for c in cols if c in df.columns]
    return df[cols].copy()


def benchmark_questdb(*, df: pd.DataFrame, cfg: BenchConfig, table_name: str) -> BenchResult:
    """
    Benchmark QuestDB ingestion (ILP) + a simple query (PGWire).

    Requires extras: questdb (Python client) + psycopg[binary].
    """
    if df.empty:
        raise ValueError("Empty dataframe (no ticks to benchmark)")

    try:
        from questdb.ingress import Sender  # type: ignore
    except Exception as e:
        raise RuntimeError("QuestDB client not installed. Install with: pip install -e '.[questdb]'") from e

    try:
        import psycopg  # type: ignore
    except Exception as e:
        raise RuntimeError("psycopg not installed. Install with: pip install -e '.[questdb]'") from e

    # QuestDB uses timestamps in UTC; we treat the int64 as epoch-ns for benchmarking purposes.
    df2 = df.copy()
    df2["ts"] = pd.to_datetime(df2["datetime"].astype("int64"), unit="ns", utc=True)

    t0 = time.time()
    with Sender(cfg.host, int(cfg.questdb_ilp_port)) as sender:
        sender.dataframe(df2.drop(columns=["datetime"]), table_name=table_name, at="ts")
    ingest_s = time.time() - t0

    # Query via PGWire
    q = f"SELECT count(*) FROM {table_name}"
    conn_params = {
        "user": cfg.questdb_pg_user,
        "password": cfg.questdb_pg_password,
        "host": cfg.host,
        "port": int(cfg.questdb_pg_port),
        "dbname": cfg.questdb_pg_dbname,
    }
    t1 = time.time()
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            _ = cur.fetchone()
    query_s = time.time() - t1

    return BenchResult(
        db_type="questdb",
        table=table_name,
        rows=int(len(df2)),
        ingest_seconds=float(ingest_s),
        query_seconds=float(query_s),
        query=q,
        extra={},
    )


def write_bench_report(*, runs_dir: Path, report: dict[str, Any]) -> Path:
    out_dir = runs_dir / "db_bench"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(report.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    out_path = out_dir / f"{run_id}.json"
    out_path.write_text(json.dumps(report, indent=2, default=str))
    return out_path


def run_db_benchmark(
    *,
    data_dir: Path,
    runs_dir: Path,
    symbol: str,
    start_date: date,
    end_date: date,
    ticks_kind: TicksKind,
    dataset_version: DatasetVersion,
    max_rows: int,
    host: str,
) -> tuple[Path, dict[str, Any]]:
    """
    Run best-effort DB benchmark against local QuestDB.

    This command is intended for operator machines where the DB daemons already exist.
    """
    df = load_tick_sample(
        data_dir=data_dir,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        ticks_kind=ticks_kind,
        dataset_version=dataset_version,
        max_rows=max_rows,
    )
    if df.empty:
        raise ValueError("No ticks found for the requested range (cannot benchmark).")

    results: list[BenchResult] = []
    errs: list[str] = []

    try:
        cfg = BenchConfig(db_type="questdb", host=host)
        res = benchmark_questdb(df=df, cfg=cfg, table_name="ghtrader_ticks_bench")
        results.append(res)
    except Exception as e:
        errs.append(f"questdb: {e}")

    report: dict[str, Any] = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        "created_at": _now_iso(),
        "data_dir": str(data_dir),
        "runs_dir": str(runs_dir),
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ticks_kind": ticks_kind,
        "dataset_version": dataset_version,
        "max_rows": int(max_rows),
        "rows_loaded": int(len(df)),
        "results": [asdict(r) for r in results],
        "errors": errs,
    }
    out_path = write_bench_report(runs_dir=runs_dir, report=report)
    log.info("db_bench.done", report_path=str(out_path), n_results=len(results), n_errors=len(errs))
    return out_path, report

