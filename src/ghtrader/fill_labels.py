"""
Fill-probability label builder (PRD §5.4.2).

This module builds per-tick execution fill labels for hypothetical limit orders
at configurable price offsets from L1 quotes.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import structlog

from ghtrader.data.ticks_schema import DatasetVersion, TicksKind, row_hash_from_ticks_df
from ghtrader.util.hash import hash_csv as _hash_csv

log = structlog.get_logger()

FILL_LABELS_TABLE_V2 = "ghtrader_fill_labels_v2"
FILL_LABEL_BUILDS_TABLE_V2 = "ghtrader_fill_label_builds_v2"

_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str) -> str:
    n = str(name or "").strip()
    if not n or not _SQL_IDENT_RE.match(n):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return n


def _normalize_horizons(horizons: Iterable[int] | None) -> list[int]:
    hs = sorted({int(h) for h in (horizons or []) if int(h) > 0})
    return hs or [10, 50, 100, 500]


def _normalize_price_levels(levels: Iterable[int] | None) -> list[int]:
    vals = sorted({int(x) for x in (levels or []) if int(x) >= 0})
    return vals or [0, 1]


def _numeric_or_zero(df: pd.DataFrame, col: str) -> np.ndarray:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0.0).to_numpy(dtype="float64", copy=False)
    return np.zeros(int(len(df)), dtype="float64")


def _first_fill_offsets(
    *,
    reference_prices: np.ndarray,
    order_prices: np.ndarray,
    horizon: int,
    is_bid_side: bool,
) -> np.ndarray:
    """
    Compute first-fill offsets (in ticks) within a horizon.

    For bid-side orders, fill condition is future ask <= order_price.
    For ask-side orders, fill condition is future bid >= order_price.
    Returns float array with NaN when not filled.
    """
    n = int(len(reference_prices))
    out = np.full(n, np.nan, dtype="float64")
    h = int(max(1, horizon))
    unresolved = np.ones(n, dtype=bool)
    for k in range(1, h + 1):
        end = n - k
        if end <= 0:
            break
        if not np.any(unresolved[:end]):
            break
        fut = reference_prices[k:]
        cur = order_prices[:end]
        cond = (fut <= cur) if is_bid_side else (fut >= cur)
        newly = cond & unresolved[:end]
        if np.any(newly):
            idx = np.nonzero(newly)[0]
            out[idx] = float(k)
            unresolved[idx] = False
    return out


def compute_fill_labels_for_day(
    *,
    df: pd.DataFrame,
    symbol: str,
    trading_day: str,
    horizons: Iterable[int] | None = None,
    price_levels: Iterable[int] | None = None,
    price_tick: float = 1.0,
    ticks_kind: str = "main_l5",
    dataset_version: str = "v2",
    build_id: str = "",
    build_ts: datetime | None = None,
    schedule_hash: str = "",
    underlying_contract: str = "",
    segment_id: int = 0,
) -> pd.DataFrame:
    """
    Compute fill labels for one day of ticks.

    Output rows are expanded by (side, price_level) for each tick.
    """
    hs = _normalize_horizons(horizons)
    levels = _normalize_price_levels(price_levels)
    n = int(len(df))
    if n <= 0:
        cols = [
            "symbol",
            "ts",
            "datetime_ns",
            "trading_day",
            "row_hash",
            "ticks_kind",
            "dataset_version",
            "build_id",
            "build_ts",
            "schedule_hash",
            "underlying_contract",
            "segment_id",
            "side",
            "price_level",
            "order_price",
            "queue_position",
        ]
        for h in hs:
            cols += [f"time_to_fill_{h}", f"fill_prob_{h}"]
        return pd.DataFrame(columns=cols)

    dt_ns = pd.to_numeric(df.get("datetime"), errors="coerce").fillna(0).astype("int64").to_numpy(copy=False)
    ts = pd.to_datetime(dt_ns, unit="ns")

    bid1 = _numeric_or_zero(df, "bid_price1")
    ask1 = _numeric_or_zero(df, "ask_price1")
    bid_vol1 = np.maximum(_numeric_or_zero(df, "bid_volume1"), 0.0)
    ask_vol1 = np.maximum(_numeric_or_zero(df, "ask_volume1"), 0.0)

    if "row_hash" in df.columns:
        row_hash = pd.to_numeric(df["row_hash"], errors="coerce").fillna(0).astype("int64").to_numpy(copy=False)
    else:
        row_hash = row_hash_from_ticks_df(df).to_numpy(dtype="int64", copy=False)

    bts = build_ts or datetime.now(timezone.utc).replace(tzinfo=None)
    pt = float(max(1e-12, price_tick))
    base = {
        "symbol": str(symbol),
        "ts": ts,
        "datetime_ns": dt_ns,
        "trading_day": str(trading_day),
        "row_hash": row_hash,
        "ticks_kind": str(ticks_kind),
        "dataset_version": str(dataset_version),
        "build_id": str(build_id),
        "build_ts": bts,
        "schedule_hash": str(schedule_hash or ""),
        "underlying_contract": str(underlying_contract or ""),
        "segment_id": int(segment_id),
    }

    out_frames: list[pd.DataFrame] = []
    for side in ("bid", "ask"):
        is_bid = side == "bid"
        top_price = bid1 if is_bid else ask1
        ref_price = ask1 if is_bid else bid1
        top_vol = bid_vol1 if is_bid else ask_vol1
        sign = -1.0 if is_bid else 1.0
        for level in levels:
            lv = int(level)
            order_price = top_price + sign * float(lv) * pt
            queue_pos = top_vol * (1.0 + float(lv))
            part = pd.DataFrame(
                {
                    **base,
                    "side": side,
                    "price_level": lv,
                    "order_price": order_price,
                    "queue_position": queue_pos,
                }
            )
            for h in hs:
                offsets = _first_fill_offsets(
                    reference_prices=ref_price,
                    order_prices=order_price,
                    horizon=int(h),
                    is_bid_side=is_bid,
                )
                part[f"time_to_fill_{int(h)}"] = pd.Series(offsets).astype("Int64")
                part[f"fill_prob_{int(h)}"] = np.where(np.isnan(offsets), 0.0, 1.0)
            out_frames.append(part)

    return pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame()


def ensure_fill_labels_tables(
    *,
    cfg: Any,
    horizons: Iterable[int] | None = None,
    labels_table: str = FILL_LABELS_TABLE_V2,
    builds_table: str = FILL_LABEL_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    from ghtrader.questdb.client import connect_pg

    lt = _ident(str(labels_table).strip() or FILL_LABELS_TABLE_V2)
    bt = _ident(str(builds_table).strip() or FILL_LABEL_BUILDS_TABLE_V2)
    hs = _normalize_horizons(horizons)

    ddl_data = f"""
    CREATE TABLE IF NOT EXISTS {lt} (
      symbol SYMBOL,
      ts TIMESTAMP,
      datetime_ns LONG,
      trading_day SYMBOL,
      row_hash LONG,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      build_id SYMBOL,
      build_ts TIMESTAMP,
      schedule_hash SYMBOL,
      underlying_contract SYMBOL,
      segment_id LONG,
      side SYMBOL,
      price_level LONG,
      order_price DOUBLE,
      queue_position DOUBLE
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, build_id, side, price_level, row_hash)
    """

    ddl_meta = f"""
    CREATE TABLE IF NOT EXISTS {bt} (
      ts TIMESTAMP,
      symbol SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      build_id SYMBOL,
      horizons STRING,
      price_levels STRING,
      price_tick DOUBLE,
      schedule_hash SYMBOL,
      rows_total LONG,
      first_day SYMBOL,
      last_day SYMBOL
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
    """

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            cur.execute(ddl_data)
            cur.execute(ddl_meta)

            base_cols = [
                ("datetime_ns", "LONG"),
                ("trading_day", "SYMBOL"),
                ("row_hash", "LONG"),
                ("ticks_kind", "SYMBOL"),
                ("dataset_version", "SYMBOL"),
                ("build_id", "SYMBOL"),
                ("build_ts", "TIMESTAMP"),
                ("schedule_hash", "SYMBOL"),
                ("underlying_contract", "SYMBOL"),
                ("segment_id", "LONG"),
                ("side", "SYMBOL"),
                ("price_level", "LONG"),
                ("order_price", "DOUBLE"),
                ("queue_position", "DOUBLE"),
            ]
            for name, typ in base_cols:
                try:
                    cur.execute(f"ALTER TABLE {lt} ADD COLUMN {name} {typ}")
                except Exception:
                    pass
            for h in hs:
                try:
                    cur.execute(f"ALTER TABLE {lt} ADD COLUMN time_to_fill_{int(h)} LONG")
                except Exception:
                    pass
                try:
                    cur.execute(f"ALTER TABLE {lt} ADD COLUMN fill_prob_{int(h)} DOUBLE")
                except Exception:
                    pass
            try:
                cur.execute(
                    f"ALTER TABLE {lt} DEDUP ENABLE UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, build_id, side, price_level, row_hash)"
                )
            except Exception:
                pass


def insert_fill_label_build(
    *,
    cfg: Any,
    symbol: str,
    ticks_kind: str,
    dataset_version: str,
    build_id: str,
    horizons: Iterable[int],
    price_levels: Iterable[int],
    price_tick: float,
    schedule_hash: str | None,
    rows_total: int,
    first_day: str | None,
    last_day: str | None,
    builds_table: str = FILL_LABEL_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    from ghtrader.questdb.client import connect_pg

    bt = _ident(str(builds_table).strip() or FILL_LABEL_BUILDS_TABLE_V2)
    now = datetime.now(timezone.utc)
    sql = (
        f"INSERT INTO {bt} "
        "(ts, symbol, ticks_kind, dataset_version, build_id, horizons, price_levels, price_tick, schedule_hash, rows_total, first_day, last_day) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    params = (
        now,
        str(symbol),
        str(ticks_kind),
        str(dataset_version),
        str(build_id),
        ",".join(str(int(h)) for h in _normalize_horizons(horizons)),
        ",".join(str(int(x)) for x in _normalize_price_levels(price_levels)),
        float(price_tick),
        str(schedule_hash or ""),
        int(rows_total),
        str(first_day or ""),
        str(last_day or ""),
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def build_fill_labels_for_symbol(
    *,
    symbol: str,
    data_dir: Path,
    horizons: Iterable[int] | None = None,
    price_levels: Iterable[int] | None = None,
    price_tick: float = 1.0,
    ticks_kind: TicksKind = "main_l5",
    dataset_version: DatasetVersion = "v2",
    table: str = FILL_LABELS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, Any]:
    """
    Build fill labels from canonical ticks and store them in QuestDB.
    """
    _ = data_dir
    from ghtrader.config import (
        get_questdb_host,
        get_questdb_ilp_port,
        get_questdb_pg_dbname,
        get_questdb_pg_password,
        get_questdb_pg_port,
        get_questdb_pg_user,
    )
    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.queries import fetch_ticks_for_day, list_trading_days_for_symbol, query_symbol_day_bounds
    from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

    hs = _normalize_horizons(horizons)
    levels = _normalize_price_levels(price_levels)
    sym = str(symbol).strip()
    tk = str(ticks_kind).strip().lower() or "main_l5"
    dv = str(dataset_version).strip().lower() or "v2"
    if not sym:
        raise ValueError("symbol is required")
    if tk != "main_l5":
        raise ValueError("fill labels currently support main_l5 only (Phase-0)")

    cfg = make_questdb_query_config_from_env()
    ensure_fill_labels_tables(cfg=cfg, horizons=hs, labels_table=table, connect_timeout_s=connect_timeout_s)

    bounds = query_symbol_day_bounds(
        cfg=cfg,
        table="ghtrader_ticks_main_l5_v2",
        symbols=[sym],
        dataset_version=dv,
        ticks_kind=tk,
        l5_only=False,
    )
    b = bounds.get(sym) or {}
    d0 = str(b.get("first_day") or "").strip()
    d1 = str(b.get("last_day") or "").strip()
    if not d0 or not d1:
        raise ValueError(f"No tick data found for {sym} (ticks_kind={tk}, dataset_version={dv}) in QuestDB")
    days = list_trading_days_for_symbol(
        cfg=cfg,
        table="ghtrader_ticks_main_l5_v2",
        symbol=sym,
        start_day=date.fromisoformat(d0),
        end_day=date.fromisoformat(d1),
        dataset_version=dv,
        ticks_kind=tk,
    )
    if not days:
        raise ValueError(f"No trading days found for {sym}")

    backend = make_serving_backend(
        ServingDBConfig(
            backend="questdb",
            host=str(get_questdb_host()),
            questdb_ilp_port=int(get_questdb_ilp_port()),
            questdb_pg_port=int(get_questdb_pg_port()),
            questdb_pg_user=str(get_questdb_pg_user()),
            questdb_pg_password=str(get_questdb_pg_password()),
            questdb_pg_dbname=str(get_questdb_pg_dbname()),
        )
    )

    build_id = _hash_csv(
        [
            sym,
            tk,
            dv,
            ",".join(str(int(h)) for h in hs),
            ",".join(str(int(x)) for x in levels),
            str(float(price_tick)),
        ]
    )
    build_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    rows_total = 0
    schedule_hash = ""

    for td in days:
        df_day = fetch_ticks_for_day(
            cfg=cfg,
            symbol=sym,
            trading_day=td,
            table="ghtrader_ticks_main_l5_v2",
            columns=[
                "datetime_ns as datetime",
                "trading_day",
                "bid_price1",
                "ask_price1",
                "bid_volume1",
                "ask_volume1",
                "row_hash",
                "underlying_contract",
                "segment_id",
                "schedule_hash",
            ],
            dataset_version=dv,
            ticks_kind=tk,
            connect_timeout_s=connect_timeout_s,
        )
        if df_day.empty:
            continue
        if "row_hash" not in df_day.columns or pd.to_numeric(df_day["row_hash"], errors="coerce").isna().all():
            df_day = df_day.copy()
            df_day["row_hash"] = row_hash_from_ticks_df(df_day)
        sh = ""
        if "schedule_hash" in df_day.columns and not df_day["schedule_hash"].empty:
            try:
                v = df_day["schedule_hash"].iloc[0]
                if not pd.isna(v):
                    sh = str(v)
            except Exception:
                sh = ""
        if sh and not schedule_hash:
            schedule_hash = sh
        uc = ""
        if "underlying_contract" in df_day.columns and not df_day["underlying_contract"].empty:
            try:
                v = df_day["underlying_contract"].iloc[0]
                if not pd.isna(v):
                    uc = str(v)
            except Exception:
                uc = ""
        seg = 0
        if "segment_id" in df_day.columns and not df_day["segment_id"].empty:
            try:
                v = pd.to_numeric(df_day["segment_id"].iloc[0], errors="coerce")
                seg = int(0 if pd.isna(v) else v)
            except Exception:
                seg = 0
        out = compute_fill_labels_for_day(
            df=df_day,
            symbol=sym,
            trading_day=td.isoformat(),
            horizons=hs,
            price_levels=levels,
            price_tick=float(price_tick),
            ticks_kind=tk,
            dataset_version=dv,
            build_id=build_id,
            build_ts=build_ts,
            schedule_hash=sh or schedule_hash,
            underlying_contract=uc,
            segment_id=seg,
        )
        if out.empty:
            continue
        backend.ingest_df(table=str(table), df=out)
        rows_total += int(len(out))
        log.info(
            "fill_labels.build_day",
            symbol=sym,
            trading_day=td.isoformat(),
            rows=int(len(out)),
            cumulative_rows=int(rows_total),
        )

    insert_fill_label_build(
        cfg=cfg,
        symbol=sym,
        ticks_kind=tk,
        dataset_version=dv,
        build_id=build_id,
        horizons=hs,
        price_levels=levels,
        price_tick=float(price_tick),
        schedule_hash=schedule_hash,
        rows_total=int(rows_total),
        first_day=(days[0].isoformat() if days else ""),
        last_day=(days[-1].isoformat() if days else ""),
        connect_timeout_s=connect_timeout_s,
    )
    return {
        "ok": True,
        "symbol": sym,
        "ticks_kind": tk,
        "dataset_version": dv,
        "build_id": build_id,
        "horizons": hs,
        "price_levels": levels,
        "price_tick": float(price_tick),
        "rows_total": int(rows_total),
        "days": int(len(days)),
    }


__all__ = [
    "FILL_LABELS_TABLE_V2",
    "FILL_LABEL_BUILDS_TABLE_V2",
    "build_fill_labels_for_symbol",
    "compute_fill_labels_for_day",
    "ensure_fill_labels_tables",
]

