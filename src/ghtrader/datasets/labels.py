"""
Event-time multi-horizon labels and walk-forward split builder.

Labels are based on mid-price movement over N ticks:
- mid = (bid_price1 + ask_price1) / 2
- label = {DOWN, FLAT, UP} based on mid[t+N] - mid[t] relative to k * price_tick
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import os
from pathlib import Path
import time
from typing import Any, Literal

import numpy as np
import pandas as pd
import structlog

from ghtrader.data.ticks_schema import DatasetVersion, TicksKind, row_hash_from_ticks_df

log = structlog.get_logger()


from ghtrader.util.hash import hash_csv as _hash_csv, stable_hash_df as _stable_hash_df

# Label classes
LabelClass = Literal["DOWN", "FLAT", "UP"]
LABEL_MAP = {"DOWN": 0, "FLAT": 1, "UP": 2}
LABEL_MAP_INV = {v: k for k, v in LABEL_MAP.items()}


# ---------------------------------------------------------------------------
# Price tick lookup (simplified; in production, fetch from TqSdk quote)
# ---------------------------------------------------------------------------

# Approximate price ticks for SHFE metals (adjust based on contract specs)
PRICE_TICKS = {
    "cu": 10.0,   # Copper: 10 yuan/ton
    "au": 0.02,   # Gold: 0.02 yuan/gram
    "ag": 1.0,    # Silver: 1 yuan/kg
}


def _get_price_tick(symbol: str) -> float:
    """Get price tick for a symbol (simplified lookup)."""
    # Extract product code from symbol like "SHFE.cu2502" or continuous like "KQ.m@SHFE.cu".
    s = str(symbol or "").strip()
    parts = s.split(".")
    code = ""
    if s.startswith("KQ.m@") and len(parts) >= 3:
        # Continuous: last component is variety (e.g. 'cu').
        code = parts[-1]
    elif len(parts) >= 2:
        code = parts[1]

    if code:
        for product, tick in PRICE_TICKS.items():
            if code.startswith(product):
                return tick
    # Default fallback
    return 1.0


# ---------------------------------------------------------------------------
# Label computation
# ---------------------------------------------------------------------------

def compute_mid_price(df: pd.DataFrame) -> pd.Series:
    """Compute mid price from bid/ask."""
    return (df["bid_price1"] + df["ask_price1"]) / 2


def compute_labels_for_horizon(
    mid: pd.Series,
    horizon_n: int,
    threshold_k: int,
    price_tick: float,
) -> pd.Series:
    """
    Compute 3-class labels for a single horizon.
    
    Args:
        mid: Mid price series
        horizon_n: Number of ticks to look ahead
        threshold_k: Threshold in price ticks
        price_tick: Price tick size for the instrument
    
    Returns:
        Series of labels (0=DOWN, 1=FLAT, 2=UP), NaN for last horizon_n rows
    """
    # Future mid price
    future_mid = mid.shift(-horizon_n)
    
    # Price change
    delta = future_mid - mid
    
    # Threshold in price units
    threshold = threshold_k * price_tick
    
    # Classify
    labels = pd.Series(index=mid.index, dtype=float)
    labels[:] = LABEL_MAP["FLAT"]  # Default
    labels[delta >= threshold] = LABEL_MAP["UP"]
    labels[delta <= -threshold] = LABEL_MAP["DOWN"]
    
    # Last horizon_n rows have no label (can't see future)
    labels.iloc[-horizon_n:] = np.nan
    
    return labels


def compute_multi_horizon_labels(
    df: pd.DataFrame,
    horizons: list[int],
    threshold_k: int,
    price_tick: float,
) -> pd.DataFrame:
    """
    Compute labels for multiple horizons.
    
    Args:
        df: Tick DataFrame with bid_price1, ask_price1
        horizons: List of horizon values (N ticks)
        threshold_k: Threshold in price ticks
        price_tick: Price tick size
    
    Returns:
        DataFrame with columns: label_N for each horizon
    """
    mid = compute_mid_price(df)
    
    result = pd.DataFrame(index=df.index)
    result["mid"] = mid
    
    for n in horizons:
        col_name = f"label_{n}"
        result[col_name] = compute_labels_for_horizon(mid, n, threshold_k, price_tick)
    
    return result


# ---------------------------------------------------------------------------
# Build labels for symbol
# ---------------------------------------------------------------------------

def build_labels_for_symbol(
    *,
    symbol: str,
    data_dir: Path,
    horizons: list[int],
    threshold_k: int = 1,
    ticks_kind: TicksKind = "raw",
    overwrite: bool = False,
    dataset_version: DatasetVersion = "v2",
) -> dict[str, Any]:
    """
    Build labels from canonical ticks and store them in QuestDB (`ghtrader_labels_v2`).

    This is the canonical QuestDB-first workflow for label building.

    - Primary storage: QuestDB (QuestDB-first).
    """
    from ghtrader.config import (
        get_questdb_host,
        get_questdb_ilp_port,
        get_questdb_pg_dbname,
        get_questdb_pg_password,
        get_questdb_pg_port,
        get_questdb_pg_user,
    )
    from ghtrader.questdb.features_labels import LABELS_TABLE_V2, ensure_labels_tables, insert_label_build
    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.queries import (
        fetch_ticks_for_symbol_day,
        list_trading_days_for_symbol,
        query_symbol_day_bounds,
    )
    from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

    dv = str(dataset_version).lower().strip() or "v2"
    tk = str(ticks_kind).lower().strip() or "main_l5"
    if tk != "main_l5":
        raise ValueError("ticks_kind raw is deferred (Phase-1/2)")
    hs = [int(h) for h in horizons if int(h) > 0]
    if not hs:
        raise ValueError("horizons must be non-empty")

    # Derived ticks require provenance for roll-boundary-safe lookahead (QuestDB-only; no file-based schedule export).
    schedule_hash: str | None = None

    cfg = make_questdb_query_config_from_env()
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

    ticks_table = "ghtrader_ticks_main_l5_v2"
    bounds = query_symbol_day_bounds(
        cfg=cfg,
        table=ticks_table,
        symbols=[symbol],
        dataset_version=dv,
        ticks_kind=tk,
        l5_only=False,
    )
    b = bounds.get(symbol) or {}
    d0s = str(b.get("first_day") or "").strip()
    d1s = str(b.get("last_day") or "").strip()
    if not d0s or not d1s:
        raise ValueError(f"No tick data found for {symbol} (ticks_kind={ticks_kind}, dataset_version={dataset_version}) in QuestDB")
    start_d = date.fromisoformat(d0s)
    end_d = date.fromisoformat(d1s)
    dates = list_trading_days_for_symbol(
        cfg=cfg,
        table=ticks_table,
        symbol=str(symbol),
        start_day=start_d,
        end_day=end_d,
        dataset_version=dv,
        ticks_kind=tk,
    )
    if not dates:
        raise ValueError(f"No tick data found for {symbol} (ticks_kind={ticks_kind}, dataset_version={dataset_version}) in QuestDB")

    price_tick = float(_get_price_tick(symbol))
    horizons_str = ",".join([str(h) for h in sorted(hs)])
    if tk == "main_l5" and schedule_hash is None:
        # Best-effort: pull schedule_hash from derived tick rows.
        try:
            from ghtrader.questdb.queries import _connect

            with _connect(cfg, connect_timeout_s=2) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT schedule_hash FROM {ticks_table} "
                        "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s LIMIT 1",
                        [str(symbol), str(tk), str(dv)],
                    )
                    r = cur.fetchone()
            if r and r[0] is not None:
                schedule_hash = str(r[0])
        except Exception:
            schedule_hash = None
    if tk == "main_l5" and schedule_hash is None and dates:
        # Fallback: read a single row from the first available day.
        try:
            df0 = fetch_ticks_for_symbol_day(
                cfg=cfg,
                table=ticks_table,
                symbol=symbol,
                trading_day=dates[0].isoformat(),
                dataset_version=dv,
                ticks_kind=tk,
                limit=1,
                order="asc",
                include_provenance=True,
                connect_timeout_s=2,
            )
            if not df0.empty and "schedule_hash" in df0.columns:
                v0 = df0["schedule_hash"].iloc[0]
                if not pd.isna(v0):
                    schedule_hash = str(v0)
        except Exception:
            schedule_hash = None

    build_id = _hash_csv([symbol, tk, dv, str(schedule_hash or ""), horizons_str, str(int(threshold_k)), str(price_tick)])
    # QuestDB ILP expects tz-naive timestamps.
    build_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    ensure_labels_tables(cfg=cfg, horizons=hs, connect_timeout_s=2)

    idx_map = {d: i for i, d in enumerate(dates)}
    max_h = max(hs) if hs else 0
    rows_total = 0
    days_done = 0
    days_total = int(len(dates))
    started_at = time.time()
    last_progress_ts = started_at
    try:
        progress_every_s = float(os.environ.get("GHTRADER_BUILD_PROGRESS_EVERY_S", "30") or "30")
    except Exception:
        progress_every_s = 30.0
    progress_every_s = max(5.0, float(progress_every_s))
    try:
        progress_every_n = int(os.environ.get("GHTRADER_BUILD_PROGRESS_EVERY_N", "5") or "5")
    except Exception:
        progress_every_n = 5
    progress_every_n = max(1, int(progress_every_n))
    log.info(
        "labels.build_start",
        symbol=str(symbol),
        ticks_kind=str(tk),
        dataset_version=str(dv),
        days_total=int(days_total),
        horizons=hs,
        threshold_k=int(threshold_k),
    )

    for dt in dates:
        df_day = fetch_ticks_for_symbol_day(
            cfg=cfg,
            table=ticks_table,
            symbol=symbol,
            trading_day=dt.isoformat(),
            dataset_version=dv,
            ticks_kind=tk,
            limit=None,
            order="asc",
            include_provenance=(tk == "main_l5"),
            connect_timeout_s=2,
        )
        if df_day.empty:
            continue
        df_day = df_day.copy()
        if "row_hash" not in df_day.columns or pd.to_numeric(df_day["row_hash"], errors="coerce").isna().all():
            df_day["row_hash"] = row_hash_from_ticks_df(df_day)
        if tk == "main_l5" and schedule_hash is None and "schedule_hash" in df_day.columns:
            try:
                sh0 = df_day["schedule_hash"].iloc[0]
                if not pd.isna(sh0):
                    schedule_hash = str(sh0)
            except Exception:
                pass

        # Per-day provenance (only for derived ticks)
        u0 = ""
        seg0_opt: int | None = None
        if tk == "main_l5":
            try:
                if "underlying_contract" in df_day.columns and not df_day["underlying_contract"].empty:
                    uval = df_day["underlying_contract"].iloc[0]
                    u0 = "" if pd.isna(uval) else str(uval)
            except Exception:
                u0 = ""
            try:
                if "segment_id" in df_day.columns and not df_day["segment_id"].empty:
                    sval = pd.to_numeric(df_day["segment_id"].iloc[0], errors="coerce")
                    seg0_opt = None if pd.isna(sval) else int(sval)
            except Exception:
                seg0_opt = None
        seg0 = int(seg0_opt or 0)

        df_future = pd.DataFrame()
        i = idx_map.get(dt)
        if max_h > 0 and i is not None and i + 1 < len(dates):
            next_dt = dates[i + 1]
            df_next = fetch_ticks_for_symbol_day(
                cfg=cfg,
                table=ticks_table,
                symbol=symbol,
                trading_day=next_dt.isoformat(),
                dataset_version=dv,
                ticks_kind=tk,
                limit=int(max_h),
                order="asc",
                include_provenance=(tk == "main_l5"),
                connect_timeout_s=2,
            )
            if not df_next.empty:
                allow_cross = True
                if tk == "main_l5":
                    u1 = ""
                    seg1_opt: int | None = None
                    try:
                        if "underlying_contract" in df_next.columns and not df_next["underlying_contract"].empty:
                            uval1 = df_next["underlying_contract"].iloc[0]
                            u1 = "" if pd.isna(uval1) else str(uval1)
                    except Exception:
                        u1 = ""
                    try:
                        if "segment_id" in df_next.columns and not df_next["segment_id"].empty:
                            sval1 = pd.to_numeric(df_next["segment_id"].iloc[0], errors="coerce")
                            seg1_opt = None if pd.isna(sval1) else int(sval1)
                    except Exception:
                        seg1_opt = None

                    allow_cross = bool(seg0_opt is not None and seg1_opt is not None and seg0_opt == seg1_opt)
                    if not allow_cross:
                        allow_cross = bool(u0 and u1 and u0 == u1)

                if allow_cross:
                    df_future = df_next.head(int(max_h)).copy()
                    if "row_hash" not in df_future.columns or pd.to_numeric(df_future["row_hash"], errors="coerce").isna().all():
                        df_future["row_hash"] = row_hash_from_ticks_df(df_future)

        if not df_future.empty:
            df_in = pd.concat([df_day, df_future], ignore_index=True)
        else:
            df_in = df_day

        labels_in = compute_multi_horizon_labels(df_in, hs, int(threshold_k), float(price_tick))
        labels_day = labels_in.iloc[: len(df_day)].reset_index(drop=True)

        datetime_ns = pd.to_numeric(df_day["datetime"], errors="coerce").fillna(0).astype("int64").values
        row_hash = pd.to_numeric(df_day.get("row_hash"), errors="coerce").fillna(0).astype("int64").values

        out = pd.DataFrame(
            {
                "symbol": str(symbol),
                "ts": pd.to_datetime(datetime_ns, unit="ns"),
                "datetime_ns": datetime_ns,
                "trading_day": str(dt.isoformat()),
                "row_hash": row_hash,
                "ticks_kind": str(tk),
                "dataset_version": str(dv),
                "build_id": str(build_id),
                "build_ts": build_ts,
                "horizons": str(horizons_str),
                "threshold_k": int(threshold_k),
                "price_tick": float(price_tick),
                "schedule_hash": str(schedule_hash or ""),
                "mid": pd.to_numeric(labels_day.get("mid"), errors="coerce"),
            }
        )
        if tk == "main_l5":
            out["underlying_contract"] = str(u0 or "")
            out["segment_id"] = int(seg0)
        else:
            out["underlying_contract"] = ""
            out["segment_id"] = 0

        for h in hs:
            out[f"label_{h}"] = pd.to_numeric(labels_day.get(f"label_{h}"), errors="coerce")

        for c in ["symbol", "trading_day", "ticks_kind", "dataset_version", "build_id", "horizons", "schedule_hash", "underlying_contract"]:
            if c in out.columns:
                try:
                    out[c] = out[c].astype("category")
                except Exception:
                    pass

        backend.ingest_df(table=LABELS_TABLE_V2, df=out)

        rows_total += int(len(out))
        days_done += 1
        if (
            days_done == 1
            or days_done == days_total
            or days_done % progress_every_n == 0
            or (time.time() - last_progress_ts) >= progress_every_s
        ):
            log.info(
                "labels.build_progress",
                symbol=str(symbol),
                trading_day=str(dt.isoformat()),
                days_done=int(days_done),
                days_total=int(days_total),
                rows_total=int(rows_total),
                last_rows=int(len(out)),
                elapsed_s=int(time.time() - started_at),
            )
            last_progress_ts = time.time()
        else:
            log.debug(
                "labels.build_day",
                symbol=str(symbol),
                trading_day=str(dt.isoformat()),
                rows=int(len(out)),
                days_done=int(days_done),
                days_total=int(days_total),
            )

    insert_label_build(
        cfg=cfg,
        symbol=str(symbol),
        ticks_kind=str(tk),
        dataset_version=str(dv),
        build_id=str(build_id),
        horizons=str(horizons_str),
        threshold_k=int(threshold_k),
        price_tick=float(price_tick),
        schedule_hash=str(schedule_hash or ""),
        rows_total=int(rows_total),
        first_day=(dates[0].isoformat() if dates else ""),
        last_day=(dates[-1].isoformat() if dates else ""),
        connect_timeout_s=2,
    )
    log.info(
        "labels.build_done",
        symbol=str(symbol),
        ticks_kind=str(tk),
        dataset_version=str(dv),
        rows_total=int(rows_total),
        days=int(days_done),
        elapsed_s=int(time.time() - started_at),
    )

    return {
        "ok": True,
        "symbol": str(symbol),
        "ticks_kind": str(tk),
        "dataset_version": str(dv),
        "build_id": str(build_id),
        "rows_total": int(rows_total),
        "days": int(days_done),
        "schedule_hash": str(schedule_hash or ""),
        "horizons": sorted(hs),
        "threshold_k": int(threshold_k),
        "price_tick": float(price_tick),
    }


def read_labels_for_symbol(data_dir: Path, symbol: str) -> pd.DataFrame:
    """Read labels for a symbol from QuestDB (canonical source)."""
    from ghtrader.questdb.client import connect_pg as _connect, make_questdb_query_config_from_env
    from ghtrader.questdb.features_labels import LABELS_TABLE_V2, get_latest_label_build

    _ = data_dir  # unused; QuestDB is the canonical source

    dv = "v2"
    pref = "main_l5" if str(symbol).startswith("KQ.m@") else "raw"
    alt = "raw" if pref == "main_l5" else "main_l5"
    cfg = make_questdb_query_config_from_env()
    b_pref = get_latest_label_build(cfg=cfg, symbol=symbol, ticks_kind=pref, dataset_version=dv)
    b_alt = get_latest_label_build(cfg=cfg, symbol=symbol, ticks_kind=alt, dataset_version=dv)
    b = b_pref or b_alt
    if not b:
        raise FileNotFoundError(f"Labels not found for {symbol} in QuestDB")

    tk = pref if b_pref else alt
    build_id = str(b.get("build_id") or "").strip()
    if not build_id:
        raise FileNotFoundError(f"QuestDB labels build not found for {symbol}")

    hs_s = str(b.get("horizons") or "").strip()
    hs = [h.strip() for h in hs_s.split(",") if h.strip()]
    label_cols = [f"label_{h}" for h in hs] if hs else []
    cols = ["datetime_ns AS datetime", "underlying_contract", "segment_id", "mid"] + label_cols
    sql = (
        f"SELECT {', '.join(cols)} FROM {LABELS_TABLE_V2} "
        "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s AND build_id=%s "
        "ORDER BY datetime ASC"
    )
    with _connect(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol), str(tk), str(dv), str(build_id)])
            rows = cur.fetchall()
    if not rows:
        raise FileNotFoundError(f"No QuestDB labels rows for {symbol}")

    df = pd.DataFrame(rows, columns=["datetime", "underlying_contract", "segment_id", "mid"] + label_cols)
    df.insert(0, "symbol", str(symbol))
    df["datetime"] = pd.to_numeric(df["datetime"], errors="coerce").fillna(0).astype("int64")
    if "segment_id" in df.columns:
        df["segment_id"] = pd.to_numeric(df["segment_id"], errors="coerce").fillna(0).astype("int64")
    df["mid"] = pd.to_numeric(df["mid"], errors="coerce")
    for c in label_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("datetime").reset_index(drop=True)


def read_labels_manifest(data_dir: Path, symbol: str) -> dict[str, Any]:
    """Read the labels manifest for a symbol from QuestDB (canonical)."""
    _ = data_dir  # unused; QuestDB is the canonical source

    # QuestDB-first: read the most recent build record for this symbol.
    try:
        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.features_labels import LABELS_TABLE_V2, LABEL_BUILDS_TABLE_V2, get_latest_label_build

        dv = "v2"
        pref = "main_l5" if str(symbol).startswith("KQ.m@") else "raw"
        alt = "raw" if pref == "main_l5" else "main_l5"
        cfg = make_questdb_query_config_from_env()
        b_pref = get_latest_label_build(cfg=cfg, symbol=symbol, ticks_kind=pref, dataset_version=dv)
        b_alt = get_latest_label_build(cfg=cfg, symbol=symbol, ticks_kind=alt, dataset_version=dv)
        b = b_pref or b_alt
        if not b:
            return {}
        tk = pref if b_pref else alt

        hs_s = str(b.get("horizons") or "").strip()
        hs = [int(x) for x in hs_s.split(",") if str(x).strip().isdigit()] if hs_s else []

        m: dict[str, Any] = {
            "created_at": str(b.get("ts") or ""),
            "dataset": "labels",
            "symbol": str(symbol),
            "ticks_kind": str(tk),
            "dataset_version": str(dv),
            "horizons": hs,
            "threshold_k": b.get("threshold_k"),
            "schema_hash": "",
            "questdb": {
                "table": LABELS_TABLE_V2,
                "builds_table": LABEL_BUILDS_TABLE_V2,
                "build_id": str(b.get("build_id") or ""),
            },
        }
        if str(tk) == "main_l5":
            m["schedule"] = {
                "hash": str(b.get("schedule_hash") or ""),
                "l5_start_date": str(b.get("first_day") or ""),
                "end_date": str(b.get("last_day") or ""),
            }
        return m
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Walk-forward splits
# ---------------------------------------------------------------------------

@dataclass
class WalkForwardSplit:
    """A single train/val/test split for walk-forward evaluation."""
    
    split_id: int
    train_start_idx: int
    train_end_idx: int
    val_start_idx: int
    val_end_idx: int
    test_start_idx: int
    test_end_idx: int


def create_walk_forward_splits(
    n_samples: int,
    n_splits: int = 5,
    train_ratio: float = 0.6,
    val_ratio: float = 0.2,
    min_train_samples: int = 10000,
) -> list[WalkForwardSplit]:
    """
    Create walk-forward (expanding or rolling) splits.
    
    Each split uses earlier data for training and later data for validation/test.
    No shuffling across time (preserves temporal ordering).
    
    Args:
        n_samples: Total number of samples
        n_splits: Number of splits to create
        train_ratio: Fraction of available data for training in each split
        val_ratio: Fraction for validation
        min_train_samples: Minimum samples required for training
    
    Returns:
        List of WalkForwardSplit objects
    """
    test_ratio = 1.0 - train_ratio - val_ratio
    
    splits: list[WalkForwardSplit] = []
    
    # Calculate step size
    step_size = n_samples // (n_splits + 2)  # Reserve space for expanding window
    
    for i in range(n_splits):
        # Expanding window: each split uses more history
        split_end = min(n_samples, (i + 3) * step_size)
        window_size = split_end
        
        train_size = int(window_size * train_ratio)
        val_size = int(window_size * val_ratio)
        test_size = window_size - train_size - val_size
        
        if train_size < min_train_samples:
            continue
        
        train_start = 0
        train_end = train_size
        val_start = train_end
        val_end = val_start + val_size
        test_start = val_end
        test_end = min(split_end, test_start + test_size)
        
        if test_end <= test_start:
            continue
        
        splits.append(WalkForwardSplit(
            split_id=i,
            train_start_idx=train_start,
            train_end_idx=train_end,
            val_start_idx=val_start,
            val_end_idx=val_end,
            test_start_idx=test_start,
            test_end_idx=test_end,
        ))
    
    return splits


def apply_split(df: pd.DataFrame, split: WalkForwardSplit) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply a walk-forward split to a DataFrame."""
    train = df.iloc[split.train_start_idx:split.train_end_idx].copy()
    val = df.iloc[split.val_start_idx:split.val_end_idx].copy()
    test = df.iloc[split.test_start_idx:split.test_end_idx].copy()
    return train, val, test
