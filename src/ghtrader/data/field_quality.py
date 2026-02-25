from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from ghtrader.data.ticks_schema import L5_BOOK_COLS, null_rate as _null_rate
from ghtrader.questdb.field_quality import FieldQualityRow
from ghtrader.questdb.queries import fetch_ticks_for_day


@dataclass(frozen=True)
class FieldQualityResult:
    row: FieldQualityRow
    details: dict[str, Any]


def _positive_mask(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return (s > 0) & (~s.isna())


def _numeric_column_or_nan(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")


def compute_orderbook_invariants(df: pd.DataFrame) -> dict[str, Any]:
    rows_total = int(len(df))
    if rows_total <= 0:
        return {
            "rows_total": 0,
            "bid_ask_cross_violations": 0,
            "bid_ask_cross_rate": 0.0,
            "bid_order_violations": 0,
            "bid_order_rate": 0.0,
            "ask_order_violations": 0,
            "ask_order_rate": 0.0,
            "negative_volume_violations": 0,
            "negative_volume_rate": 0.0,
            "negative_price_violations": 0,
            "negative_price_rate": 0.0,
        }

    bid1 = _numeric_column_or_nan(df, "bid_price1")
    ask1 = _numeric_column_or_nan(df, "ask_price1")
    bid1_ok = _positive_mask(bid1)
    ask1_ok = _positive_mask(ask1)
    spread_applicable = bid1_ok & ask1_ok
    bid_ask_cross = (bid1 > ask1) & spread_applicable
    bid_ask_cross_violations = int(bid_ask_cross.sum())
    bid_ask_cross_rate = float(bid_ask_cross_violations) / float(spread_applicable.sum() or 1)

    bid_order_violation = pd.Series([False] * rows_total, index=df.index)
    bid_applicable = pd.Series([False] * rows_total, index=df.index)
    ask_order_violation = pd.Series([False] * rows_total, index=df.index)
    ask_applicable = pd.Series([False] * rows_total, index=df.index)

    for i in range(1, 5):
        b1 = _numeric_column_or_nan(df, f"bid_price{i}")
        b2 = _numeric_column_or_nan(df, f"bid_price{i+1}")
        bmask = _positive_mask(b1) & _positive_mask(b2)
        bid_applicable |= bmask
        bid_order_violation |= (b1 < b2) & bmask

        a1 = _numeric_column_or_nan(df, f"ask_price{i}")
        a2 = _numeric_column_or_nan(df, f"ask_price{i+1}")
        amask = _positive_mask(a1) & _positive_mask(a2)
        ask_applicable |= amask
        ask_order_violation |= (a1 > a2) & amask

    bid_order_violations = int(bid_order_violation.sum())
    bid_order_rate = float(bid_order_violations) / float(bid_applicable.sum() or 1)
    ask_order_violations = int(ask_order_violation.sum())
    ask_order_rate = float(ask_order_violations) / float(ask_applicable.sum() or 1)

    volume_cols = [c for c in L5_BOOK_COLS if "volume" in c] + ["volume"]
    volume_cols = list(dict.fromkeys(volume_cols))
    vol_df = df.reindex(columns=volume_cols)
    vol_vals = pd.to_numeric(vol_df.stack(dropna=False), errors="coerce")
    negative_volume_violations = int((vol_vals < 0).sum())
    negative_volume_rate = float(negative_volume_violations) / float(len(vol_vals) or 1)

    price_cols = [c for c in L5_BOOK_COLS if "price" in c] + ["last_price", "bid_price1", "ask_price1"]
    price_cols = list(dict.fromkeys(price_cols))
    price_df = df.reindex(columns=price_cols)
    price_vals = pd.to_numeric(price_df.stack(dropna=False), errors="coerce")
    negative_price_violations = int((price_vals < 0).sum())
    negative_price_rate = float(negative_price_violations) / float(len(price_vals) or 1)

    return {
        "rows_total": rows_total,
        "bid_ask_cross_violations": bid_ask_cross_violations,
        "bid_ask_cross_rate": bid_ask_cross_rate,
        "bid_order_violations": bid_order_violations,
        "bid_order_rate": bid_order_rate,
        "ask_order_violations": ask_order_violations,
        "ask_order_rate": ask_order_rate,
        "negative_volume_violations": negative_volume_violations,
        "negative_volume_rate": negative_volume_rate,
        "negative_price_violations": negative_price_violations,
        "negative_price_rate": negative_price_rate,
    }


def compute_field_quality_for_day(
    *,
    cfg: Any,
    symbol: str,
    trading_day: date,
    table: str = "ghtrader_ticks_main_l5_v2",
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    limit: int | None = None,
) -> FieldQualityResult:
    cols = [
        "symbol",
        "datetime_ns as datetime",
        "last_price",
        "volume",
        "open_interest",
        "bid_price1",
        "ask_price1",
    ] + list(L5_BOOK_COLS)
    cols = list(dict.fromkeys(cols))
    df = fetch_ticks_for_day(
        cfg=cfg,
        symbol=symbol,
        trading_day=trading_day,
        table=table,
        columns=cols,
        dataset_version=dataset_version,
        ticks_kind=ticks_kind,
        limit=limit,
    )

    rows_total = int(len(df))
    nulls = {
        "bid_price1_null_rate": _null_rate(df.get("bid_price1", pd.Series(dtype=float)), rows_total),
        "ask_price1_null_rate": _null_rate(df.get("ask_price1", pd.Series(dtype=float)), rows_total),
        "last_price_null_rate": _null_rate(df.get("last_price", pd.Series(dtype=float)), rows_total),
        "volume_null_rate": _null_rate(df.get("volume", pd.Series(dtype=float)), rows_total),
        "open_interest_null_rate": _null_rate(df.get("open_interest", pd.Series(dtype=float)), rows_total),
    }
    l5_cols = [c for c in L5_BOOK_COLS if c in df.columns]
    if rows_total > 0 and l5_cols:
        l5_nulls = df[l5_cols].isna().sum().sum()
        l5_fields_null_rate = float(l5_nulls) / float(rows_total * len(l5_cols))
    else:
        l5_fields_null_rate = 0.0

    invariants = compute_orderbook_invariants(df)

    row = FieldQualityRow(
        symbol=str(symbol),
        trading_day=trading_day.isoformat(),
        ticks_kind=str(ticks_kind),
        dataset_version=str(dataset_version),
        rows_total=rows_total,
        bid_price1_null_rate=float(nulls["bid_price1_null_rate"]),
        ask_price1_null_rate=float(nulls["ask_price1_null_rate"]),
        last_price_null_rate=float(nulls["last_price_null_rate"]),
        volume_null_rate=float(nulls["volume_null_rate"]),
        open_interest_null_rate=float(nulls["open_interest_null_rate"]),
        l5_fields_null_rate=float(l5_fields_null_rate),
        price_jump_outliers=0,
        volume_spike_outliers=0,
        spread_anomaly_outliers=0,
        bid_ask_cross_violations=int(invariants["bid_ask_cross_violations"]),
        bid_ask_cross_rate=float(invariants["bid_ask_cross_rate"]),
        bid_order_violations=int(invariants["bid_order_violations"]),
        bid_order_rate=float(invariants["bid_order_rate"]),
        ask_order_violations=int(invariants["ask_order_violations"]),
        ask_order_rate=float(invariants["ask_order_rate"]),
        negative_volume_violations=int(invariants["negative_volume_violations"]),
        negative_volume_rate=float(invariants["negative_volume_rate"]),
        negative_price_violations=int(invariants["negative_price_violations"]),
        negative_price_rate=float(invariants["negative_price_rate"]),
    )
    return FieldQualityResult(row=row, details={"rows_total": rows_total})


def list_symbol_trading_days(
    *,
    cfg: Any,
    symbol: str,
    table: str = "ghtrader_ticks_main_l5_v2",
    dataset_version: str = "v2",
    ticks_kind: str = "main_l5",
    start_day: date | None = None,
    end_day: date | None = None,
) -> list[date]:
    from ghtrader.questdb.client import connect_pg

    where = ["symbol=%s", "ticks_kind=%s", "dataset_version=%s"]
    params: list[Any] = [str(symbol), str(ticks_kind), str(dataset_version)]
    if start_day is not None:
        where.append("trading_day >= %s")
        params.append(start_day.isoformat())
    if end_day is not None:
        where.append("trading_day <= %s")
        params.append(end_day.isoformat())
    sql = (
        f"SELECT DISTINCT trading_day FROM {table} "
        f"WHERE {' AND '.join(where)} ORDER BY trading_day"
    )
    out: list[date] = []
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    out.append(date.fromisoformat(str(row[0])))
                except Exception:
                    continue
    return out
