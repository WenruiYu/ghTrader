from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import structlog

from .client import QuestDBQueryConfig, connect_pg

log = structlog.get_logger()

FIELD_QUALITY_TABLE = "ghtrader_field_quality_v2"


def _day_to_ts_utc(day_s: str) -> datetime:
    d = date.fromisoformat(str(day_s).strip())
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


@dataclass(frozen=True)
class FieldQualityRow:
    symbol: str
    trading_day: str
    ticks_kind: str
    dataset_version: str
    rows_total: int
    bid_price1_null_rate: float
    ask_price1_null_rate: float
    last_price_null_rate: float
    volume_null_rate: float
    open_interest_null_rate: float
    l5_fields_null_rate: float
    price_jump_outliers: int
    volume_spike_outliers: int
    spread_anomaly_outliers: int
    bid_ask_cross_violations: int
    bid_ask_cross_rate: float
    bid_order_violations: int
    bid_order_rate: float
    ask_order_violations: int
    ask_order_rate: float
    negative_volume_violations: int
    negative_volume_rate: float
    negative_price_violations: int
    negative_price_rate: float


def ensure_field_quality_table(
    *,
    cfg: QuestDBQueryConfig,
    table: str = FIELD_QUALITY_TABLE,
    connect_timeout_s: int = 2,
) -> None:
    tbl = str(table).strip() or FIELD_QUALITY_TABLE
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {tbl} (
      ts TIMESTAMP,
      symbol SYMBOL,
      trading_day SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      rows_total LONG,
      bid_price1_null_rate DOUBLE,
      ask_price1_null_rate DOUBLE,
      last_price_null_rate DOUBLE,
      volume_null_rate DOUBLE,
      open_interest_null_rate DOUBLE,
      l5_fields_null_rate DOUBLE,
      price_jump_outliers LONG,
      volume_spike_outliers LONG,
      spread_anomaly_outliers LONG,
      bid_ask_cross_violations LONG,
      bid_ask_cross_rate DOUBLE,
      bid_order_violations LONG,
      bid_order_rate DOUBLE,
      ask_order_violations LONG,
      ask_order_rate DOUBLE,
      negative_volume_violations LONG,
      negative_volume_rate DOUBLE,
      negative_price_violations LONG,
      negative_price_rate DOUBLE,
      updated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)
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
                    ("ticks_kind", "SYMBOL"),
                    ("dataset_version", "SYMBOL"),
                    ("rows_total", "LONG"),
                    ("bid_price1_null_rate", "DOUBLE"),
                    ("ask_price1_null_rate", "DOUBLE"),
                    ("last_price_null_rate", "DOUBLE"),
                    ("volume_null_rate", "DOUBLE"),
                    ("open_interest_null_rate", "DOUBLE"),
                    ("l5_fields_null_rate", "DOUBLE"),
                    ("price_jump_outliers", "LONG"),
                    ("volume_spike_outliers", "LONG"),
                    ("spread_anomaly_outliers", "LONG"),
                    ("bid_ask_cross_violations", "LONG"),
                    ("bid_ask_cross_rate", "DOUBLE"),
                    ("bid_order_violations", "LONG"),
                    ("bid_order_rate", "DOUBLE"),
                    ("ask_order_violations", "LONG"),
                    ("ask_order_rate", "DOUBLE"),
                    ("negative_volume_violations", "LONG"),
                    ("negative_volume_rate", "DOUBLE"),
                    ("negative_price_violations", "LONG"),
                    ("negative_price_rate", "DOUBLE"),
                    ("updated_at", "TIMESTAMP"),
                ]:
                    try:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {name} {typ}")
                    except Exception:
                        pass
                try:
                    cur.execute(
                        f"ALTER TABLE {tbl} DEDUP ENABLE UPSERT KEYS(ts, symbol, trading_day, ticks_kind, dataset_version)"
                    )
                except Exception:
                    pass
    except Exception as e:
        log.warning("questdb_field_quality.ensure_failed", table=tbl, error=str(e))


def upsert_field_quality_rows(
    *,
    cfg: QuestDBQueryConfig,
    rows: Iterable[FieldQualityRow],
    table: str = FIELD_QUALITY_TABLE,
    connect_timeout_s: int = 2,
) -> int:
    tbl = str(table).strip() or FIELD_QUALITY_TABLE
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
                str(r.ticks_kind).strip(),
                str(r.dataset_version).strip(),
                int(r.rows_total),
                float(r.bid_price1_null_rate),
                float(r.ask_price1_null_rate),
                float(r.last_price_null_rate),
                float(r.volume_null_rate),
                float(r.open_interest_null_rate),
                float(r.l5_fields_null_rate),
                int(r.price_jump_outliers),
                int(r.volume_spike_outliers),
                int(r.spread_anomaly_outliers),
                int(r.bid_ask_cross_violations),
                float(r.bid_ask_cross_rate),
                int(r.bid_order_violations),
                float(r.bid_order_rate),
                int(r.ask_order_violations),
                float(r.ask_order_rate),
                int(r.negative_volume_violations),
                float(r.negative_volume_rate),
                int(r.negative_price_violations),
                float(r.negative_price_rate),
                now,
            )
        )
    if not params:
        return 0
    sql = (
        f"INSERT INTO {tbl} "
        "(ts, symbol, trading_day, ticks_kind, dataset_version, rows_total, "
        "bid_price1_null_rate, ask_price1_null_rate, last_price_null_rate, volume_null_rate, "
        "open_interest_null_rate, l5_fields_null_rate, price_jump_outliers, volume_spike_outliers, "
        "spread_anomaly_outliers, bid_ask_cross_violations, bid_ask_cross_rate, bid_order_violations, "
        "bid_order_rate, ask_order_violations, ask_order_rate, negative_volume_violations, "
        "negative_volume_rate, negative_price_violations, negative_price_rate, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
    return int(len(params))
