from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable

import structlog

from .client import QuestDBQueryConfig, connect_pg as _connect

log = structlog.get_logger()

FEATURES_TABLE_V2 = "ghtrader_features_v2"
FEATURE_BUILDS_TABLE_V2 = "ghtrader_feature_builds_v2"
LABELS_TABLE_V2 = "ghtrader_labels_v2"
LABEL_BUILDS_TABLE_V2 = "ghtrader_label_builds_v2"

_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str) -> str:
    n = str(name or "").strip()
    if not n or not _SQL_IDENT_RE.match(n):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return n


def ensure_features_tables(
    *,
    cfg: QuestDBQueryConfig,
    factor_columns: Iterable[str],
    features_table: str = FEATURES_TABLE_V2,
    builds_table: str = FEATURE_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    """
    Ensure QuestDB tables for features exist (data + build metadata), and evolve schema.
    """
    ft = _ident(str(features_table).strip() or FEATURES_TABLE_V2)
    bt = _ident(str(builds_table).strip() or FEATURE_BUILDS_TABLE_V2)
    factors = [_ident(c) for c in factor_columns if str(c).strip()]

    # Data table (wide schema; one row per tick).
    ddl_data = f"""
    CREATE TABLE IF NOT EXISTS {ft} (
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
      segment_id LONG
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, build_id, row_hash)
    """

    # Build metadata table (small; one row per build).
    ddl_meta = f"""
    CREATE TABLE IF NOT EXISTS {bt} (
      ts TIMESTAMP,
      symbol SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      build_id SYMBOL,
      factors_hash SYMBOL,
      factors STRING,
      schema_hash SYMBOL,
      schedule_hash SYMBOL,
      rows_total LONG,
      first_day SYMBOL,
      last_day SYMBOL
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
    """

    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        # DDL/schema evolution: use autocommit so per-statement failures don't
        # abort the whole transaction (psycopg behavior).
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            cur.execute(ddl_data)
            cur.execute(ddl_meta)

            # Best-effort schema evolution (ignore failures).
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
            ]
            # Best-effort column rename migration (v2): tolerate older column names.
            from .migrate import best_effort_rename_provenance_columns_v2

            for tbl in [ft, bt]:
                best_effort_rename_provenance_columns_v2(cur=cur, table=str(tbl))
            for name, typ in base_cols:
                try:
                    cur.execute(f"ALTER TABLE {ft} ADD COLUMN {name} {typ}")
                except Exception:
                    pass
            for c in factors:
                try:
                    cur.execute(f"ALTER TABLE {ft} ADD COLUMN {c} DOUBLE")
                except Exception:
                    pass

            # Ensure dedup is enabled for idempotent rebuilds.
            try:
                cur.execute(
                    f"ALTER TABLE {ft} DEDUP ENABLE UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, build_id, row_hash)"
                )
            except Exception:
                pass


def ensure_labels_tables(
    *,
    cfg: QuestDBQueryConfig,
    horizons: Iterable[int],
    labels_table: str = LABELS_TABLE_V2,
    builds_table: str = LABEL_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    """
    Ensure QuestDB tables for labels exist (data + build metadata), and evolve schema.
    """
    lt = _ident(str(labels_table).strip() or LABELS_TABLE_V2)
    bt = _ident(str(builds_table).strip() or LABEL_BUILDS_TABLE_V2)
    hs = sorted({int(h) for h in horizons if int(h) > 0})
    label_cols = [_ident(f"label_{h}") for h in hs]

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
      horizons STRING,
      threshold_k LONG,
      price_tick DOUBLE,
      schedule_hash SYMBOL,
      underlying_contract SYMBOL,
      segment_id LONG,
      mid DOUBLE
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
      DEDUP UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, build_id, row_hash)
    """

    ddl_meta = f"""
    CREATE TABLE IF NOT EXISTS {bt} (
      ts TIMESTAMP,
      symbol SYMBOL,
      ticks_kind SYMBOL,
      dataset_version SYMBOL,
      build_id SYMBOL,
      horizons STRING,
      threshold_k LONG,
      price_tick DOUBLE,
      schedule_hash SYMBOL,
      rows_total LONG,
      first_day SYMBOL,
      last_day SYMBOL
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
    """

    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        # DDL/schema evolution: use autocommit so per-statement failures don't
        # abort the whole transaction (psycopg behavior).
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
                ("horizons", "STRING"),
                ("threshold_k", "LONG"),
                ("price_tick", "DOUBLE"),
                ("schedule_hash", "SYMBOL"),
                ("underlying_contract", "SYMBOL"),
                ("segment_id", "LONG"),
                ("mid", "DOUBLE"),
            ]
            # Best-effort column rename migration (v2): tolerate older column names.
            from .migrate import best_effort_rename_provenance_columns_v2

            for tbl in [lt, bt]:
                best_effort_rename_provenance_columns_v2(cur=cur, table=str(tbl))
            for name, typ in base_cols:
                try:
                    cur.execute(f"ALTER TABLE {lt} ADD COLUMN {name} {typ}")
                except Exception:
                    pass
            for c in label_cols:
                try:
                    cur.execute(f"ALTER TABLE {lt} ADD COLUMN {c} DOUBLE")
                except Exception:
                    pass

            try:
                cur.execute(
                    f"ALTER TABLE {lt} DEDUP ENABLE UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, build_id, row_hash)"
                )
            except Exception:
                pass


def insert_feature_build(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    ticks_kind: str,
    dataset_version: str,
    build_id: str,
    factors_hash: str,
    factors: str,
    schema_hash: str,
    schedule_hash: str | None,
    rows_total: int,
    first_day: str | None,
    last_day: str | None,
    builds_table: str = FEATURE_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    bt = _ident(str(builds_table).strip() or FEATURE_BUILDS_TABLE_V2)
    now = datetime.now(timezone.utc)
    sql = (
        f"INSERT INTO {bt} "
        "(ts, symbol, ticks_kind, dataset_version, build_id, factors_hash, factors, schema_hash, schedule_hash, rows_total, first_day, last_day) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    params = (
        now,
        str(symbol),
        str(ticks_kind),
        str(dataset_version),
        str(build_id),
        str(factors_hash),
        str(factors),
        str(schema_hash),
        str(schedule_hash or ""),
        int(rows_total),
        str(first_day or ""),
        str(last_day or ""),
    )
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def insert_label_build(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    ticks_kind: str,
    dataset_version: str,
    build_id: str,
    horizons: str,
    threshold_k: int,
    price_tick: float,
    schedule_hash: str | None,
    rows_total: int,
    first_day: str | None,
    last_day: str | None,
    builds_table: str = LABEL_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    bt = _ident(str(builds_table).strip() or LABEL_BUILDS_TABLE_V2)
    now = datetime.now(timezone.utc)
    sql = (
        f"INSERT INTO {bt} "
        "(ts, symbol, ticks_kind, dataset_version, build_id, horizons, threshold_k, price_tick, schedule_hash, rows_total, first_day, last_day) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    params = (
        now,
        str(symbol),
        str(ticks_kind),
        str(dataset_version),
        str(build_id),
        str(horizons),
        int(threshold_k),
        float(price_tick),
        str(schedule_hash or ""),
        int(rows_total),
        str(first_day or ""),
        str(last_day or ""),
    )
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def get_latest_feature_build(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    ticks_kind: str,
    dataset_version: str,
    builds_table: str = FEATURE_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, Any] | None:
    bt = _ident(str(builds_table).strip() or FEATURE_BUILDS_TABLE_V2)
    sql = (
        f"SELECT ts, build_id, factors_hash, factors, schema_hash, schedule_hash, rows_total, first_day, last_day "
        f"FROM {bt} WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s "
        "ORDER BY ts DESC LIMIT 1"
    )
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol), str(ticks_kind), str(dataset_version)])
            row = cur.fetchone()
            if not row:
                return None
            return {
                "ts": str(row[0]) if row[0] is not None else "",
                "build_id": str(row[1] or ""),
                "factors_hash": str(row[2] or ""),
                "factors": str(row[3] or ""),
                "schema_hash": str(row[4] or ""),
                "schedule_hash": str(row[5] or ""),
                "rows_total": int(row[6]) if row[6] is not None else None,
                "first_day": str(row[7] or ""),
                "last_day": str(row[8] or ""),
            }


def get_latest_label_build(
    *,
    cfg: QuestDBQueryConfig,
    symbol: str,
    ticks_kind: str,
    dataset_version: str,
    builds_table: str = LABEL_BUILDS_TABLE_V2,
    connect_timeout_s: int = 2,
) -> dict[str, Any] | None:
    bt = _ident(str(builds_table).strip() or LABEL_BUILDS_TABLE_V2)
    sql = (
        f"SELECT ts, build_id, horizons, threshold_k, price_tick, schedule_hash, rows_total, first_day, last_day "
        f"FROM {bt} WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s "
        "ORDER BY ts DESC LIMIT 1"
    )
    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol), str(ticks_kind), str(dataset_version)])
            row = cur.fetchone()
            if not row:
                return None
            return {
                "ts": str(row[0]) if row[0] is not None else "",
                "build_id": str(row[1] or ""),
                "horizons": str(row[2] or ""),
                "threshold_k": int(row[3]) if row[3] is not None else None,
                "price_tick": float(row[4]) if row[4] is not None else None,
                "schedule_hash": str(row[5] or ""),
                "rows_total": int(row[6]) if row[6] is not None else None,
                "first_day": str(row[7] or ""),
                "last_day": str(row[8] or ""),
            }


# ---------------------------------------------------------------------------
# Feature Registry Table (PRD 5.4.0.4)
# ---------------------------------------------------------------------------

FEATURE_REGISTRY_TABLE_V2 = "ghtrader_feature_registry_v2"


def ensure_feature_registry_table(
    *,
    cfg: QuestDBQueryConfig,
    registry_table: str = FEATURE_REGISTRY_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    """
    Ensure QuestDB table for feature registry exists.

    The registry table stores feature definitions with versioning and lineage.
    """
    rt = _ident(str(registry_table).strip() or FEATURE_REGISTRY_TABLE_V2)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {rt} (
      ts TIMESTAMP,
      feature_name SYMBOL,
      version LONG,
      definition_hash SYMBOL,
      input_columns STRING,
      output_dtype SYMBOL,
      lookback_ticks LONG,
      ttl_seconds LONG,
      deprecated_at TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY DAY WAL
    """

    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

            # Best-effort schema evolution
            cols = [
                ("input_columns", "STRING"),
                ("output_dtype", "SYMBOL"),
                ("lookback_ticks", "LONG"),
                ("ttl_seconds", "LONG"),
                ("deprecated_at", "TIMESTAMP"),
            ]
            for name, typ in cols:
                try:
                    cur.execute(f"ALTER TABLE {rt} ADD COLUMN {name} {typ}")
                except Exception:
                    pass


def sync_feature_registry(
    *,
    cfg: QuestDBQueryConfig,
    features: list,
    registry_table: str = FEATURE_REGISTRY_TABLE_V2,
    connect_timeout_s: int = 2,
) -> None:
    """
    Sync feature definitions to QuestDB registry table.

    Args:
        cfg: QuestDB config
        features: List of FeatureDefinition objects
        registry_table: Target table name
        connect_timeout_s: Connection timeout
    """
    import json

    rt = _ident(str(registry_table).strip() or FEATURE_REGISTRY_TABLE_V2)

    # Ensure table exists
    ensure_feature_registry_table(cfg=cfg, registry_table=registry_table, connect_timeout_s=connect_timeout_s)

    if not features:
        return

    sql = (
        f"INSERT INTO {rt} "
        "(ts, feature_name, version, definition_hash, input_columns, output_dtype, lookback_ticks, ttl_seconds, deprecated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            for feat in features:
                # Handle both dict and dataclass
                if hasattr(feat, "to_dict"):
                    fd = feat.to_dict()
                else:
                    fd = feat if isinstance(feat, dict) else vars(feat)

                input_cols = fd.get("input_columns", [])
                if isinstance(input_cols, (list, tuple)):
                    input_cols_str = json.dumps(list(input_cols))
                else:
                    input_cols_str = str(input_cols)

                deprecated_at = fd.get("deprecated_at")
                if isinstance(deprecated_at, str) and deprecated_at:
                    deprecated_at = datetime.fromisoformat(deprecated_at)

                created_at = fd.get("created_at")
                if isinstance(created_at, str) and created_at:
                    created_at = datetime.fromisoformat(created_at)
                elif created_at is None:
                    created_at = datetime.now(timezone.utc)

                params = (
                    created_at,
                    str(fd.get("name", "")),
                    int(fd.get("version", 1)),
                    str(fd.get("definition_hash", "")),
                    input_cols_str,
                    str(fd.get("output_dtype", "float64")),
                    int(fd.get("lookback_ticks", 0)),
                    int(fd.get("ttl_seconds", 1800)),
                    deprecated_at,
                )
                try:
                    cur.execute(sql, params)
                except Exception as e:
                    log.warning("feature_registry.sync_failed", feature=fd.get("name"), error=str(e))


def get_feature_registry_from_questdb(
    *,
    cfg: QuestDBQueryConfig,
    registry_table: str = FEATURE_REGISTRY_TABLE_V2,
    active_only: bool = True,
    connect_timeout_s: int = 2,
) -> list[dict[str, Any]]:
    """
    Load feature definitions from QuestDB registry.

    Args:
        cfg: QuestDB config
        registry_table: Source table name
        active_only: Only return non-deprecated features
        connect_timeout_s: Connection timeout

    Returns:
        List of feature definition dicts
    """
    import json

    rt = _ident(str(registry_table).strip() or FEATURE_REGISTRY_TABLE_V2)

    sql = (
        f"SELECT ts, feature_name, version, definition_hash, input_columns, output_dtype, lookback_ticks, ttl_seconds, deprecated_at "
        f"FROM {rt} "
    )
    if active_only:
        sql += "WHERE deprecated_at IS NULL "
    sql += "ORDER BY feature_name, version"

    result = []
    try:
        with _connect(cfg, connect_timeout_s=connect_timeout_s) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    input_cols = []
                    if row[4]:
                        try:
                            input_cols = json.loads(row[4])
                        except Exception:
                            input_cols = []

                    result.append(
                        {
                            "created_at": str(row[0]) if row[0] else None,
                            "name": str(row[1] or ""),
                            "version": int(row[2]) if row[2] is not None else 1,
                            "definition_hash": str(row[3] or ""),
                            "input_columns": input_cols,
                            "output_dtype": str(row[5] or "float64"),
                            "lookback_ticks": int(row[6]) if row[6] is not None else 0,
                            "ttl_seconds": int(row[7]) if row[7] is not None else 1800,
                            "deprecated_at": str(row[8]) if row[8] else None,
                        }
                    )
    except Exception as e:
        log.warning("feature_registry.load_failed", error=str(e))

    return result

