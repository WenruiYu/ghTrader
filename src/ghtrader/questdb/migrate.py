from __future__ import annotations

"""
QuestDB schema migration helpers.

This module provides **idempotent** migrations that can be run multiple times.
It is intentionally defensive: when a table/column does not exist (or is already migrated),
the migration skips it and records the outcome.

Current migration:
- Rename `ticks_lake` -> `ticks_kind`
- Rename `lake_version` -> `dataset_version`
"""

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .client import QuestDBQueryConfig, connect_pg

_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


from ghtrader.util.time import now_iso as _now_iso


def _ident(name: str) -> str:
    n = str(name or "").strip()
    if not n or not _SQL_IDENT_RE.match(n):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return n


@dataclass(frozen=True)
class RenameOp:
    table: str
    old: str
    new: str

    def sql(self) -> str:
        return f"ALTER TABLE {_ident(self.table)} RENAME COLUMN {_ident(self.old)} TO {_ident(self.new)}"


@dataclass(frozen=True)
class OpResult:
    ok: bool
    table: str
    action: str
    sql: str
    error: str | None = None


def _list_tables(*, cfg: QuestDBQueryConfig, connect_timeout_s: int = 2) -> list[str]:
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM tables()")
            rows = cur.fetchall()
    out: list[str] = []
    for (name,) in rows:
        try:
            s = str(name or "").strip()
        except Exception:
            s = ""
        if s:
            out.append(s)
    return out


def _list_columns(*, cfg: QuestDBQueryConfig, table: str, connect_timeout_s: int = 2) -> list[str]:
    t = _ident(table)
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            # "column" is a SQL keyword in QuestDB and must be double-quoted.
            cur.execute(f'SELECT "column" FROM table_columns(\'{t}\')')
            rows = cur.fetchall()
    out: list[str] = []
    for (name,) in rows:
        try:
            s = str(name or "").strip()
        except Exception:
            s = ""
        if s:
            out.append(s)
    return out


def _apply_sql(
    *,
    cfg: QuestDBQueryConfig,
    statements: Iterable[str],
    connect_timeout_s: int = 2,
) -> list[OpResult]:
    out: list[OpResult] = []
    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        # Each statement should be resilient to per-statement failures.
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            for sql in statements:
                try:
                    cur.execute(str(sql))
                    out.append(OpResult(ok=True, table="", action="exec", sql=str(sql)))
                except Exception as e:
                    out.append(OpResult(ok=False, table="", action="exec", sql=str(sql), error=str(e)))
    return out


def migrate_column_names_v2(
    *,
    cfg: QuestDBQueryConfig,
    table_prefix: str = "ghtrader_",
    apply: bool = False,
    connect_timeout_s: int = 2,
) -> dict[str, Any]:
    """
    Rename provenance columns across ghTrader QuestDB tables:
    - ticks_lake -> ticks_kind
    - lake_version -> dataset_version

    If `apply=False` (default): returns a plan (dry-run).
    If `apply=True`: executes the ALTER TABLE statements.
    """
    prefix = str(table_prefix or "").strip()
    if not prefix:
        raise ValueError("table_prefix must be non-empty")

    renames = [("ticks_lake", "ticks_kind"), ("lake_version", "dataset_version")]

    planned: list[RenameOp] = []
    warnings: list[str] = []

    try:
        tables = _list_tables(cfg=cfg, connect_timeout_s=connect_timeout_s)
    except Exception as e:
        return {"ok": False, "error": str(e), "generated_at": _now_iso()}

    target_tables = [t for t in tables if str(t).startswith(prefix)]

    for t in target_tables:
        try:
            cols = set(_list_columns(cfg=cfg, table=t, connect_timeout_s=connect_timeout_s))
        except Exception:
            # Table may have been dropped between listing and inspection; ignore.
            continue
        for old, new in renames:
            if old in cols and new in cols:
                warnings.append(f"table={t}: both columns exist ({old},{new}); manual cleanup may be required")
                continue
            if old in cols and new not in cols:
                planned.append(RenameOp(table=t, old=old, new=new))

    # Additional best-effort: after rename, ensure expected UPSERT KEYS use the new column names.
    dedup_keys_by_table: dict[str, str] = {
        # ticks
        "ghtrader_ticks_raw_v2": "ts, symbol, ticks_kind, dataset_version, row_hash",
        "ghtrader_ticks_main_l5_v2": "ts, symbol, ticks_kind, dataset_version, row_hash",
        # coverage / no-data
        "ghtrader_symbol_day_index_v2": "ts, symbol, trading_day, ticks_kind, dataset_version",
        "ghtrader_no_data_days_v2": "ts, symbol, trading_day, ticks_kind, dataset_version",
        # features / labels (wide per-tick tables)
        "ghtrader_features_v2": "ts, symbol, ticks_kind, dataset_version, build_id, row_hash",
        "ghtrader_labels_v2": "ts, symbol, ticks_kind, dataset_version, build_id, row_hash",
    }

    plan_sql: list[str] = [op.sql() for op in planned]
    for tbl, keys in dedup_keys_by_table.items():
        if tbl in target_tables:
            plan_sql.append(f"ALTER TABLE {_ident(tbl)} DEDUP ENABLE UPSERT KEYS({keys})")

    if not apply:
        return {
            "ok": True,
            "mode": "dry_run",
            "table_prefix": prefix,
            "tables_considered": len(target_tables),
            "planned_renames": [asdict(op) | {"sql": op.sql()} for op in planned],
            "planned_sql": plan_sql,
            "warnings": warnings,
            "generated_at": _now_iso(),
        }

    # Apply: execute renames first, then dedup enable statements.
    results: list[dict[str, Any]] = []
    ok = True
    try:
        res1 = _apply_sql(cfg=cfg, statements=[op.sql() for op in planned], connect_timeout_s=connect_timeout_s)
        for r in res1:
            ok = ok and bool(r.ok)
            results.append(asdict(r))
    except Exception as e:
        ok = False
        results.append(asdict(OpResult(ok=False, table="", action="exec", sql="(rename batch)", error=str(e))))

    try:
        res2 = _apply_sql(cfg=cfg, statements=[s for s in plan_sql if "DEDUP ENABLE UPSERT KEYS" in s], connect_timeout_s=connect_timeout_s)
        for r in res2:
            ok = ok and bool(r.ok)
            results.append(asdict(r))
    except Exception as e:
        ok = False
        results.append(asdict(OpResult(ok=False, table="", action="exec", sql="(dedup batch)", error=str(e))))

    return {
        "ok": bool(ok),
        "mode": "apply",
        "table_prefix": prefix,
        "tables_considered": len(target_tables),
        "planned_renames": [asdict(op) | {"sql": op.sql()} for op in planned],
        "warnings": warnings,
        "results": results,
        "generated_at": _now_iso(),
    }


def best_effort_rename_provenance_columns_v2(*, cur: Any, table: str) -> None:
    """
    Best-effort in-transaction rename for a single table.

    Intended for "ensure table" codepaths that already hold a live cursor/connection.
    Safe to call repeatedly; failures (missing columns, already renamed) are ignored.
    """
    t = _ident(table)
    for old, new in [("ticks_lake", "ticks_kind"), ("lake_version", "dataset_version")]:
        try:
            cur.execute(RenameOp(table=t, old=old, new=new).sql())
        except Exception:
            pass


def ensure_schema_v2(
    *,
    cfg: QuestDBQueryConfig,
    apply: bool = False,
    connect_timeout_s: int = 2,
) -> dict[str, Any]:
    """
    Ensure all required columns exist on ghTrader QuestDB tables.

    This function adds missing columns (dataset_version, ticks_kind, etc.) to
    existing tables that were created before the schema was updated.

    If `apply=False` (default): returns a plan (dry-run).
    If `apply=True`: executes the ALTER TABLE statements.

    Returns a dict with:
    - ok: bool
    - mode: "dry_run" or "apply"
    - planned_adds: list of {table, column, type} dicts
    - results: list of {ok, sql, error} dicts (if apply=True)
    """
    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

    tick_numeric_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]

    # Define required columns per table type.
    # Format: {table_name: [(column_name, column_type), ...]}
    required_columns: dict[str, list[tuple[str, str]]] = {
        "ghtrader_ticks_raw_v2": [
            ("symbol", "SYMBOL"),
            ("ts", "TIMESTAMP"),
            ("datetime_ns", "LONG"),
            ("trading_day", "SYMBOL"),
            ("row_hash", "LONG"),
            ("dataset_version", "SYMBOL"),
            ("ticks_kind", "SYMBOL"),
        ] + [(c, "DOUBLE") for c in tick_numeric_cols],
        "ghtrader_ticks_main_l5_v2": [
            ("symbol", "SYMBOL"),
            ("ts", "TIMESTAMP"),
            ("datetime_ns", "LONG"),
            ("trading_day", "SYMBOL"),
            ("row_hash", "LONG"),
            ("dataset_version", "SYMBOL"),
            ("ticks_kind", "SYMBOL"),
            ("underlying_contract", "SYMBOL"),
            ("segment_id", "LONG"),
            ("schedule_hash", "SYMBOL"),
        ] + [(c, "DOUBLE") for c in tick_numeric_cols],
        "ghtrader_symbol_day_index_v2": [
            ("ts", "TIMESTAMP"),
            ("symbol", "SYMBOL"),
            ("trading_day", "SYMBOL"),
            ("ticks_kind", "SYMBOL"),
            ("dataset_version", "SYMBOL"),
            ("rows_total", "LONG"),
            ("first_datetime_ns", "LONG"),
            ("last_datetime_ns", "LONG"),
            ("l5_present", "BOOLEAN"),
            ("row_hash_min", "LONG"),
            ("row_hash_max", "LONG"),
            ("row_hash_sum", "LONG"),
            ("row_hash_sum_abs", "LONG"),
            ("updated_at", "TIMESTAMP"),
        ],
        "ghtrader_no_data_days_v2": [
            ("ts", "TIMESTAMP"),
            ("symbol", "SYMBOL"),
            ("trading_day", "SYMBOL"),
            ("ticks_kind", "SYMBOL"),
            ("dataset_version", "SYMBOL"),
            ("reason", "SYMBOL"),
            ("created_at", "TIMESTAMP"),
        ],
    }

    planned_adds: list[dict[str, str]] = []
    warnings: list[str] = []

    try:
        tables = _list_tables(cfg=cfg, connect_timeout_s=connect_timeout_s)
    except Exception as e:
        return {"ok": False, "error": str(e), "generated_at": _now_iso()}

    existing_tables = set(tables)

    for table, cols in required_columns.items():
        if table not in existing_tables:
            warnings.append(f"table={table} does not exist; skipping")
            continue

        try:
            existing_cols = set(_list_columns(cfg=cfg, table=table, connect_timeout_s=connect_timeout_s))
        except Exception as e:
            warnings.append(f"table={table}: failed to list columns: {e}")
            continue

        for col_name, col_type in cols:
            if col_name not in existing_cols:
                planned_adds.append({"table": table, "column": col_name, "type": col_type})

    plan_sql: list[str] = [
        f"ALTER TABLE {_ident(a['table'])} ADD COLUMN {_ident(a['column'])} {a['type']}"
        for a in planned_adds
    ]

    if not apply:
        return {
            "ok": True,
            "mode": "dry_run",
            "tables_checked": len([t for t in required_columns if t in existing_tables]),
            "planned_adds": planned_adds,
            "planned_sql": plan_sql,
            "warnings": warnings,
            "generated_at": _now_iso(),
        }

    # Apply: execute ALTER TABLE ADD COLUMN statements.
    results: list[dict[str, Any]] = []
    ok = True
    try:
        res = _apply_sql(cfg=cfg, statements=plan_sql, connect_timeout_s=connect_timeout_s)
        for r in res:
            # Treat "column already exists" as success (idempotent).
            if not r.ok and "already exists" in str(r.error or "").lower():
                results.append(asdict(r) | {"ok": True, "note": "column_already_exists"})
            else:
                ok = ok and bool(r.ok)
                results.append(asdict(r))
    except Exception as e:
        ok = False
        results.append(asdict(OpResult(ok=False, table="", action="exec", sql="(add columns batch)", error=str(e))))

    return {
        "ok": bool(ok),
        "mode": "apply",
        "tables_checked": len([t for t in required_columns if t in existing_tables]),
        "planned_adds": planned_adds,
        "warnings": warnings,
        "results": results,
        "generated_at": _now_iso(),
    }


__all__ = ["migrate_column_names_v2", "best_effort_rename_provenance_columns_v2", "ensure_schema_v2"]

