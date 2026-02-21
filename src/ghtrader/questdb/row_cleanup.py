from __future__ import annotations

from collections.abc import Callable, Sequence
import time
from typing import Any

from .client import QuestDBQueryConfig, connect_pg


def replace_table_delete_where(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    columns: Sequence[str],
    delete_where_sql: str,
    delete_params: Sequence[Any] | None,
    ensure_table: Callable[[str], None],
    connect_timeout_s: int = 2,
) -> int:
    """
    Delete rows via fixed replace-table strategy (no DELETE SQL branch).

    Strategy:
    1) Create ensured temp table.
    2) Copy rows that do NOT match delete condition.
    3) Drop original table.
    4) Rename temp table to original name.

    Returns count of rows removed by the condition.
    Raises on any failure (fail-fast, no runtime fallback).
    """
    tbl = str(table).strip()
    if not tbl:
        raise RuntimeError("table must not be empty")
    cols = [str(c).strip() for c in columns if str(c).strip()]
    if not cols:
        raise RuntimeError("columns must not be empty")
    where_sql = str(delete_where_sql or "").strip()
    if not where_sql:
        raise RuntimeError("delete_where_sql must not be empty")
    params = list(delete_params or [])

    ensure_table(tbl)
    cols_sql = ", ".join(cols)
    count_sql = f"SELECT count() FROM {tbl} WHERE {where_sql}"
    tmp_table = f"{tbl}_clean_{int(time.time() * 1000)}"
    insert_sql = (
        f"INSERT INTO {tmp_table} ({cols_sql}) "
        f"SELECT {cols_sql} FROM {tbl} WHERE NOT ({where_sql})"
    )

    with connect_pg(cfg, connect_timeout_s=connect_timeout_s) as conn:
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            (count,) = cur.fetchone() or (0,)
            ensure_table(tmp_table)
            cur.execute(insert_sql, params)
            cur.execute(f"DROP TABLE {tbl}")
            cur.execute(f"RENAME TABLE {tmp_table} TO {tbl}")
            return int(count or 0)
