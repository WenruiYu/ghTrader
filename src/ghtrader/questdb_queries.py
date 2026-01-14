from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import re
import structlog

log = structlog.get_logger()


@dataclass(frozen=True)
class QuestDBQueryConfig:
    host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_dbname: str


def _psycopg():
    try:
        import psycopg  # type: ignore

        return psycopg
    except Exception as e:
        raise RuntimeError("psycopg not installed. Install with: pip install -e '.[questdb]'") from e


def _connect(cfg: QuestDBQueryConfig):
    psycopg = _psycopg()
    return psycopg.connect(
        user=cfg.pg_user,
        password=cfg.pg_password,
        host=cfg.host,
        port=int(cfg.pg_port),
        dbname=cfg.pg_dbname,
    )


_READ_ONLY_START_RE = re.compile(r"^\s*(with|select)\b", flags=re.IGNORECASE)
_DISALLOWED_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|create|alter|truncate|grant|revoke|copy|vacuum|analyze|attach|detach)\b",
    flags=re.IGNORECASE,
)


def sanitize_read_only_sql(query: str) -> str:
    """
    Best-effort guardrail for the dashboard SQL explorer.

    Allows only single-statement SELECT/WITH queries.
    """
    q = str(query or "").strip()
    if not q:
        raise ValueError("Query is required")

    # Disallow multi-statement queries. Allow at most one trailing semicolon.
    if ";" in q:
        q2 = q.rstrip()
        if q2.endswith(";"):
            q2 = q2[:-1].rstrip()
        if ";" in q2:
            raise ValueError("Only one SQL statement is allowed")
        q = q2

    if not _READ_ONLY_START_RE.match(q):
        raise ValueError("Only SELECT/WITH queries are allowed")

    if _DISALLOWED_SQL_RE.search(q):
        raise ValueError("Disallowed keyword in query (read-only mode)")

    return q


def query_sql_read_only(
    *,
    cfg: QuestDBQueryConfig,
    query: str,
    limit: int = 200,
    connect_timeout_s: int = 2,
) -> tuple[list[str], list[dict[str, str]]]:
    """
    Run a read-only SQL query against QuestDB via PGWire.

    Returns: (columns, rows) where rows is a list of stringified dict records.

    Notes:
    - Enforces a row cap best-effort by wrapping the query with a LIMIT.
    - If wrapping fails (e.g., unsupported nested WITH), falls back to fetching at most `limit` rows client-side.
    """
    q = sanitize_read_only_sql(query=query)
    lim = max(1, min(int(limit or 200), 500))

    psycopg = _psycopg()
    cols: list[str] = []
    rows_out: list[dict[str, str]] = []

    def _stringify(v: Any) -> str:
        if v is None:
            return ""
        try:
            return str(v)
        except Exception:
            return ""

    # Best-effort: execute a wrapped query to enforce LIMIT in the engine.
    wrapped = f"SELECT * FROM ({q}) LIMIT {lim}"
    with psycopg.connect(
        user=cfg.pg_user,
        password=cfg.pg_password,
        host=cfg.host,
        port=int(cfg.pg_port),
        dbname=cfg.pg_dbname,
        connect_timeout=int(connect_timeout_s),
    ) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(wrapped)
            except Exception:
                # Fallback: execute user query as-is and fetch at most `lim`.
                cur.execute(q)
            desc = cur.description or []
            cols = [str(d.name) for d in desc if getattr(d, "name", None)]
            fetched = cur.fetchmany(size=lim)
            for r in fetched:
                try:
                    row = {c: _stringify(v) for c, v in zip(cols, r)}
                except Exception:
                    row = {}
                rows_out.append(row)

    return cols, rows_out


def _l5_condition_sql() -> str:
    # True L5 means at least one of levels 2–5 has positive, non-NaN price values.
    parts: list[str] = []
    for i in range(2, 6):
        parts.append(f"bid_price{i} > 0")
        parts.append(f"ask_price{i} > 0")
    return "(" + " OR ".join(parts) + ")"


def query_symbol_day_bounds(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    lake_version: str,
    ticks_lake: str = "raw",
    l5_only: bool = False,
) -> dict[str, dict[str, Any]]:
    """
    Return {symbol: {first_day, last_day, n_days}} using QuestDB canonical ticks.

    Notes:
    - Uses `trading_day` column (ISO YYYY-MM-DD strings, stored as SYMBOL).
    - Uses `cast(trading_day as string)` for compatibility with min/max.
    - `l5_only=True` restricts to rows that appear to have true L5 values.
    """
    if not symbols:
        return {}

    lv = str(lake_version).lower().strip()
    tl = str(ticks_lake).lower().strip()

    # Safe placeholders for IN (...)
    placeholders = ", ".join(["%s"] * len(symbols))
    where = [f"symbol IN ({placeholders})", "ticks_lake = %s", "lake_version = %s"]
    params: list[Any] = list(symbols) + [tl, lv]
    if l5_only:
        where.append(_l5_condition_sql())

    sql = (
        "SELECT symbol, "
        "min(cast(trading_day as string)) AS first_day, "
        "max(cast(trading_day as string)) AS last_day, "
        "count(DISTINCT cast(trading_day as string)) AS n_days "
        f"FROM {table} "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY symbol"
    )

    out: dict[str, dict[str, Any]] = {}
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                try:
                    sym = str(row[0])
                    out[sym] = {"first_day": row[1], "last_day": row[2], "n_days": row[3]}
                except Exception:
                    continue
    return out


def query_contract_coverage(
    *,
    cfg: QuestDBQueryConfig,
    table: str,
    symbols: list[str],
    lake_version: str,
    ticks_lake: str = "raw",
) -> dict[str, dict[str, Any]]:
    """
    Return per-symbol coverage:
      {
        symbol: {
          first_tick_day, last_tick_day,
          first_l5_day, last_l5_day
        }
      }
    """
    base = query_symbol_day_bounds(
        cfg=cfg, table=table, symbols=symbols, lake_version=lake_version, ticks_lake=ticks_lake, l5_only=False
    )
    l5 = query_symbol_day_bounds(
        cfg=cfg, table=table, symbols=symbols, lake_version=lake_version, ticks_lake=ticks_lake, l5_only=True
    )

    out: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        b = base.get(sym) or {}
        l = l5.get(sym) or {}
        if not b and not l:
            continue
        out[sym] = {
            "first_tick_day": b.get("first_day"),
            "last_tick_day": b.get("last_day"),
            "tick_days": b.get("n_days"),
            "first_l5_day": l.get("first_day"),
            "last_l5_day": l.get("last_day"),
            "l5_days": l.get("n_days"),
        }
    return out

