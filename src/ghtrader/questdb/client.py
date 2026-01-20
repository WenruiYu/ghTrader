from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any


@dataclass(frozen=True)
class QuestDBQueryConfig:
    """
    Connection config for QuestDB via PGWire (psycopg).

    This is used for read queries and DDL that are best expressed over SQL.
    """

    host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_dbname: str


def psycopg_module():
    """
    Import psycopg lazily (QuestDB support is optional via extras).
    """
    try:
        import psycopg  # type: ignore

        return psycopg
    except Exception as e:
        raise RuntimeError("psycopg not installed. Install with: pip install -e '.[questdb]'") from e


def connect_pg(cfg: QuestDBQueryConfig, *, connect_timeout_s: int = 2):
    """
    Connect to QuestDB PGWire using psycopg.
    """
    psycopg = psycopg_module()
    to = int(connect_timeout_s) if int(connect_timeout_s) > 0 else 2
    return psycopg.connect(
        user=cfg.pg_user,
        password=cfg.pg_password,
        host=cfg.host,
        port=int(cfg.pg_port),
        dbname=cfg.pg_dbname,
        connect_timeout=to,
    )


_TRANSIENT_PG_ERRORS = (
    "server closed the connection unexpectedly",
    "consuming input failed",
    "connection reset",
    "connection refused",
    "terminating connection",
    "could not connect",
    "timeout",
    "broken pipe",
    "eof detected",
    "network is unreachable",
)


def is_transient_pg_error(err: Exception) -> bool:
    """
    Best-effort classification for transient PGWire failures.
    """
    msg = str(err or "").lower()
    return any(s in msg for s in _TRANSIENT_PG_ERRORS)


def connect_pg_safe(
    cfg: QuestDBQueryConfig,
    *,
    connect_timeout_s: int = 2,
    retries: int = 2,
    backoff_s: float = 0.2,
    autocommit: bool = True,
):
    """
    Connect to QuestDB PGWire with retries and optional autocommit.
    """
    last_err: Exception | None = None
    for attempt in range(int(retries) + 1):
        try:
            conn = connect_pg(cfg, connect_timeout_s=int(connect_timeout_s))
            if autocommit:
                try:
                    conn.autocommit = True  # type: ignore[attr-defined]
                except Exception:
                    pass
            return conn
        except Exception as e:
            last_err = e
            if attempt >= int(retries) or not is_transient_pg_error(e):
                raise
            try:
                time.sleep(float(backoff_s) * (2 ** attempt))
            except Exception:
                pass
    if last_err:
        raise last_err
    raise RuntimeError("connect_pg_safe failed without exception")


def make_questdb_query_config_from_env() -> QuestDBQueryConfig:
    """
    Build QuestDB PGWire config from ghTrader env/config helpers.
    """
    from ghtrader.config import (
        get_questdb_host,
        get_questdb_pg_dbname,
        get_questdb_pg_password,
        get_questdb_pg_port,
        get_questdb_pg_user,
    )

    return QuestDBQueryConfig(
        host=str(get_questdb_host()),
        pg_port=int(get_questdb_pg_port()),
        pg_user=str(get_questdb_pg_user()),
        pg_password=str(get_questdb_pg_password()),
        pg_dbname=str(get_questdb_pg_dbname()),
    )


def questdb_reachable_pg(
    *,
    connect_timeout_s: int = 1,
    retries: int = 1,
    backoff_s: float = 0.2,
) -> dict[str, Any]:
    """
    Best-effort QuestDB reachability check (PGWire).

    Returns {ok: bool, error?: str}.
    """
    try:
        cfg = make_questdb_query_config_from_env()
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        with connect_pg_safe(
            cfg,
            connect_timeout_s=int(connect_timeout_s),
            retries=int(retries),
            backoff_s=float(backoff_s),
            autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

