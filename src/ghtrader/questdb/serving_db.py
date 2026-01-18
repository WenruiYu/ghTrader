"""
QuestDB backend for tick data storage.

This module provides the QuestDB ILP (Influx Line Protocol) backend for
writing ticks to QuestDB. The QuestDBBackend class and make_serving_backend()
factory are the primary entry points.

Tick ingestion writes directly to QuestDB via tq_ingest.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd
import structlog

from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

log = structlog.get_logger()


ServingBackendType = Literal["questdb"]


@dataclass(frozen=True)
class ServingDBConfig:
    backend: ServingBackendType
    host: str = "127.0.0.1"

    # QuestDB
    questdb_ilp_port: int = 9009
    questdb_pg_port: int = 8812
    questdb_pg_user: str = "admin"
    questdb_pg_password: str = "quest"
    questdb_pg_dbname: str = "qdb"


class ServingDBBackend:
    def __init__(self, *, config: ServingDBConfig) -> None:
        self.config = config

    def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
        raise NotImplementedError

    def ingest_df(self, *, table: str, df: pd.DataFrame) -> None:
        raise NotImplementedError


class QuestDBBackend(ServingDBBackend):
    """
    QuestDB backend (ILP ingestion; optional SQL for table DDL).

    Requires extras: questdb (Python client) + psycopg[binary].
    """

    def _sender(self):
        try:
            from questdb.ingress import Sender  # type: ignore

            return Sender
        except Exception as e:
            raise RuntimeError("QuestDB client not installed. Install with: pip install -e '.[questdb]'") from e

    def _psycopg(self):
        try:
            import psycopg  # type: ignore

            return psycopg
        except Exception as e:
            raise RuntimeError("psycopg not installed. Install with: pip install -e '.[questdb]'") from e

    def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
        # Best-effort DDL via PGWire. ILP can auto-create but may not create WAL/partitioning settings.
        psycopg = self._psycopg()
        # Store the full canonical tick schema (including L5 columns) plus provenance tags.
        tick_numeric_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
        cols = ["symbol SYMBOL", "ts TIMESTAMP", "datetime_ns LONG", "trading_day SYMBOL", "row_hash LONG"]
        cols += [f"{c} DOUBLE" for c in tick_numeric_cols]
        cols += ["dataset_version SYMBOL", "ticks_kind SYMBOL"]
        if include_segment_metadata:
            cols += ["underlying_contract SYMBOL", "segment_id LONG", "schedule_hash SYMBOL"]

        # DEDUP UPSERT KEYS makes ingest idempotent:
        # - include ts + symbol + provenance tags + row_hash so re-ingest replaces identical rows
        # - row_hash prevents collapsing legitimate same-timestamp distinct rows
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {", ".join(cols)}
        ) TIMESTAMP(ts) PARTITION BY DAY WAL
          DEDUP UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, row_hash)
        """
        conn_params = {
            "user": self.config.questdb_pg_user,
            "password": self.config.questdb_pg_password,
            "host": self.config.host,
            "port": int(self.config.questdb_pg_port),
            "dbname": self.config.questdb_pg_dbname,
        }
        try:
            with psycopg.connect(**conn_params) as conn:
                # DDL/schema evolution: use autocommit so per-statement failures don't
                # abort the whole transaction (psycopg behavior).
                try:
                    conn.autocommit = True  # type: ignore[attr-defined]
                except Exception:
                    pass
                with conn.cursor() as cur:
                    cur.execute(ddl)
                    # Best-effort column rename migration (v2): tolerate older column names.
                    from .migrate import best_effort_rename_provenance_columns_v2

                    best_effort_rename_provenance_columns_v2(cur=cur, table=str(table))
                    # Best-effort schema evolution: add any newly-required columns to existing tables.
                    # QuestDB lacks strong IF NOT EXISTS for columns across versions; ignore failures.
                    for name, typ in [("trading_day", "SYMBOL"), ("row_hash", "LONG")] + [(c, "DOUBLE") for c in tick_numeric_cols] + [
                        ("dataset_version", "SYMBOL"),
                        ("ticks_kind", "SYMBOL"),
                    ]:
                        try:
                            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
                        except Exception:
                            pass
                    if include_segment_metadata:
                        for name, typ in [("underlying_contract", "SYMBOL"), ("segment_id", "LONG"), ("schedule_hash", "SYMBOL")]:
                            try:
                                cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
                            except Exception:
                                pass

                    # Best-effort: enable dedup on existing tables (no-op if already enabled).
                    try:
                        cur.execute(f"ALTER TABLE {table} DEDUP ENABLE UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, row_hash)")
                    except Exception:
                        pass
        except Exception as e:
            log.warning("serving_db.questdb_ddl_failed", table=table, error=str(e))

    def ingest_df(self, *, table: str, df: pd.DataFrame) -> None:
        Sender = self._sender()

        host = str(self.config.host)
        port = int(self.config.questdb_ilp_port)

        # Help the QuestDB client map stable tag-like columns to SYMBOL.
        # (The DataFrame ingestion API uses pandas categoricals to represent SYMBOL columns.)
        for c in ["symbol", "trading_day", "ticks_kind", "dataset_version", "underlying_contract", "schedule_hash"]:
            if c in df.columns:
                try:
                    df[c] = df[c].astype("category")
                except Exception:
                    pass

        # questdb>=4 uses Sender.from_conf() for TCP ILP.
        # The direct Sender(host, port) constructor is not supported on 4.1.0.
        sender_ctx = None
        conf = f"tcp::addr={host}:{port};"
        if hasattr(Sender, "from_conf"):
            sender_ctx = Sender.from_conf(conf)
        else:
            # Backward-compat (older clients): attempt the old constructor forms.
            try:
                sender_ctx = Sender(host, port)
            except TypeError:
                # Some older variants accept a single config string.
                sender_ctx = Sender(conf)

        with sender_ctx as sender:
            sender.dataframe(df, table_name=table, at="ts")
            # Ensure buffered ILP gets sent (safe best-effort; auto_flush may already handle it).
            try:
                sender.flush()
            except Exception:
                pass


def make_serving_backend(config: ServingDBConfig) -> ServingDBBackend:
    if config.backend == "questdb":
        return QuestDBBackend(config=config)
    raise ValueError(f"Unknown backend: {config.backend}")

