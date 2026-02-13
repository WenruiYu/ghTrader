"""
QuestDB backend for tick data storage.

This module provides the QuestDB ILP (Influx Line Protocol) backend for
writing ticks to QuestDB. The QuestDBBackend class and make_serving_backend()
factory are the primary entry points.

Tick ingestion writes directly to QuestDB via tq_ingest.py.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import threading
from typing import Literal

import pandas as pd
import structlog

from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

log = structlog.get_logger()


ServingBackendType = Literal["questdb"]


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except Exception:
        return int(default)


def _env_bool(key: str, default: bool) -> bool:
    raw = str(os.environ.get(key, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


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

    def _sender_context(self):
        Sender = self._sender()
        host = str(self.config.host)
        port = int(self.config.questdb_ilp_port)
        conf = f"tcp::addr={host}:{port};"
        if hasattr(Sender, "from_conf"):
            return Sender.from_conf(conf)
        try:
            return Sender(host, port)
        except TypeError:
            return Sender(conf)

    def _sender_thread_local(self) -> threading.local:
        tl = getattr(self, "_sender_tls", None)
        if tl is None:
            tl = threading.local()
            setattr(self, "_sender_tls", tl)
        return tl

    def _drop_thread_sender(self) -> None:
        tl = self._sender_thread_local()
        sender = getattr(tl, "sender", None)
        if sender is not None:
            try:
                sender.close()
            except Exception:
                pass
        tl.sender = None

    def _get_thread_sender(self):
        tl = self._sender_thread_local()
        sender = getattr(tl, "sender", None)
        if sender is not None:
            return sender
        sender = self._sender_context()
        tl.sender = sender
        return sender

    def _ingest_with_sender(self, *, sender: object, table: str, df: pd.DataFrame) -> None:
        batch_rows = max(1, _env_int("GHTRADER_QDB_ILP_BATCH_ROWS", 200_000))
        flush_every_batches = max(1, _env_int("GHTRADER_QDB_ILP_FLUSH_EVERY_BATCHES", 2))

        total_rows = int(len(df))
        if total_rows <= 0:
            return

        if total_rows <= batch_rows:
            sender.dataframe(df, table_name=table, at="ts")
            try:
                sender.flush()
            except Exception:
                pass
            return

        batch_idx = 0
        for start in range(0, total_rows, batch_rows):
            stop = min(total_rows, start + batch_rows)
            sender.dataframe(df.iloc[start:stop], table_name=table, at="ts")
            batch_idx += 1
            if batch_idx % flush_every_batches == 0:
                try:
                    sender.flush()
                except Exception:
                    pass
        try:
            sender.flush()
        except Exception:
            pass

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
        df2 = df.copy(deep=False)
        # Help the QuestDB client map stable tag-like columns to SYMBOL.
        # (The DataFrame ingestion API uses pandas categoricals to represent SYMBOL columns.)
        for c in ["symbol", "trading_day", "ticks_kind", "dataset_version", "underlying_contract", "schedule_hash"]:
            if c in df2.columns:
                try:
                    df2[c] = df2[c].astype("category")
                except Exception:
                    pass

        persistent_sender = _env_bool("GHTRADER_QDB_ILP_PERSISTENT_SENDER", True)
        try:
            if persistent_sender:
                sender = self._get_thread_sender()
                self._ingest_with_sender(sender=sender, table=table, df=df2)
            else:
                with self._sender_context() as sender:
                    self._ingest_with_sender(sender=sender, table=table, df=df2)
        except Exception:
            # Drop poisoned sender so the next write reconnects cleanly.
            if persistent_sender:
                self._drop_thread_sender()
            raise
        else:
            try:
                log.debug(
                    "serving_db.questdb_ingest",
                    table=str(table),
                    rows=int(len(df2)),
                    ilp_batch_rows=int(_env_int("GHTRADER_QDB_ILP_BATCH_ROWS", 200_000)),
                    persistent_sender=bool(persistent_sender),
                )
            except Exception:
                pass


def make_serving_backend(config: ServingDBConfig) -> ServingDBBackend:
    if config.backend == "questdb":
        return QuestDBBackend(config=config)
    raise ValueError(f"Unknown backend: {config.backend}")

