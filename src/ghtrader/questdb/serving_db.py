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
import time
from typing import Literal

import pandas as pd
import structlog

from ghtrader.config import env_bool, env_int
from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

log = structlog.get_logger()


ServingBackendType = Literal["questdb"]


def _env_int(key: str, default: int) -> int:
    return env_int(key, default)


def _env_bool(key: str, default: bool) -> bool:
    return env_bool(key, default)


def _ticks_partition_by(default: str = "DAY") -> str:
    """Resolve QuestDB partition strategy for tick tables."""
    allowed = {"DAY", "MONTH", "YEAR"}
    raw = str(os.getenv("GHTRADER_QDB_TICKS_PARTITION_BY", default) or default).strip().upper()
    if raw in allowed:
        return raw
    try:
        log.warning(
            "serving_db.invalid_partition_by",
            env_key="GHTRADER_QDB_TICKS_PARTITION_BY",
            value=raw,
            fallback=default,
        )
    except Exception:
        pass
    return str(default).upper().strip()


def _is_recoverable_sender_error(exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    if not msg:
        return False
    patterns = (
        "sender is closed",
        "can't be called: sender is closed",
        "broken pipe",
        "connection reset",
        "connection aborted",
        "transport endpoint is not connected",
        "socket is closed",
    )
    if any(p in msg for p in patterns):
        return True
    name = type(exc).__name__.lower()
    # QuestDB client may wrap transport issues into IngressError.
    return "ingresserror" in name and ("closed" in msg or "connection" in msg or "socket" in msg)


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
        # QuestDB Sender objects created outside a context manager must be
        # explicitly established before dataframe()/flush() calls.
        establish = getattr(sender, "establish", None)
        if callable(establish):
            establish()
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
            sender.flush()
            return

        batch_idx = 0
        for start in range(0, total_rows, batch_rows):
            stop = min(total_rows, start + batch_rows)
            sender.dataframe(df.iloc[start:stop], table_name=table, at="ts")
            batch_idx += 1
            if batch_idx % flush_every_batches == 0:
                sender.flush()
        sender.flush()

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
        partition_by = _ticks_partition_by(default="DAY")
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {", ".join(cols)}
        ) TIMESTAMP(ts) PARTITION BY {partition_by} WAL
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
                    migration_id = f"ensure_table:{str(table)}:v2"
                    cur.execute(ddl)
                    from .migrate import (
                        append_schema_migration_ledger,
                        best_effort_rename_provenance_columns_v2,
                        ensure_schema_migration_ledger,
                        list_table_columns_with_cursor,
                    )

                    # Ledger is best-effort; schema contract checks remain strict.
                    try:
                        ensure_schema_migration_ledger(cur=cur)
                    except Exception:
                        pass

                    best_effort_rename_provenance_columns_v2(cur=cur, table=str(table))

                    required_cols: list[tuple[str, str]] = [("trading_day", "SYMBOL"), ("row_hash", "LONG")] + [
                        (c, "DOUBLE") for c in tick_numeric_cols
                    ] + [
                        ("dataset_version", "SYMBOL"),
                        ("ticks_kind", "SYMBOL"),
                    ]
                    if include_segment_metadata:
                        required_cols += [("underlying_contract", "SYMBOL"), ("segment_id", "LONG"), ("schedule_hash", "SYMBOL")]

                    ddl_errors: list[str] = []
                    for name, typ in required_cols:
                        try:
                            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
                        except Exception as e:
                            err_s = str(e)
                            if "already exists" in err_s.lower():
                                continue
                            ddl_errors.append(f"add_column:{name}:{err_s}")

                    # Best-effort: enable dedup on existing tables (no-op if already enabled).
                    try:
                        cur.execute(f"ALTER TABLE {table} DEDUP ENABLE UPSERT KEYS(ts, symbol, ticks_kind, dataset_version, row_hash)")
                    except Exception as e:
                        ddl_errors.append(f"dedup_enable:{e}")

                    existing_cols = list_table_columns_with_cursor(cur=cur, table=str(table))
                    missing_required = [name for name, _ in required_cols if name not in existing_cols]

                    status = "ok"
                    detail = f"required={len(required_cols)} missing={len(missing_required)} ddl_errors={len(ddl_errors)}"
                    if missing_required or ddl_errors:
                        status = "failed"
                        detail = (
                            f"missing_required={missing_required}; "
                            f"ddl_errors={ddl_errors}"
                        )
                    try:
                        append_schema_migration_ledger(
                            cur=cur,
                            migration_id=migration_id,
                            table_name=str(table),
                            action="ensure_table",
                            status=status,
                            detail=detail,
                        )
                    except Exception:
                        pass

                    if missing_required:
                        raise RuntimeError(
                            f"QuestDB schema contract violation for {table}: missing required columns {missing_required}"
                        )
                    if ddl_errors:
                        raise RuntimeError(f"QuestDB schema evolution failed for {table}: {ddl_errors}")
        except Exception as e:
            log.warning("serving_db.questdb_ddl_failed", table=table, error=str(e))
            raise

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
        retry_max = max(0, int(_env_int("GHTRADER_QDB_ILP_RETRY_MAX", 1 if persistent_sender else 0)))
        for attempt in range(retry_max + 1):
            try:
                if persistent_sender:
                    sender = self._get_thread_sender()
                    self._ingest_with_sender(sender=sender, table=table, df=df2)
                else:
                    with self._sender_context() as sender:
                        self._ingest_with_sender(sender=sender, table=table, df=df2)
                break
            except Exception as e:
                if persistent_sender:
                    # Drop poisoned sender so the next write reconnects cleanly.
                    self._drop_thread_sender()
                recoverable = bool(persistent_sender and _is_recoverable_sender_error(e))
                if (not recoverable) or attempt >= retry_max:
                    raise
                wait_s = min(2.0, 0.1 * (2**attempt))
                log.warning(
                    "serving_db.questdb_sender_retry",
                    table=str(table),
                    attempt=int(attempt + 1),
                    max_attempts=int(retry_max + 1),
                    wait_s=float(wait_s),
                    error=str(e),
                )
                if wait_s > 0:
                    time.sleep(float(wait_s))
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

