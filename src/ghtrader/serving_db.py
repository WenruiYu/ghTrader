from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import structlog

from ghtrader.lake import LakeVersion, TicksLake, list_available_dates_in_lake, ticks_date_dir

log = structlog.get_logger()


ServingBackendType = Literal["questdb", "clickhouse"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_ticks_partition_any_schema(
    *,
    data_dir: Path,
    symbol: str,
    dt: date,
    ticks_lake: TicksLake,
    lake_version: LakeVersion,
) -> pd.DataFrame:
    """
    Read a single (symbol, date) partition into a DataFrame, preserving any extra columns.

    This is intentionally more permissive than ghtrader.lake readers because serving DBs
    must preserve segment metadata columns (e.g., underlying_contract, segment_id).
    """
    d = ticks_date_dir(data_dir, symbol, dt, ticks_lake=ticks_lake, lake_version=lake_version)
    if not d.exists():
        return pd.DataFrame()
    parts = sorted(d.glob("*.parquet"))
    if not parts:
        return pd.DataFrame()
    tables: list[pa.Table] = []
    for p in parts:
        try:
            t = pq.ParquetFile(p).read()
            # Normalize dictionary-encoded strings (common for parquet writers).
            cols: list[pa.Array] = []
            fields: list[pa.Field] = []
            for name in t.column_names:
                arr = t.column(name).combine_chunks()
                if pa.types.is_dictionary(arr.type):
                    arr = pc.cast(arr, pa.string())
                cols.append(arr)
                fields.append(pa.field(name, arr.type))
            tables.append(pa.Table.from_arrays(cols, schema=pa.schema(fields)))
        except Exception as e:
            log.warning("serving_db.read_failed", path=str(p), error=str(e))
    if not tables:
        return pd.DataFrame()
    t = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    df = t.to_pandas()
    if "datetime" in df.columns:
        df = df.sort_values("datetime").reset_index(drop=True)
    return df


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

    # ClickHouse (HTTP)
    clickhouse_port: int = 8123
    clickhouse_database: str = "default"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""


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

    Requires extras: questdb-ingress + psycopg[binary].
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
        cols = [
            "symbol SYMBOL",
            "ts TIMESTAMP",
            "datetime_ns LONG",
            "last_price DOUBLE",
            "bid_price1 DOUBLE",
            "ask_price1 DOUBLE",
            "volume DOUBLE",
            "open_interest DOUBLE",
            "lake_version SYMBOL",
            "ticks_lake SYMBOL",
        ]
        if include_segment_metadata:
            cols += ["underlying_contract SYMBOL", "segment_id LONG"]

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {", ".join(cols)}
        ) TIMESTAMP(ts) PARTITION BY DAY WAL
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
                with conn.cursor() as cur:
                    cur.execute(ddl)
        except Exception as e:
            log.warning("serving_db.questdb_ddl_failed", table=table, error=str(e))

    def ingest_df(self, *, table: str, df: pd.DataFrame) -> None:
        Sender = self._sender()
        with Sender(self.config.host, int(self.config.questdb_ilp_port)) as sender:
            sender.dataframe(df, table_name=table, at="ts")


class ClickHouseBackend(ServingDBBackend):
    """
    ClickHouse backend (MergeTree analytics).

    Requires extras: clickhouse-connect.
    """

    def _client(self):
        try:
            import clickhouse_connect  # type: ignore

            return clickhouse_connect.get_client(
                host=self.config.host,
                port=int(self.config.clickhouse_port),
                username=self.config.clickhouse_user,
                password=self.config.clickhouse_password,
                database=self.config.clickhouse_database,
            )
        except Exception as e:
            raise RuntimeError("clickhouse-connect not installed. Install with: pip install -e '.[clickhouse]'") from e

    def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
        client = self._client()
        cols = [
            "symbol String",
            "ts DateTime64(9)",
            "datetime_ns Int64",
            "last_price Float64",
            "bid_price1 Float64",
            "ask_price1 Float64",
            "volume Float64",
            "open_interest Float64",
            "lake_version LowCardinality(String)",
            "ticks_lake LowCardinality(String)",
        ]
        if include_segment_metadata:
            cols += ["underlying_contract LowCardinality(String)", "segment_id Int64"]
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
              {", ".join(cols)}
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMMDD(ts)
            ORDER BY (symbol, ts)
            """
        )

    def ingest_df(self, *, table: str, df: pd.DataFrame) -> None:
        client = self._client()
        cols = list(df.columns)
        rows = df.to_records(index=False).tolist()
        client.insert(table, rows, column_names=cols)


def make_serving_backend(config: ServingDBConfig) -> ServingDBBackend:
    if config.backend == "questdb":
        return QuestDBBackend(config=config)
    if config.backend == "clickhouse":
        return ClickHouseBackend(config=config)
    raise ValueError(f"Unknown backend: {config.backend}")


@dataclass
class IngestState:
    updated_at: str
    ingested_dates: list[str]


def _state_path(state_dir: Path, *, backend: ServingBackendType, table: str, symbol: str, ticks_lake: TicksLake, lake_version: LakeVersion) -> Path:
    safe_sym = symbol.replace("/", "_")
    return state_dir / backend / table / f"ticks_lake={ticks_lake}" / f"lake_version={lake_version}" / f"symbol={safe_sym}.json"


def load_state(state_dir: Path, *, backend: ServingBackendType, table: str, symbol: str, ticks_lake: TicksLake, lake_version: LakeVersion) -> IngestState:
    p = _state_path(state_dir, backend=backend, table=table, symbol=symbol, ticks_lake=ticks_lake, lake_version=lake_version)
    if not p.exists():
        return IngestState(updated_at=_now_iso(), ingested_dates=[])
    try:
        raw = json.loads(p.read_text())
        return IngestState(updated_at=str(raw.get("updated_at") or _now_iso()), ingested_dates=list(raw.get("ingested_dates") or []))
    except Exception:
        return IngestState(updated_at=_now_iso(), ingested_dates=[])


def save_state(state_dir: Path, *, backend: ServingBackendType, table: str, symbol: str, ticks_lake: TicksLake, lake_version: LakeVersion, ingested_dates: list[str]) -> Path:
    p = _state_path(state_dir, backend=backend, table=table, symbol=symbol, ticks_lake=ticks_lake, lake_version=lake_version)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": _now_iso(), "ingested_dates": sorted(set(ingested_dates))}
    p.write_text(json.dumps(payload, indent=2))
    return p


def sync_ticks_to_serving_db(
    *,
    backend: ServingDBBackend,
    backend_type: ServingBackendType,
    table: str,
    data_dir: Path,
    symbol: str,
    ticks_lake: TicksLake,
    lake_version: LakeVersion,
    mode: Literal["backfill", "incremental"],
    start_date: date | None,
    end_date: date | None,
    state_dir: Path,
) -> dict[str, Any]:
    """
    Sync ticks into a serving DB by (symbol, date) partitions.

    - backfill: ingest all partitions in [start_date, end_date]
    - incremental: ingest only partitions not recorded in state file
    """
    avail = list_available_dates_in_lake(data_dir, symbol, ticks_lake=ticks_lake, lake_version=lake_version)
    if not avail:
        raise ValueError(f"No available tick dates for {symbol} ticks_lake={ticks_lake} lake_version={lake_version}")

    d0 = start_date or avail[0]
    d1 = end_date or avail[-1]
    targets = [d for d in avail if d0 <= d <= d1]
    if not targets:
        return {"ingested": 0, "skipped": 0, "state_path": "", "dates": []}

    st = load_state(state_dir, backend=backend_type, table=table, symbol=symbol, ticks_lake=ticks_lake, lake_version=lake_version)
    done = set(st.ingested_dates)

    dates_to_ingest: list[date] = []
    if mode == "backfill":
        dates_to_ingest = targets
    else:
        for d in targets:
            if d.isoformat() not in done:
                dates_to_ingest.append(d)

    include_seg = bool(ticks_lake == "main_l5" and lake_version == "v2")
    backend.ensure_table(table=table, include_segment_metadata=include_seg)

    ingested: list[str] = []
    skipped = 0
    t0 = time.time()
    for d in dates_to_ingest:
        df = _read_ticks_partition_any_schema(data_dir=data_dir, symbol=symbol, dt=d, ticks_lake=ticks_lake, lake_version=lake_version)
        if df.empty:
            skipped += 1
            continue

        # Prepare columns for serving DB.
        df2 = pd.DataFrame(
            {
                "symbol": df["symbol"].astype(str),
                "ts": pd.to_datetime(df["datetime"].astype("int64"), unit="ns", utc=True),
                "datetime_ns": df["datetime"].astype("int64"),
                "last_price": pd.to_numeric(df.get("last_price"), errors="coerce"),
                "bid_price1": pd.to_numeric(df.get("bid_price1"), errors="coerce"),
                "ask_price1": pd.to_numeric(df.get("ask_price1"), errors="coerce"),
                "volume": pd.to_numeric(df.get("volume"), errors="coerce"),
                "open_interest": pd.to_numeric(df.get("open_interest"), errors="coerce"),
                "lake_version": str(lake_version),
                "ticks_lake": str(ticks_lake),
            }
        )
        if include_seg:
            df2["underlying_contract"] = df.get("underlying_contract", "").astype(str)
            df2["segment_id"] = pd.to_numeric(df.get("segment_id"), errors="coerce").fillna(-1).astype("int64")

        backend.ingest_df(table=table, df=df2)
        ingested.append(d.isoformat())

    # Update state (incremental only; backfill also records for idempotency).
    new_done = sorted(set(done) | set(ingested))
    st_path = save_state(
        state_dir,
        backend=backend_type,
        table=table,
        symbol=symbol,
        ticks_lake=ticks_lake,
        lake_version=lake_version,
        ingested_dates=new_done,
    )

    return {
        "ingested": int(len(ingested)),
        "skipped": int(skipped),
        "state_path": str(st_path),
        "dates": ingested,
        "seconds": float(time.time() - t0),
    }

