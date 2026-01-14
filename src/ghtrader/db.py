from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd


class DuckDBNotInstalled(RuntimeError):
    pass


def _require_duckdb():
    try:
        import duckdb  # type: ignore

        return duckdb
    except Exception as e:  # pragma: no cover
        raise DuckDBNotInstalled(
            "DuckDB is not installed. Install with: pip install -e '.[db]' (or '.[dev]' for dev/tests)."
        ) from e


def _as_posix(p: Path) -> str:
    # DuckDB SQL expects forward slashes in globs.
    return p.as_posix()


def _any_parquet(root: Path) -> bool:
    if not root.exists():
        return False
    try:
        next(root.glob("symbol=*/date=*/*.parquet"))
        return True
    except StopIteration:
        return False


@dataclass(frozen=True)
class DuckDBConfig:
    db_path: Path | None = None
    read_only: bool = False


class DuckDBBackend:
    """
    DuckDB lakehouse query layer.

    Design:
    - Parquet remains canonical.
    - DuckDB is an optional query/index cache (can be rebuilt).
    """

    def __init__(self, *, config: DuckDBConfig) -> None:
        self.config = config
        self._duckdb = _require_duckdb()

    def connect(self):
        if self.config.db_path is None:
            return self._duckdb.connect(database=":memory:")
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
        return self._duckdb.connect(database=str(self.config.db_path), read_only=bool(self.config.read_only))

    def init_views(self, *, con, data_dir: Path) -> list[str]:
        """
        Create (or replace) Parquet-backed views for querying the lake.

        Returns: list of view names created.
        """
        created: list[str] = []

        # ---- ticks: raw + main_l5 (v2 only) ----
        lake_version = "v2"
        lake_root = data_dir / "lake_v2"

        # Raw ticks
        raw_root = lake_root / "ticks"
        if _any_parquet(raw_root):
            glob = _as_posix(raw_root / "symbol=*" / "date=*" / "*.parquet")
            view = "ticks_raw_v2"
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {view} AS
                SELECT
                  '{lake_version}' AS lake_version,
                  regexp_extract(filename, 'symbol=([^/]+)', 1) AS hive_symbol,
                  regexp_extract(filename, 'date=([0-9-]+)', 1) AS hive_date,
                  * EXCLUDE (filename)
                FROM read_parquet('{glob}', filename=true)
                """
            )
            created.append(view)

        # Derived main_l5 ticks
        main_l5_root = lake_root / "main_l5" / "ticks"
        if _any_parquet(main_l5_root):
            glob = _as_posix(main_l5_root / "symbol=*" / "date=*" / "*.parquet")
            view = "ticks_main_l5_v2"
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {view} AS
                SELECT
                  '{lake_version}' AS lake_version,
                  regexp_extract(filename, 'symbol=([^/]+)', 1) AS hive_symbol,
                  regexp_extract(filename, 'date=([0-9-]+)', 1) AS hive_date,
                  * EXCLUDE (filename)
                FROM read_parquet('{glob}', filename=true, union_by_name=true)
                """
            )
            created.append(view)

        # ---- features / labels ----
        feat_root = data_dir / "features"
        if _any_parquet(feat_root):
            glob = _as_posix(feat_root / "symbol=*" / "date=*" / "*.parquet")
            con.execute(
                f"""
                CREATE OR REPLACE VIEW features_all AS
                SELECT
                  regexp_extract(filename, 'symbol=([^/]+)', 1) AS hive_symbol,
                  regexp_extract(filename, 'date=([0-9-]+)', 1) AS hive_date,
                  * EXCLUDE (filename)
                FROM read_parquet('{glob}', filename=true, union_by_name=true)
                """
            )
            created.append("features_all")

        lab_root = data_dir / "labels"
        if _any_parquet(lab_root):
            glob = _as_posix(lab_root / "symbol=*" / "date=*" / "*.parquet")
            con.execute(
                f"""
                CREATE OR REPLACE VIEW labels_all AS
                SELECT
                  regexp_extract(filename, 'symbol=([^/]+)', 1) AS hive_symbol,
                  regexp_extract(filename, 'date=([0-9-]+)', 1) AS hive_date,
                  * EXCLUDE (filename)
                FROM read_parquet('{glob}', filename=true, union_by_name=true)
                """
            )
            created.append("labels_all")

        return created

    def init_metrics_tables(self, *, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS run_metrics (
              kind VARCHAR NOT NULL,
              symbol VARCHAR,
              model VARCHAR,
              run_id VARCHAR,
              created_at VARCHAR,
              path VARCHAR NOT NULL,
              payload_json VARCHAR NOT NULL
            )
            """
        )

    def ingest_runs_metrics(self, *, con, runs_dir: Path) -> int:
        """
        Index JSON metrics under runs/ into DuckDB for ad-hoc querying.

        - backtests: runs/<symbol>/<model>/<run_id>/metrics.json
        - benchmarks: runs/benchmarks/<symbol>/<model>/<run_id>.json
        - daily pipeline: runs/daily_pipeline/<run_id>/report.json

        Returns number of rows inserted.
        """
        self.init_metrics_tables(con=con)

        rows: list[dict[str, Any]] = []

        # Backtests
        if runs_dir.exists():
            for sym_dir in runs_dir.iterdir():
                if not sym_dir.is_dir():
                    continue
                if sym_dir.name in {"benchmarks", "daily_pipeline", "control", "audit", "sweeps"}:
                    continue
                symbol = sym_dir.name
                for model_dir in sym_dir.iterdir():
                    if not model_dir.is_dir():
                        continue
                    model = model_dir.name
                    for run_dir in model_dir.iterdir():
                        if not run_dir.is_dir():
                            continue
                        p = run_dir / "metrics.json"
                        if not p.exists():
                            continue
                        try:
                            payload = json.loads(p.read_text())
                        except Exception:
                            continue
                        rows.append(
                            {
                                "kind": "backtest",
                                "symbol": symbol,
                                "model": model,
                                "run_id": run_dir.name,
                                "created_at": str(payload.get("created_at") or ""),
                                "path": str(p),
                                "payload_json": json.dumps(payload, ensure_ascii=False),
                            }
                        )

        # Benchmarks
        bench_root = runs_dir / "benchmarks"
        if bench_root.exists():
            for sym_dir in bench_root.glob("*"):
                if not sym_dir.is_dir():
                    continue
                symbol = sym_dir.name
                for model_dir in sym_dir.glob("*"):
                    if not model_dir.is_dir():
                        continue
                    model = model_dir.name
                    for p in model_dir.glob("*.json"):
                        if not p.is_file():
                            continue
                        try:
                            payload = json.loads(p.read_text())
                        except Exception:
                            continue
                        rows.append(
                            {
                                "kind": "benchmark",
                                "symbol": symbol,
                                "model": model,
                                "run_id": str(payload.get("run_id") or p.stem),
                                "created_at": str(payload.get("timestamp") or ""),
                                "path": str(p),
                                "payload_json": json.dumps(payload, ensure_ascii=False),
                            }
                        )

        # Daily pipeline reports
        dp_root = runs_dir / "daily_pipeline"
        if dp_root.exists():
            for run_dir in dp_root.glob("*"):
                if not run_dir.is_dir():
                    continue
                p = run_dir / "report.json"
                if not p.exists():
                    continue
                try:
                    payload = json.loads(p.read_text())
                except Exception:
                    continue
                rows.append(
                    {
                        "kind": "daily_pipeline",
                        "symbol": str(payload.get("symbol") or ""),
                        "model": str(payload.get("model") or ""),
                        "run_id": str(payload.get("run_id") or run_dir.name),
                        "created_at": str(payload.get("created_at") or ""),
                        "path": str(p),
                        "payload_json": json.dumps(payload, ensure_ascii=False),
                    }
                )

        if not rows:
            return 0

        df = pd.DataFrame(rows)

        # Replace-by-path (idempotent)
        # DuckDB doesn't require explicit transactions for this small indexing workload.
        con.register("_rm_tmp", df)
        con.execute("DELETE FROM run_metrics WHERE path IN (SELECT DISTINCT path FROM _rm_tmp)")
        con.execute("INSERT INTO run_metrics SELECT * FROM _rm_tmp")
        con.unregister("_rm_tmp")
        return int(len(df))

    def query_df(self, *, con, sql: str) -> pd.DataFrame:
        return self.query_df_limited(con=con, sql=sql, limit=None)

    def query_df_limited(self, *, con, sql: str, limit: int | None) -> pd.DataFrame:
        """
        Execute a read-only query with optional LIMIT pushdown.

        Guardrails:
        - Only SELECT/WITH queries.
        - Reject common DDL/DML/unsafe keywords even if embedded.
        """
        s = str(sql).strip()
        head = re.sub(r"\\s+", " ", s[:200].lower())
        if not (head.startswith("select") or head.startswith("with")):
            raise ValueError("Only SELECT/WITH queries are allowed in this interface.")

        bad = [
            "create",
            "drop",
            "alter",
            "insert",
            "update",
            "delete",
            "copy",
            "attach",
            "detach",
            "install",
            "load",
            "pragma",
            "export",
            "import",
            "call",
        ]
        for kw in bad:
            if re.search(rf"\\b{kw}\\b", s, flags=re.IGNORECASE):
                raise ValueError(f"Disallowed keyword in query: {kw}")

        if limit is None:
            return con.execute(s).df()

        lim = int(limit)
        if lim <= 0:
            lim = 1
        # Use Relation.limit() to ensure limit pushdown even for WITH queries.
        rel = con.sql(s).limit(lim)
        return rel.df()

