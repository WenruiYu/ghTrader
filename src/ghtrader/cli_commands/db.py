from __future__ import annotations

from datetime import datetime
from pathlib import Path

import click
import structlog

log = structlog.get_logger()


def register(main: click.Group) -> None:
    """
    Register `ghtrader db ...` commands onto the root CLI group.
    """

    @main.group("db")
    @click.pass_context
    def db_group(ctx: click.Context) -> None:
        """Database/query-layer utilities (QuestDB)."""
        _ = ctx

    @db_group.command("questdb-health")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--connect-timeout", default=1, type=int, show_default=True, help="Connect timeout seconds")
    def db_questdb_health(
        host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        connect_timeout: int,
    ) -> None:
        """Check QuestDB reachability via PGWire (SELECT 1)."""
        import json
        import time

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.client import QuestDBQueryConfig, connect_pg_safe

        h = str(host).strip() or str(get_questdb_host())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        t0 = time.time()
        try:
            cfg = QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn)
            with connect_pg_safe(cfg, connect_timeout_s=int(connect_timeout), retries=1, backoff_s=0.2, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            dt_ms = int((time.time() - t0) * 1000.0)
            click.echo(
                json.dumps(
                    {"ok": True, "host": h, "pg_port": int(p), "pg_user": u, "pg_dbname": dbn, "latency_ms": dt_ms}
                )
            )
        except Exception as e:
            dt_ms = int((time.time() - t0) * 1000.0)
            click.echo(
                json.dumps(
                    {
                        "ok": False,
                        "host": h,
                        "pg_port": int(p),
                        "pg_user": u,
                        "pg_dbname": dbn,
                        "latency_ms": dt_ms,
                        "error": str(e),
                    }
                )
            )
            raise SystemExit(1)

    @db_group.command("questdb-init")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--ilp-port", default=0, type=int, help="QuestDB ILP port (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--raw-table", default="ghtrader_ticks_raw_v2", show_default=True, help="Raw ticks table name")
    @click.option("--main-l5-table", default="ghtrader_ticks_main_l5_v2", show_default=True, help="Derived main_l5 ticks table name")
    def db_questdb_init(
        host: str,
        ilp_port: int,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        raw_table: str,
        main_l5_table: str,
    ) -> None:
        """Ensure required ghTrader tables exist in QuestDB (DDL via PGWire)."""
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_ilp_port,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

        h = str(host).strip() or str(get_questdb_host())
        ilp = int(ilp_port) if int(ilp_port) > 0 else int(get_questdb_ilp_port())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        cfg = ServingDBConfig(
            backend="questdb",
            host=h,
            questdb_ilp_port=int(ilp),
            questdb_pg_port=int(p),
            questdb_pg_user=u,
            questdb_pg_password=pw,
            questdb_pg_dbname=dbn,
        )
        backend = make_serving_backend(cfg)

        # Ensure both table schemas exist (best-effort idempotent DDL).
        backend.ensure_table(table=str(raw_table), include_segment_metadata=False)
        backend.ensure_table(table=str(main_l5_table), include_segment_metadata=True)

        # Validate tables are queryable (so failures are surfaced loudly).
        try:
            import psycopg  # type: ignore

            with psycopg.connect(user=u, password=pw, host=h, port=int(p), dbname=dbn, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    for tbl in [str(raw_table), str(main_l5_table)]:
                        cur.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                        cur.fetchone()
        except Exception as e:
            click.echo(json.dumps({"ok": False, "error": str(e)}))
            raise SystemExit(1)

        click.echo(
            json.dumps(
                {
                    "ok": True,
                    "host": h,
                    "pg_port": int(p),
                    "ilp_port": int(ilp),
                    "pg_user": u,
                    "pg_dbname": dbn,
                    "tables": {"raw": str(raw_table), "main_l5": str(main_l5_table)},
                }
            )
        )

    @db_group.command("migrate-columns-v2")
    @click.option("--apply/--dry-run", default=False, show_default=True, help="Apply schema changes (default: dry-run)")
    @click.option("--table-prefix", default="ghtrader_", show_default=True, help="Only migrate tables with this prefix")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--connect-timeout", default=2, type=int, show_default=True, help="Connect timeout seconds")
    def db_migrate_columns_v2(
        apply: bool,
        table_prefix: str,
        host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        connect_timeout: int,
    ) -> None:
        """
        Migrate ghTrader QuestDB schemas (v2): rename provenance columns.
        """
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.client import QuestDBQueryConfig
        from ghtrader.questdb.migrate import migrate_column_names_v2

        h = str(host).strip() or str(get_questdb_host())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        cfg = QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn)
        out = migrate_column_names_v2(
            cfg=cfg, table_prefix=str(table_prefix), apply=bool(apply), connect_timeout_s=int(connect_timeout)
        )
        click.echo(json.dumps(out, ensure_ascii=False))
        if not bool(out.get("ok", False)):
            raise SystemExit(1)

    @db_group.command("ensure-schema")
    @click.option("--apply/--dry-run", default=False, show_default=True, help="Apply schema changes (default: dry-run)")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--connect-timeout", default=2, type=int, show_default=True, help="Connect timeout seconds")
    def db_ensure_schema(
        apply: bool,
        host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        connect_timeout: int,
    ) -> None:
        """
        Ensure all required columns exist on ghTrader QuestDB tables.

        This command adds missing columns (dataset_version, ticks_kind, etc.) to
        existing tables that were created before the schema was updated.

        Use --dry-run (default) to see what would be changed.
        Use --apply to execute the schema changes.
        """
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.client import QuestDBQueryConfig
        from ghtrader.questdb.migrate import ensure_schema_v2

        h = str(host).strip() or str(get_questdb_host())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        cfg = QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn)
        out = ensure_schema_v2(cfg=cfg, apply=bool(apply), connect_timeout_s=int(connect_timeout))
        click.echo(json.dumps(out, ensure_ascii=False, indent=2))
        if not bool(out.get("ok", False)):
            raise SystemExit(1)

    @db_group.command("benchmark")
    @click.option("--symbol", required=True, help="Symbol to benchmark (e.g. SHFE.cu2602 or KQ.m@SHFE.cu)")
    @click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
    @click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--runs-dir", default="runs", help="Runs directory root")
    @click.option(
        "--ticks-kind",
        "--ticks-lake",
        default="raw",
        type=click.Choice(["raw", "main_l5"]),
        show_default=True,
        help="Which ticks kind to benchmark (raw vs main_l5).",
    )
    @click.option("--max-rows", default=200000, type=int, show_default=True, help="Max rows to load/ingest for the benchmark")
    @click.option("--host", default="127.0.0.1", show_default=True, help="QuestDB host")
    @click.pass_context
    def db_benchmark(
        ctx: click.Context,
        symbol: str,
        start: datetime,
        end: datetime,
        data_dir: str,
        runs_dir: str,
        ticks_kind: str,
        max_rows: int,
        host: str,
    ) -> None:
        """Benchmark QuestDB ingestion/query on a real tick sample."""
        from ghtrader.questdb.bench import run_db_benchmark

        _ = ctx

        out_path, report = run_db_benchmark(
            data_dir=Path(data_dir),
            runs_dir=Path(runs_dir),
            symbol=symbol,
            start_date=start.date(),
            end_date=end.date(),
            ticks_kind=ticks_kind,  # type: ignore[arg-type]
            dataset_version="v2",
            max_rows=int(max_rows),
            host=str(host),
        )
        log.info(
            "db.benchmark_done",
            report_path=str(out_path),
            n_results=len(report.get("results") or []),
            n_errors=len(report.get("errors") or []),
        )

